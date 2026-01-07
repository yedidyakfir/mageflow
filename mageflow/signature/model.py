import asyncio
from datetime import datetime
from typing import Optional, Self, Any, TypeAlias, ClassVar

import rapyer
from hatchet_sdk.runnables.types import EmptyModel
from hatchet_sdk.runnables.workflow import Workflow
from pydantic import BaseModel, field_validator, Field
from rapyer import AtomicRedisModel
from rapyer.config import RedisConfig
from rapyer.errors.base import KeyNotFound
from rapyer.types import RedisDict, RedisList, RedisDatetime

from mageflow.errors import MissingSignatureError
from mageflow.models.message import ReturnValue
from mageflow.root.context import current_root_swarm
from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.status import TaskStatus, SignatureStatus, PauseActionTypes
from mageflow.signature.types import TaskIdentifierType, HatchetTaskType
from mageflow.startup import mageflow_config
from mageflow.task.model import HatchetTaskModel
from mageflow.utils.models import get_marked_fields
from mageflow.workflows import MageflowWorkflow


class TaskSignature(AtomicRedisModel):
    task_name: str
    kwargs: RedisDict = Field(default_factory=dict)
    creation_time: RedisDatetime = Field(default_factory=datetime.now)
    model_validators: Optional[Any] = None
    success_callbacks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    error_callbacks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    task_status: TaskStatus = Field(default_factory=TaskStatus)
    task_identifiers: RedisDict = Field(default_factory=dict)

    Meta: ClassVar[RedisConfig] = RedisConfig(ttl=24 * 60 * 60)

    @field_validator("success_callbacks", "error_callbacks", mode="before")
    @classmethod
    def validate_tasks_id(cls, v: list) -> list[TaskIdentifierType]:
        return [cls.validate_task_key(item) for item in v]

    @classmethod
    def validate_task_key(cls, v) -> TaskIdentifierType:
        if isinstance(v, bytes):
            return v.decode()
        if isinstance(v, TaskIdentifierType):
            return v
        elif isinstance(v, TaskSignature):
            return v.key
        else:
            raise ValueError(
                f"Expected task ID or TaskSignature, got {type(v).__name__}"
            )

    @classmethod
    async def from_task(
        cls,
        task: HatchetTaskType,
        success_callbacks: list[TaskIdentifierType | Self] = None,
        error_callbacks: list[TaskIdentifierType | Self] = None,
        **kwargs,
    ) -> Self:
        signature = cls(
            task_name=task.name,
            model_validators=task.input_validator,
            success_callbacks=success_callbacks or [],
            error_callbacks=error_callbacks or [],
            **kwargs,
        )
        await signature.save()
        return signature

    @classmethod
    async def get_safe(cls, task_key: TaskIdentifierType) -> Optional[Self]:
        try:
            return await rapyer.aget(task_key)
        except KeyNotFound:
            return None

    @classmethod
    async def from_task_name(
        cls, task_name: str, model_validators: type[BaseModel] = None, **kwargs
    ) -> Self:
        if not model_validators:
            task_def = await HatchetTaskModel.safe_get(task_name)
            model_validators = task_def.input_validator if task_def else None

        signature = cls(
            task_name=task_name, model_validators=model_validators, **kwargs
        )
        await signature.save()
        return signature

    @classmethod
    async def delete_signature(cls, task_key: TaskIdentifierType):
        result = await mageflow_config.redis_client.remove(task_key)
        return result

    async def add_callbacks(
        self, success: list[Self] = None, errors: list[Self] = None
    ):
        if success:
            success = [self.validate_task_key(s) for s in success]
        if errors:
            errors = [self.validate_task_key(e) for e in errors]
        async with self.pipeline() as signature:
            await signature.success_callbacks.aextend(success)
            await signature.error_callbacks.aextend(errors)

    def return_value_field(self) -> Optional[str]:
        marked_field = get_marked_fields(self.model_validators, ReturnValue)
        try:
            return_field_name = marked_field[0][1]
        except IndexError:
            return_field_name = None
        return return_field_name or "results"

    async def workflow(self, use_return_field: bool = True, **task_additional_params):
        total_kwargs = self.kwargs | task_additional_params
        task_def = await HatchetTaskModel.safe_get(self.task_name)
        task = task_def.task_name if task_def else self.task_name
        return_field = self.return_value_field() if use_return_field else None

        workflow = mageflow_config.hatchet_client.workflow(
            name=task, input_validator=self.model_validators
        )
        mageflow_wf = MageflowWorkflow(
            workflow,
            workflow_params=total_kwargs,
            return_value_field=return_field,
            task_ctx=self.task_ctx(),
        )
        return mageflow_wf

    def task_ctx(self) -> dict:
        return self.task_identifiers | {TASK_ID_PARAM_NAME: self.key}

    async def aio_run_no_wait(self, msg: BaseModel, **kwargs):
        root_swarm = current_root_swarm.get()
        if root_swarm:
            batch_item = await root_swarm.add_task(self)
            return await batch_item.aio_run_no_wait(msg, **kwargs)

        workflow = await self.workflow(use_return_field=False)
        return await workflow.aio_run_no_wait(msg, **kwargs)

    async def callback_workflows(
        self, with_success: bool = True, with_error: bool = True, **kwargs
    ) -> list[Workflow]:
        callback_ids = []
        if with_success:
            callback_ids.extend(self.success_callbacks)
        if with_error:
            callback_ids.extend(self.error_callbacks)
        callbacks_signatures = await asyncio.gather(
            *[TaskSignature.get_safe(callback_id) for callback_id in callback_ids]
        )
        if any([sign is None for sign in callbacks_signatures]):
            raise MissingSignatureError(
                f"Some callbacks not found {callback_ids}, signature can be called only once"
            )
        workflows = await asyncio.gather(
            *[callback.workflow(**kwargs) for callback in callbacks_signatures]
        )
        return workflows

    async def activate_callbacks(
        self, msg, with_success: bool = True, with_error: bool = True, **kwargs
    ):
        workflows = await self.callback_workflows(with_success, with_error, **kwargs)
        await asyncio.gather(*[workflow.aio_run_no_wait(msg) for workflow in workflows])

    async def activate_success(self, msg, **kwargs):
        return await self.activate_callbacks(
            msg, with_success=True, with_error=False, **kwargs
        )

    async def activate_error(self, msg, **kwargs):
        return await self.activate_callbacks(
            msg,
            with_success=False,
            with_error=True,
            use_return_field=False,
            **kwargs,
        )

    @classmethod
    async def try_remove(cls, task_key: TaskIdentifierType, **kwargs):
        try:
            task = await cls.get_safe(task_key)
            await task.remove(**kwargs)
        except Exception as e:
            pass

    async def remove(self, with_error: bool = True, with_success: bool = True):
        return await self._remove(with_error, with_success)

    async def _remove(self, with_error: bool = True, with_success: bool = True):
        addition_tasks_to_delete = []
        if with_error:
            addition_tasks_to_delete.extend(
                [error_id for error_id in self.error_callbacks]
            )
        if with_success:
            addition_tasks_to_delete.extend(
                [success_id for success_id in self.success_callbacks]
            )

        signatures_to_delete = await asyncio.gather(
            *[TaskSignature.get_safe(task_id) for task_id in addition_tasks_to_delete]
        )

        delete_tasks = [self.delete()]
        delete_tasks.extend(
            [
                signature_to_delete.remove()
                for signature_to_delete in signatures_to_delete
                if signature_to_delete
            ]
        )

        return await asyncio.gather(*delete_tasks)

    async def handle_inactive_task(self, msg: BaseModel):
        if self.task_status.status == SignatureStatus.SUSPENDED:
            await self.on_pause_signature(msg)
        elif self.task_status.status == SignatureStatus.CANCELED:
            await self.on_cancel_signature(msg)

    async def should_run(self):
        return self.task_status.should_run()

    async def change_status(self, status: SignatureStatus):
        await self.task_status.aupdate(
            last_status=self.task_status.status, status=status
        )

    async def start_task(self) -> Self:
        return self

    async def end_task(self, success: bool = True) -> Self:
        return self

    async def aupdate_real_task_kwargs(self, **kwargs):
        return await self.kwargs.aupdate(**kwargs)

    # When pausing signature from outside the task
    @classmethod
    async def safe_change_status(
        cls, task_id: TaskIdentifierType, status: SignatureStatus
    ) -> bool:
        try:
            async with rapyer.alock_from_key(task_id) as task:
                return await task.change_status(status)
        except Exception as e:
            return False

    async def on_pause_signature(self, msg: BaseModel):
        await self.kwargs.aupdate(**msg.model_dump(mode="json"))

    async def on_cancel_signature(self, msg: BaseModel):
        await self.remove()

    @classmethod
    async def resume_from_key(cls, task_key: TaskIdentifierType):
        async with rapyer.alock_from_key(task_key) as task:
            await task.resume()

    async def resume(self):
        last_status = self.task_status.last_status
        if last_status == SignatureStatus.ACTIVE:
            await self.change_status(SignatureStatus.PENDING)
            await self.aio_run_no_wait(EmptyModel())
        else:
            await self.change_status(last_status)

    @classmethod
    async def suspend_from_key(cls, task_key: TaskIdentifierType):
        async with rapyer.alock_from_key(task_key) as task:
            await task.suspend()

    async def suspend(self):
        """
        Task suspension will try and stop the task at before it starts
        """
        await self.change_status(SignatureStatus.SUSPENDED)

    @classmethod
    async def interrupt_from_key(cls, task_key: TaskIdentifierType):
        async with rapyer.alock_from_key(task_key) as task:
            return task.interrupt()

    async def interrupt(self):
        """
        Task interrupt will try to aggressively take hold of the async loop and stop the task
        """
        raise NotImplementedError()

    @classmethod
    async def pause_from_key(
        cls,
        task_key: TaskIdentifierType,
        pause_type: PauseActionTypes = PauseActionTypes.SUSPEND,
    ):
        async with rapyer.alock_from_key(task_key) as task:
            await task.pause_task(pause_type)

    async def pause_task(self, pause_type: PauseActionTypes = PauseActionTypes.SUSPEND):
        if pause_type == PauseActionTypes.SUSPEND:
            return await self.suspend()
        elif pause_type == PauseActionTypes.INTERRUPT:
            return await self.interrupt()
        raise NotImplementedError(f"Pause type {pause_type} not supported")


SIGNATURES_NAME_MAPPING: dict[str, type[TaskSignature]] = {}
TaskInputType: TypeAlias = TaskIdentifierType | TaskSignature
