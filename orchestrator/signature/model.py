import asyncio
from datetime import datetime
from typing import Optional, Self, Any

from hatchet_sdk.runnables.workflow import Workflow
from pydantic import (
    BaseModel,
    field_validator,
    Field,
)
from rapyer import AtomicRedisModel
from rapyer.errors.base import KeyNotFound

from orchestrator.errors import MissingSignatureError
from orchestrator.models.message import ReturnValue
from orchestrator.signature.consts import TASK_ID_PARAM_NAME
from orchestrator.signature.status import TaskStatus, SignatureStatus, PauseActionTypes
from orchestrator.signature.types import TaskIdentifierType, HatchetTaskType
from orchestrator.utils.models import get_marked_fields


class TaskSignature(AtomicRedisModel):
    task_name: str
    kwargs: RedisDict = Field(default_factory=dict)
    workflow_params: RedisDict = Field(default_factory=dict)
    creation_time: RedisDatetime = Field(default_factory=datetime.now)
    model_validators: Optional[Any] = None
    success_callbacks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    error_callbacks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    task_status: TaskStatus = Field(default_factory=TaskStatus)
    task_identifiers: RedisDict = Field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.__class__.__name__}:{self.key}"

    @field_validator("success_callbacks", "error_callbacks", mode="before")
    @classmethod
    def validate_tasks_id(cls, v: list) -> list[TaskIdentifierType]:
        return [cls.validate_task_id(item) for item in v]

    @classmethod
    def validate_task_id(cls, v) -> TaskIdentifierType:
        if isinstance(v, bytes):
            return v.decode()
        if isinstance(v, TaskIdentifierType):
            return v
        elif isinstance(v, TaskSignature):
            return v.id
        else:
            raise ValueError(
                f"Expected task ID or TaskSignature, got {type(v).__name__}"
            )

    @classmethod
    async def from_task(
        cls,
        task: HatchetTaskType,
        workflow_params: dict = None,
        success_callbacks: list[TaskIdentifierType | Self] = None,
        error_callbacks: list[TaskIdentifierType | Self] = None,
        **kwargs,
    ) -> Self:
        signature = cls(
            task_name=task.name,
            kwargs=kwargs,
            model_validators=task.input_validator,
            success_callbacks=success_callbacks or [],
            error_callbacks=error_callbacks or [],
            workflow_params=workflow_params or {},
        )
        await signature.save()
        return signature

    @classmethod
    async def from_id(cls, task_id: TaskIdentifierType) -> Self:
        signature_class, pk = extract_class_and_id(task_id)
        return await signature_class.get(pk)

    @classmethod
    async def from_id_safe(cls, task_id: TaskIdentifierType) -> Optional[Self]:
        try:
            return await cls.from_id(task_id)
        except KeyNotFound:
            return None

    @classmethod
    async def from_task_name(
        cls, task_name: str, input_validator: type[BaseModel] = None, **kwargs
    ) -> Self:
        if not input_validator:
            input_validator = await load_validator(
                dono_hatchet_config.redis_client, task_name
            )

        model_fields = list(cls.__pydantic_fields__)
        optional_task_params = {
            field_name: kwargs.pop(field_name)
            for field_name in model_fields
            if field_name in kwargs
        }
        more_kwargs = dict(model_validators=input_validator)
        kwargs |= more_kwargs
        signature = cls(task_name=task_name, kwargs=kwargs, **optional_task_params)
        await signature.save()
        return signature

    @classmethod
    async def delete_signature(cls, task_id: TaskIdentifierType):
        result = await dono_hatchet_config.redis_client.remove(task_id)
        return result

    async def add_callbacks(
        self, success: list[Self] = None, errors: list[Self] = None
    ) -> bool:
        if success:
            success = [self.validate_task_id(s) for s in success]
        if errors:
            errors = [self.validate_task_id(e) for e in errors]
        async with self.pipeline() as signature:
            await signature.success_callbacks.aextend(success)
            await signature.error_callbacks.aextend(errors)

    async def workflow(self, use_return_field: bool = True, **task_additional_params):
        input_validators = self.model_validators
        total_kwargs = self.kwargs | task_additional_params
        task = await load_name(redis, task)

        return_field = "results" if use_return_field else None
        if input_validators and return_field:
            return_value_fields = get_marked_fields(input_validators, ReturnValue)
            if return_value_fields:
                return_field = return_value_fields[0][1]

        workflow = hatchet.workflow(
            name=task, input_validator=input_validators, **self.workflow_params
        )
        orchestrator_workflow = OrchestratorWorkflow(
            workflow,
            workflow_params=total_kwargs,
            return_value_field=return_field,
            task_ctx=self.task_ctx(),
        )
        return orchestrator_workflow

    def task_ctx(self) -> dict:
        return self.task_identifiers | {TASK_ID_PARAM_NAME: self.id}

    async def aio_run_no_wait(self, msg: BaseModel, **kwargs):
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
            *[TaskSignature.from_id_safe(callback_id) for callback_id in callback_ids]
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
    async def try_remove(cls, task_id: TaskIdentifierType, **kwargs):
        try:
            task = await cls.from_id_safe(task_id)
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
            *[
                TaskSignature.from_id_safe(task_id)
                for task_id in addition_tasks_to_delete
            ]
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
        return self.task_status.status == SignatureStatus.PENDING

    async def change_status(self, status: SignatureStatus) -> bool:
        return await self.task_status.aupdate(
            last_status=self.task_status.status, status=status
        )

    # When pausing signature from outside the task
    @classmethod
    async def safe_change_status(
        cls, task_id: TaskIdentifierType, status: SignatureStatus
    ) -> bool:
        try:
            async with cls.lock_from_id(task_id) as task:
                return await task.change_status(status)
        except Exception as e:
            return False

    async def on_pause_signature(self, msg: BaseModel):
        await self.kwargs.aupdate(**msg.model_dump(mode="json"))

    async def on_cancel_signature(self, msg: BaseModel):
        await self.remove()

    @classmethod
    def lock_from_id(cls, task_id: TaskIdentifierType, **kwargs):
        signature_cls, key = extract_class_and_id(task_id)
        return signature_cls.lock_from_key(key, **kwargs)

    @classmethod
    async def resume_from_id(cls, task_id: TaskIdentifierType):
        async with cls.lock_from_id(task_id) as task:
            await task.resume()

    async def resume(self):
        last_status = self.task_status.last_status
        if last_status == SignatureStatus.ACTIVE:
            await self.change_status(SignatureStatus.PENDING)
            await self.aio_run_no_wait(FakeModel())
        else:
            await self.change_status(last_status)

    @classmethod
    async def suspend_from_id(self, task_id: TaskIdentifierType):
        async with self.lock_from_id(task_id) as task:
            await task.suspend()

    async def suspend(self):
        """
        Task suspention will try and stop the task at before it starts
        """
        await self.change_status(SignatureStatus.SUSPENDED)

    @classmethod
    async def interrupt_from_id(cls, task_id: TaskIdentifierType):
        async with cls.lock_from_id(task_id) as task:
            return task.interrupt()

    async def interrupt(self):
        """
        Task interrupt will try to aggresivley take hold of the async loop and stop the task
        """
        raise NotImplementedError()

    @classmethod
    async def pause_from_id(
        cls,
        task_id: TaskIdentifierType,
        pause_type: PauseActionTypes = PauseActionTypes.SUSPEND,
    ):
        async with cls.lock_from_id(task_id) as task:
            await task.pause_task(pause_type)

    async def pause_task(self, pause_type: PauseActionTypes = PauseActionTypes.SUSPEND):
        if pause_type == PauseActionTypes.SUSPEND:
            return await self.suspend()
        elif pause_type == PauseActionTypes.INTERRUPT:
            return await self.interrupt()
        raise NotImplementedError(f"Pause type {pause_type} not supported")


SIGNATURES_NAME_MAPPING: dict[str, type[TaskSignature]] = {}


def extract_class_and_id(
    task_id: TaskIdentifierType,
) -> tuple[type[TaskSignature], str]:
    class_name, pk = task_id.split(":", 1)
    signature_class = SIGNATURES_NAME_MAPPING.get(class_name, TaskSignature)
    return signature_class, pk


async def sign(task: str | HatchetTaskType, **kwargs):
    if isinstance(task, str):
        return await TaskSignature.from_task_name(task, **kwargs)
    else:
        return await TaskSignature.from_task(task, **kwargs)


load_signature = TaskSignature.from_id_safe
resume_task = TaskSignature.resume_from_id
lock_task = TaskSignature.lock_from_id
resume = TaskSignature.resume_from_id
pause = TaskSignature.pause_from_id
