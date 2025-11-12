import asyncio
from typing import Self, Any, Optional

from hatchet_sdk.runnables.types import EmptyModel
from pydantic import Field, field_validator, BaseModel
from rapyer.types import RedisListType, RedisIntType, RedisDictType

from orchestrator.errors import MissingSignatureError, MissingSwarmItemError
from orchestrator.signature.creator import (
    TaskSignatureConvertible,
    resolve_signature_id,
)
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.status import SignatureStatus
from orchestrator.signature.types import TaskIdentifierType
from orchestrator.swarm.consts import (
    BATCH_TASK_NAME_INITIALS,
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
    ON_SWARM_END,
    ON_SWARM_ERROR,
    ON_SWARM_START,
)
from orchestrator.swarm.messages import SwarmResultsMessage
from orchestrator.utils.pythonic import deep_merge


class BatchItemTaskSignature(TaskSignature):
    swarm_id: TaskIdentifierType
    original_task_id: TaskIdentifierType

    async def aio_run_no_wait(self, msg: BaseModel, **orig_task_kwargs):
        async with self.lock() as swarm_item:
            swarm_task = await SwarmTaskSignature.from_id_safe(self.swarm_id)
            original_task = await TaskSignature.from_id_safe(self.original_task_id)
            if swarm_task is None:
                raise MissingSignatureError(
                    f"Swarm {self.swarm_id} was deleted before finish"
                )
            if original_task is None:
                raise MissingSwarmItemError(
                    f"Task {self.original_task_id} was deleted before it was run in swarm"
                )
            can_run_task = await swarm_task.add_to_running_tasks(self)
            kwargs = deep_merge(self.kwargs.clone(), original_task.kwargs.clone())
            kwargs = deep_merge(kwargs, swarm_task.task_kwargs.clone())
            if not can_run_task:
                kwargs = deep_merge(kwargs, msg.model_dump(mode="json"))
            await original_task.kwargs.aupdate(**kwargs)
            if can_run_task:
                return await original_task.aio_run_no_wait(msg, **orig_task_kwargs)

            return None

    async def _remove(self, with_error: bool = True, with_success: bool = True):
        remove_self = super()._remove(with_error, with_success)
        remove_original = TaskSignature.try_remove(self.original_task_id)
        return await asyncio.gather(remove_self, remove_original)

    async def change_status(self, status: SignatureStatus):
        return await TaskSignature.safe_change_status(self.original_task_id, status)

    async def resume(self):
        async with TaskSignature.lock_from_id(self.original_task_id) as task:
            await task.resume()
            return await super().change_status(task.task_status.last_status)

    async def suspend(self):
        await TaskSignature.suspend_from_id(self.original_task_id)
        return await super().change_status(SignatureStatus.SUSPENDED)

    async def interrupt(self):
        await TaskSignature.interrupt_from_id(self.original_task_id)
        return await super().change_status(SignatureStatus.INTERRUPTED)


class SwarmConfig(BaseModel):
    max_concurrency: int = 30
    stop_after_n_failures: Optional[int] = None


class SwarmTaskSignature(TaskSignature):
    tasks: RedisListType[TaskIdentifierType] = Field(default_factory=list)
    tasks_left_to_run: RedisListType[TaskIdentifierType] = Field(default_factory=list)
    finished_tasks: RedisListType[TaskIdentifierType] = Field(default_factory=list)
    failed_tasks: RedisListType[TaskIdentifierType] = Field(default_factory=list)
    tasks_results: RedisListType[Any] = Field(default_factory=list)
    # This flag is raised when no more tasks can be added to the swarm
    is_swarm_closed: bool = False
    # How many tasks can be added to the swarm at a time
    current_running_tasks: RedisIntType = 0
    config: SwarmConfig = Field(default_factory=SwarmConfig)

    # Specific task kwargs
    task_kwargs: RedisDictType = Field(default_factory=dict)

    @field_validator(
        "tasks", "tasks_left_to_run", "finished_tasks", "failed_tasks", mode="before"
    )
    @classmethod
    def validate_tasks(cls, v):
        return [cls.validate_task_id(item) for item in v]

    @property
    def has_swarm_started(self):
        return self.current_running_tasks or self.failed_tasks or self.finished_tasks

    async def aio_run_no_wait(self, msg: BaseModel, **kwargs):
        await self.task_kwargs.aupdate(**msg.model_dump(mode="json"))
        workflow = await self.workflow(use_return_field=False, context=msg.context)
        return await workflow.aio_run_no_wait(msg, **kwargs)

    async def workflow(self, use_return_field: bool = True, **task_additional_params):
        # Use on swarm start task name for wf
        task_name = self.task_name
        self.task_name = ON_SWARM_START
        workflow = await super().workflow(
            **task_additional_params, use_return_field=True
        )
        self.task_name = task_name
        return workflow

    def task_ctx(self) -> dict:
        original_ctx = super().task_ctx()
        swarm_ctx = {SWARM_TASK_ID_PARAM_NAME: self.id}
        return original_ctx | swarm_ctx

    async def try_delete_sub_tasks(
        self, with_error: bool = True, with_success: bool = True
    ):
        tasks = await asyncio.gather(
            *[TaskSignature.from_id_safe(task_id) for task_id in self.tasks],
            return_exceptions=True,
        )
        tasks = [task for task in tasks if isinstance(task, TaskSignature)]
        await asyncio.gather(
            *[task.remove(with_error, with_success) for task in tasks],
            return_exceptions=True,
        )

    async def _remove(self, *args, **kwargs):
        delete_signature = super()._remove(*args, **kwargs)
        delete_tasks = self.try_delete_sub_tasks()

        return await asyncio.gather(delete_signature, delete_tasks)

    async def change_status(self, status: SignatureStatus):
        paused_chain_tasks = [
            TaskSignature.safe_change_status(task, status) for task in self.tasks
        ]
        pause_chain = super().change_status(status)
        await asyncio.gather(pause_chain, *paused_chain_tasks, return_exceptions=True)

    async def add_task(self, task: TaskSignatureConvertible) -> BatchItemTaskSignature:
        if self.task_status.is_done():
            raise RuntimeError(
                f"Swarm {self.task_name} is {self.task_status} - can't add task"
            )
        task = await resolve_signature_id(task)
        dump = task.model_dump(exclude={"task_name"})
        batch_task_name = f"{BATCH_TASK_NAME_INITIALS}{task.task_name}"
        batch_task = BatchItemTaskSignature(
            **dump,
            task_name=batch_task_name,
            swarm_id=self.id,
            original_task_id=task.id,
        )

        swarm_identifiers = {
            SWARM_TASK_ID_PARAM_NAME: self.id,
            SWARM_ITEM_TASK_ID_PARAM_NAME: batch_task.id,
        }
        on_success_swarm_item = await TaskSignature.from_task_name(
            task_name=ON_SWARM_END,
            input_validator=SwarmResultsMessage,
            task_identifiers=swarm_identifiers,
        )
        on_error_swarm_item = await TaskSignature.from_task_name(
            task_name=ON_SWARM_ERROR, task_identifiers=swarm_identifiers
        )
        task.success_callbacks.append(on_success_swarm_item.id)
        task.error_callbacks.append(on_error_swarm_item.id)
        await task.save()
        await batch_task.save()
        await self.tasks.aappend(batch_task.id)
        return batch_task

    async def add_to_running_tasks(self, task: TaskSignatureConvertible) -> bool:
        async with self.lock() as swarm_task:
            task = await resolve_signature_id(task)
            if self.current_running_tasks < self.config.max_concurrency:
                await self.current_running_tasks.increase()
                self.current_running_tasks += 1
                return True
            else:
                await self.tasks_left_to_run.aappend(task.id)
                return False

    async def decrease_running_tasks_count(self):
        await self.current_running_tasks.increase(-1)
        self.current_running_tasks -= 1

    async def pop_task_to_run(self) -> TaskIdentifierType | None:
        task = await self.tasks_left_to_run.apop()
        return task

    async def add_to_finished_tasks(self, task: TaskIdentifierType):
        await self.finished_tasks.aappend(task)

    async def add_to_failed_tasks(self, task: TaskIdentifierType):
        await self.failed_tasks.aappend(task)

    async def is_swarm_done(self):
        done_tasks = self.finished_tasks + self.failed_tasks
        finished_all_tasks = set(done_tasks) == set(self.tasks)
        return self.is_swarm_closed and finished_all_tasks

    async def activate_error(self, msg, **kwargs):
        full_kwargs = self.task_kwargs | self.kwargs | kwargs
        return await super().activate_error(msg, **full_kwargs)

    async def activate_success(self, msg, **kwargs):
        await self.tasks_results.load()
        tasks_results = [res for res in self.tasks_results]

        full_kwargs = {"context": msg.context} | self.task_kwargs | self.kwargs | kwargs
        await super().activate_success(tasks_results, **full_kwargs)
        await self.remove(with_success=False)

    async def suspend(self):
        await asyncio.gather(
            *[TaskSignature.suspend_from_id(swarm_id) for swarm_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(SignatureStatus.SUSPENDED)

    async def resume(self):
        await asyncio.gather(
            *[TaskSignature.resume_from_id(task_id) for task_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(self.task_status.last_status)

    async def close_swarm(self) -> Self:
        async with self.lock() as swarm_task:
            await swarm_task.aupdate(is_swarm_closed=True)
            should_finish_swarm = await swarm_task.is_swarm_done()
            if should_finish_swarm:
                await swarm_task.activate_success(EmptyModel())
        return self
