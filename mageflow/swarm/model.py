import asyncio
from typing import Self, Any, Optional

from hatchet_sdk.runnables.types import EmptyModel
from mageflow.errors import (
    MissingSignatureError,
    MissingSwarmItemError,
    TooManyTasksError,
    SwarmIsCanceledError,
)
from mageflow.signature.creator import (
    TaskSignatureConvertible,
    resolve_signature_key,
)
from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.signature.types import TaskIdentifierType
from mageflow.swarm.consts import (
    BATCH_TASK_NAME_INITIALS,
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
    ON_SWARM_END,
    ON_SWARM_ERROR,
    ON_SWARM_START,
)
from mageflow.swarm.messages import SwarmResultsMessage
from mageflow.utils.pythonic import deep_merge
from pydantic import Field, field_validator, BaseModel
from rapyer import AtomicRedisModel
from rapyer.types import RedisList, RedisInt


class BatchItemTaskSignature(TaskSignature):
    swarm_id: TaskIdentifierType
    original_task_id: TaskIdentifierType

    async def aio_run_no_wait(self, msg: BaseModel, **orig_task_kwargs):
        async with self.lock() as swarm_item:
            swarm_task = await SwarmTaskSignature.get_safe(self.swarm_id)
            original_task = await TaskSignature.get_safe(self.original_task_id)
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
            kwargs = deep_merge(kwargs, swarm_task.kwargs.clone())
            if not can_run_task:
                kwargs = deep_merge(kwargs, msg.model_dump(mode="json"))
            await original_task.aupdate_real_task_kwargs(**kwargs)
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
        async with TaskSignature.lock_from_key(self.original_task_id) as task:
            await task.resume()
            return await super().change_status(task.task_status.last_status)

    async def suspend(self):
        await TaskSignature.suspend_from_key(self.original_task_id)
        return await super().change_status(SignatureStatus.SUSPENDED)

    async def interrupt(self):
        await TaskSignature.interrupt_from_key(self.original_task_id)
        return await super().change_status(SignatureStatus.INTERRUPTED)


class SwarmConfig(AtomicRedisModel):
    max_concurrency: int = 30
    stop_after_n_failures: Optional[int] = None
    max_task_allowed: Optional[int] = None

    def can_add_task(self, swarm: "SwarmTaskSignature") -> bool:
        if self.max_task_allowed is None:
            return True
        return len(swarm.tasks) < self.max_task_allowed


class SwarmTaskSignature(TaskSignature):
    tasks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    tasks_left_to_run: RedisList[TaskIdentifierType] = Field(default_factory=list)
    finished_tasks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    failed_tasks: RedisList[TaskIdentifierType] = Field(default_factory=list)
    tasks_results: RedisList[Any] = Field(default_factory=list)
    # This flag is raised when no more tasks can be added to the swarm
    is_swarm_closed: bool = False
    # How many tasks can be added to the swarm at a time
    current_running_tasks: RedisInt = 0
    config: SwarmConfig = Field(default_factory=SwarmConfig)

    @field_validator(
        "tasks", "tasks_left_to_run", "finished_tasks", "failed_tasks", mode="before"
    )
    @classmethod
    def validate_tasks(cls, v):
        return [cls.validate_task_key(item) for item in v]

    @property
    def has_swarm_started(self):
        return self.current_running_tasks or self.failed_tasks or self.finished_tasks

    async def aio_run_no_wait(self, msg: BaseModel, **kwargs):
        await self.kwargs.aupdate(**msg.model_dump(mode="json"))
        workflow = await self.workflow(use_return_field=False)
        return await workflow.aio_run_no_wait(msg, **kwargs)

    async def workflow(self, use_return_field: bool = True, **task_additional_params):
        # Use on swarm start task name for wf
        task_name = self.task_name
        self.task_name = ON_SWARM_START
        workflow = await super().workflow(
            **task_additional_params, use_return_field=use_return_field
        )
        self.task_name = task_name
        return workflow

    def task_ctx(self) -> dict:
        original_ctx = super().task_ctx()
        swarm_ctx = {SWARM_TASK_ID_PARAM_NAME: self.key}
        return original_ctx | swarm_ctx

    async def try_delete_sub_tasks(
        self, with_error: bool = True, with_success: bool = True
    ):
        tasks = await asyncio.gather(
            *[TaskSignature.get_safe(task_id) for task_id in self.tasks],
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

    async def add_task(
        self, task: TaskSignatureConvertible, close_on_max_task: bool = True
    ) -> BatchItemTaskSignature:
        """
        task - task signature to add to swarm
        close_on_max_task - if true, and you set max task allowed on swarm, this swarm will close if the task reached maximum capcity
        """
        if not self.config.can_add_task(self):
            raise TooManyTasksError(
                f"Swarm {self.task_name} has reached max tasks limit"
            )
        if self.task_status.is_canceled():
            raise SwarmIsCanceledError(
                f"Swarm {self.task_name} is {self.task_status} - can't add task"
            )
        task = await resolve_signature_key(task)
        dump = task.model_dump(exclude={"task_name"})
        batch_task_name = f"{BATCH_TASK_NAME_INITIALS}{task.task_name}"
        batch_task = BatchItemTaskSignature(
            **dump,
            task_name=batch_task_name,
            swarm_id=self.key,
            original_task_id=task.key,
        )

        swarm_identifiers = {
            SWARM_TASK_ID_PARAM_NAME: self.key,
            SWARM_ITEM_TASK_ID_PARAM_NAME: batch_task.key,
        }
        on_success_swarm_item = await TaskSignature.from_task_name(
            task_name=ON_SWARM_END,
            input_validator=SwarmResultsMessage,
            task_identifiers=swarm_identifiers,
        )
        on_error_swarm_item = await TaskSignature.from_task_name(
            task_name=ON_SWARM_ERROR, task_identifiers=swarm_identifiers
        )
        task.success_callbacks.append(on_success_swarm_item.key)
        task.error_callbacks.append(on_error_swarm_item.key)
        await task.save()
        await batch_task.save()
        await self.tasks.aappend(batch_task.key)

        if close_on_max_task and not self.config.can_add_task(self):
            await self.close_swarm()

        return batch_task

    async def add_to_running_tasks(self, task: TaskSignatureConvertible) -> bool:
        async with self.lock() as swarm_task:
            task = await resolve_signature_key(task)
            if self.current_running_tasks < self.config.max_concurrency:
                await self.current_running_tasks.increase()
                self.current_running_tasks += 1
                return True
            else:
                await self.tasks_left_to_run.aappend(task.key)
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
        full_kwargs = self.kwargs | kwargs
        return await super().activate_error(msg, **full_kwargs)

    async def activate_success(self, msg, **kwargs):
        results = await self.tasks_results.load()
        tasks_results = [res for res in results]

        await super().activate_success(tasks_results, **kwargs)
        await self.remove(with_success=False)

    async def suspend(self):
        await asyncio.gather(
            *[TaskSignature.suspend_from_key(swarm_id) for swarm_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(SignatureStatus.SUSPENDED)

    async def resume(self):
        await asyncio.gather(
            *[TaskSignature.resume_from_key(task_id) for task_id in self.tasks],
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
