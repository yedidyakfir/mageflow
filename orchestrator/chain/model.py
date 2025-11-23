import asyncio

from pydantic import field_validator, Field

from orchestrator.errors import MissingSignatureError
from orchestrator.signature.model import TaskSignature, TaskIdentifierType
from orchestrator.signature.status import SignatureStatus


class ChainTaskSignature(TaskSignature):
    tasks: list[TaskIdentifierType] = Field(default_factory=list)

    @field_validator("tasks", mode="before")
    @classmethod
    def validate_tasks(cls, v: list[TaskSignature]):
        return [cls.validate_task_id(item) for item in v]

    async def workflow(self, **task_additional_params):
        first_task = await TaskSignature.from_id(self.tasks[0])
        if first_task is None:
            raise MissingSignatureError(f"First task from chain {self.id} not found")
        return await first_task.workflow(**task_additional_params)

    async def delete_chain_tasks(self, with_errors=True, with_success=True):
        signatures = await asyncio.gather(
            *[TaskSignature.from_id(signature_id) for signature_id in self.tasks],
            return_exceptions=True,
        )
        signatures = [sign for sign in signatures if isinstance(sign, TaskSignature)]
        delete_tasks = [
            signature.remove(with_errors, with_success) for signature in signatures
        ]
        await asyncio.gather(*delete_tasks)

    async def change_status(self, status: SignatureStatus):
        pause_chain_tasks = [
            TaskSignature.safe_change_status(task, status) for task in self.tasks
        ]
        pause_chain = super().change_status(status)
        await asyncio.gather(pause_chain, *pause_chain_tasks, return_exceptions=True)

    async def suspend(self):
        await asyncio.gather(
            *[TaskSignature.suspend_from_id(task_id) for task_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(SignatureStatus.SUSPENDED)

    async def interrupt(self):
        await asyncio.gather(
            *[TaskSignature.interrupt_from_id(task_id) for task_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(SignatureStatus.INTERRUPTED)

    async def resume(self):
        await asyncio.gather(
            *[self.resume_from_id(task_id) for task_id in self.tasks],
            return_exceptions=True,
        )
        await super().change_status(self.task_status.last_status)
