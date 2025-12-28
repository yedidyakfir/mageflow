import asyncio
from typing import Any

from hatchet_sdk import Context
from hatchet_sdk.runnables.contextvars import ctx_additional_metadata
from pydantic import BaseModel

from mageflow.invokers.base import BaseInvoker
from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.workflows import TASK_DATA_PARAM_NAME


class HatchetInvoker(BaseInvoker):
    def __init__(self, message: BaseModel, ctx: Context):
        self.message = message
        self.task_data = ctx.additional_metadata.get(TASK_DATA_PARAM_NAME, {})
        self.workflow_id = ctx.workflow_id
        hatchet_ctx_metadata = ctx_additional_metadata.get() or {}
        hatchet_ctx_metadata.pop(TASK_DATA_PARAM_NAME, None)
        ctx_additional_metadata.set(hatchet_ctx_metadata)

    @property
    def task_ctx(self) -> dict:
        return self.task_data

    async def start_task(self) -> TaskSignature | None:
        task_id = self.task_data.get(TASK_ID_PARAM_NAME, None)
        if task_id:
            async with TaskSignature.lock_from_key(task_id) as signature:
                await signature.change_status(SignatureStatus.ACTIVE)
                await signature.task_status.aupdate(worker_task_id=self.workflow_id)
                return signature

    async def run_success(self, result: Any) -> bool:
        success_publish_tasks = []
        task_id = self.task_data.get(TASK_ID_PARAM_NAME, None)
        if task_id:
            current_task = await TaskSignature.get_safe(task_id)
            task_success_workflows = current_task.activate_success(result)
            success_publish_tasks.append(asyncio.create_task(task_success_workflows))

        if success_publish_tasks:
            await asyncio.gather(*success_publish_tasks)
            return True
        return False

    async def run_error(self) -> bool:
        error_publish_tasks = []
        task_id = self.task_data.get(TASK_ID_PARAM_NAME, None)
        if task_id:
            current_task = await TaskSignature.get_safe(task_id)
            task_error_workflows = current_task.activate_error(self.message)
            error_publish_tasks.append(asyncio.create_task(task_error_workflows))

        if error_publish_tasks:
            await asyncio.gather(*error_publish_tasks)
            return True
        return False

    async def remove_task(
        self, with_success: bool = True, with_error: bool = True
    ) -> TaskSignature | None:
        task_id = self.task_data.get(TASK_ID_PARAM_NAME, None)
        if task_id:
            signature = await TaskSignature.get_safe(task_id)
            if signature:
                await signature.remove(with_error, with_success)

    async def should_run_task(self) -> bool:
        task_id = self.task_data.get(TASK_ID_PARAM_NAME, None)
        if task_id:
            signature = await TaskSignature.get_safe(task_id)
            if signature is None:
                return False
            should_task_run = await signature.should_run()
            if should_task_run:
                return True
            await signature.task_status.aupdate(last_status=SignatureStatus.ACTIVE)
            await signature.handle_inactive_task(self.message)
            return False
        return True
