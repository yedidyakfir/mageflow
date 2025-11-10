from orchestrator.callbacks import register_task, handle_task_callback
from orchestrator.init import init_from_dynaconf, init_orchestrator_hatchet_tasks
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.types import HatchetTaskType


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

__all__ = [
    "load_signature",
    "resume_task",
    "lock_task",
    "resume",
    "pause",
    "sign",
    "init_from_dynaconf",
    "init_orchestrator_hatchet_tasks",
    "register_task",
    "handle_task_callback",
]
