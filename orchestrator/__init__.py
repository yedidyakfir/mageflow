from datetime import datetime
from typing import TypedDict, overload, Any

from orchestrator.callbacks import register_task, handle_task_callback
from orchestrator.chain.creator import chain
from orchestrator.client import Orchestrator
from orchestrator.init import init_orchestrator_hatchet_tasks
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.status import TaskStatus
from orchestrator.signature.types import HatchetTaskType, TaskIdentifierType
from orchestrator.startup import init_from_dynaconf
from orchestrator.swarm.creator import swarm

try:
    # Python 3.12+
    from typing import NotRequired, Unpack
except ImportError:
    # Older Python versions
    from typing_extensions import NotRequired, Unpack


class TaskSignatureOptions(TypedDict, total=False):
    kwargs: dict
    workflow_params: dict
    creation_time: datetime
    model_validators: Any
    success_callbacks: list[TaskIdentifierType]
    error_callbacks: list[TaskIdentifierType]
    task_status: TaskStatus
    task_identifiers: dict


@overload
async def sign(
    task: str | HatchetTaskType, **options: Unpack[TaskSignatureOptions]
) -> TaskSignature: ...
@overload
async def sign(task: str | HatchetTaskType, **options: Any) -> TaskSignature: ...


async def sign(task: str | HatchetTaskType, **options: Any) -> TaskSignature:
    if isinstance(task, str):
        return await TaskSignature.from_task_name(task, **options)
    else:
        return await TaskSignature.from_task(task, **options)


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
    "Orchestrator",
    "chain",
    "swarm",
]
