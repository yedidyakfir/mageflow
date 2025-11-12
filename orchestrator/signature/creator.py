from datetime import datetime
from typing import TypeAlias, TypedDict, Any, overload

from orchestrator.signature.model import (
    TaskSignature,
    TaskIdentifierType,
    HatchetTaskType,
)
from orchestrator.signature.status import TaskStatus

TaskSignatureConvertible: TypeAlias = (
    TaskIdentifierType | TaskSignature | HatchetTaskType
)


async def resolve_signature_id(task: TaskSignatureConvertible) -> TaskSignature:
    if isinstance(task, TaskSignature):
        return task
    elif isinstance(task, TaskIdentifierType):
        return await TaskSignature.from_id_safe(task)
    else:
        return await TaskSignature.from_task(task)


try:
    # Python 3.12+
    from typing import Unpack
except ImportError:
    # Older Python versions
    from typing_extensions import Unpack


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
