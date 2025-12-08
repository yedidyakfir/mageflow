from datetime import datetime
from typing import TypeAlias, TypedDict, Any, overload

from mageflow.signature.model import (
    TaskSignature,
    TaskIdentifierType,
    HatchetTaskType,
)
from mageflow.signature.status import TaskStatus

TaskSignatureConvertible: TypeAlias = (
    TaskIdentifierType | TaskSignature | HatchetTaskType
)


async def resolve_signature_id(task: TaskSignatureConvertible) -> TaskSignature:
    if isinstance(task, TaskSignature):
        return task
    elif isinstance(task, TaskIdentifierType):
        return await TaskSignature.from_id(task)
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
    model_fields = list(TaskSignature.model_fields.keys())
    kwargs = {
        field_name: options.pop(field_name)
        for field_name in model_fields
        if field_name in options
    }

    if isinstance(task, str):
        return await TaskSignature.from_task_name(task, kwargs=options, **kwargs)
    else:
        return await TaskSignature.from_task(task, kwargs=options, **kwargs)


load_signature = TaskSignature.from_id
resume_task = TaskSignature.resume_from_id
lock_task = TaskSignature.lock_from_id
resume = TaskSignature.resume_from_id
pause = TaskSignature.pause_from_id
