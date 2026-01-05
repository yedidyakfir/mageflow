from datetime import datetime
from typing import TypeAlias, TypedDict, Any, overload

from mageflow.root.model import RootTaskSignature
from mageflow.signature.model import (
    TaskSignature,
    TaskIdentifierType,
    HatchetTaskType,
)
from mageflow.signature.status import TaskStatus
from mageflow.swarm.model import SwarmConfig
from mageflow.task.model import HatchetTaskModel

TaskSignatureConvertible: TypeAlias = (
    TaskIdentifierType | TaskSignature | HatchetTaskType
)


async def resolve_signature_key(task: TaskSignatureConvertible) -> TaskSignature:
    if isinstance(task, TaskSignature):
        return task
    elif isinstance(task, TaskIdentifierType):
        return await TaskSignature.get_safe(task)
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
    task_name = task if isinstance(task, str) else task.name

    task_model = await HatchetTaskModel.safe_get(task_name)
    is_root = task_model and task_model.is_root_task

    signature_class = RootTaskSignature if is_root else TaskSignature

    model_fields = list(signature_class.model_fields.keys())
    kwargs = {
        field_name: options.pop(field_name)
        for field_name in model_fields
        if field_name in options
    }

    if is_root and task_model.root_task_config:
        kwargs["swarm_config"] = SwarmConfig(**task_model.root_task_config)

    if isinstance(task, str):
        return await signature_class.from_task_name(task, kwargs=options, **kwargs)
    else:
        return await signature_class.from_task(task, kwargs=options, **kwargs)


load_signature = TaskSignature.get_safe
resume_task = TaskSignature.resume_from_key
lock_task = TaskSignature.lock_from_key
resume = TaskSignature.resume_from_key
pause = TaskSignature.pause_from_key
