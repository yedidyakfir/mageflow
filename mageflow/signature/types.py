from datetime import datetime
from typing import Callable, TYPE_CHECKING, TypeAlias, Any, TypedDict

from hatchet_sdk.runnables.workflow import BaseWorkflow
from mageflow.signature.status import TaskStatus

if TYPE_CHECKING:
    from mageflow.signature.model import TaskSignature

TaskIdentifierType = str
HatchetTaskType = BaseWorkflow | Callable

TaskSignatureConvertible: TypeAlias = (
    "TaskIdentifierType | TaskSignature | HatchetTaskType"
)


class TaskSignatureOptions(TypedDict, total=False):
    kwargs: dict
    creation_time: datetime
    model_validators: Any
    success_callbacks: list[TaskIdentifierType]
    error_callbacks: list[TaskIdentifierType]
    task_status: TaskStatus
    task_identifiers: dict
