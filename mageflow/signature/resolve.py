from mageflow.signature.model import TaskSignature, TaskIdentifierType
from mageflow.signature.types import TaskSignatureConvertible


async def resolve_signature_key(task: TaskSignatureConvertible) -> TaskSignature:
    if isinstance(task, TaskSignature):
        return task
    elif isinstance(task, TaskIdentifierType):
        return await TaskSignature.get_safe(task)
    else:
        return await TaskSignature.from_task(task)
