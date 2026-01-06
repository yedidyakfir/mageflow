import asyncio
import uuid

from mageflow.signature.creator import (
    TaskSignatureConvertible,
    TaskSignatureOptions,
)
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from rapyer.typing_support import Unpack


class SignatureOptions(TaskSignatureOptions):
    is_swarm_closed: bool
    config: SwarmConfig
    task_kwargs: dict


async def swarm(
    tasks: list[TaskSignatureConvertible] = None,
    task_name: str = None,
    **kwargs: Unpack[SignatureOptions],
) -> SwarmTaskSignature:
    tasks = tasks or []
    task_name = task_name or f"swarm-task-{uuid.uuid4()}"
    swarm_signature = SwarmTaskSignature(**kwargs, task_name=task_name)
    await swarm_signature.save()
    await asyncio.gather(*[swarm_signature.add_task(task) for task in tasks])
    return swarm_signature
