import asyncio
import uuid

from orchestrator.signature.creator import TaskSignatureConvertible
from orchestrator.swarm.model import SwarmTaskSignature


async def swarm(
    tasks: list[TaskSignatureConvertible] = None, task_name: str = None, **kwargs
) -> SwarmTaskSignature:
    tasks = tasks or []
    task_name = task_name or f"swarm-task-{uuid.uuid4()}"
    swarm_signature = SwarmTaskSignature(**kwargs, task_name=task_name)
    await swarm_signature.save()
    await asyncio.gather(*[swarm_signature.add_task(task) for task in tasks])
    return swarm_signature
