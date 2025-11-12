from orchestrator.callbacks import register_task, handle_task_callback
from orchestrator.chain.creator import chain
from orchestrator.client import Orchestrator
from orchestrator.init import init_orchestrator_hatchet_tasks
from orchestrator.signature.creator import sign
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.status import TaskStatus
from orchestrator.signature.types import HatchetTaskType, TaskIdentifierType
from orchestrator.startup import init_from_dynaconf
from orchestrator.swarm.creator import swarm


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
