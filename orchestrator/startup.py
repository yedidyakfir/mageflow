import rapyer
from hatchet_sdk import Hatchet
from pydantic import BaseModel
from rapyer.base import REDIS_MODELS
from redis.asyncio.client import Redis

from orchestrator.task.model import HatchetTaskModel

REGISTERED_TASKS: list[tuple] = []


class ConfigModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class OrchestratorConfigModel(ConfigModel):
    hatchet_client: Hatchet | None = None
    redis_client: Redis | None = None


orchestrator_config = OrchestratorConfigModel()


async def init_orchestrator():
    await rapyer.init_rapyer(orchestrator_config.redis_client)
    await register_workflows()
    await update_register_signature_models()


async def update_register_signature_models():
    from orchestrator.signature.model import SIGNATURES_NAME_MAPPING, TaskSignature

    await rapyer.init_rapyer(orchestrator_config.redis_client)

    signature_classes = [cls for cls in REDIS_MODELS if issubclass(cls, TaskSignature)]
    SIGNATURES_NAME_MAPPING.update(
        {
            signature_class.__name__: signature_class
            for signature_class in signature_classes
        }
    )


async def register_workflows():
    for reg_task in REGISTERED_TASKS:
        workflow, orchestrator_task_name = reg_task
        hatchet_task = HatchetTaskModel(
            orchestrator_task_name=orchestrator_task_name,
            task_name=workflow.name,
            input_validator=workflow.input_validator,
        )
        await hatchet_task.save()


async def lifespan_initialize():
    await init_orchestrator()
    # yield makes the function usable as a Hatchet lifespan context manager (can also be used for FastAPI):
    # - code before yield runs at startup (init config, register workers, etc.)
    # - code after yield would run at shutdown
    yield
