import rapyer
import redis
from hatchet_sdk import Hatchet, ClientConfig
from pydantic import BaseModel
from rapyer.base import REDIS_MODELS
from redis.asyncio.client import Redis

from config import settings
from orchestrator.task.model import HatchetTaskModel

REGISTERED_TASKS: list[tuple] = []


class ConfigModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class OrchestratorConfigModel(ConfigModel):
    hatchet_client: Hatchet | None = None
    redis_client: Redis | None = None

    def set_from_dynaconf(self):
        if settings.redis and settings.hatchet:
            self.redis_client = redis.asyncio.from_url(settings.redis.url)
            token = settings.hatchet.api_key

            config_obj = ClientConfig(token=token, **settings.hatchet.to_dict())
            self.hatchet_client = Hatchet(debug=True, config=config_obj)


orchestrator_config = OrchestratorConfigModel()


async def init_from_dynaconf():
    orchestrator_config.set_from_dynaconf()
    await init_orchestrator()


async def init_orchestrator():
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
    await init_from_dynaconf()
    # yield makes the function usable as a Hatchet lifespan context manager (can alos be used for FastAPI):
    # - code before yield runs at startup (init config, register workers, etc.)
    # - code after yield would run at shutdown
    yield
