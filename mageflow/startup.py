from typing import Optional, Any

import rapyer
from hatchet_sdk import Hatchet
from hatchet_sdk.runnables.workflow import Standalone
from mageflow.task.model import HatchetTaskModel
from pydantic import BaseModel
from redis.asyncio.client import Redis

REGISTERED_TASKS: list[tuple[Standalone, str, bool, Optional[Any]]] = []


class ConfigModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class MageFlowConfigModel(ConfigModel):
    hatchet_client: Hatchet | None = None
    redis_client: Redis | None = None


mageflow_config = MageFlowConfigModel()


async def init_mageflow():
    await rapyer.init_rapyer(mageflow_config.redis_client)
    await register_workflows()
    await update_register_signature_models()


async def teardown_mageflow():
    await rapyer.teardown_rapyer()


async def update_register_signature_models():
    from mageflow.signature.model import SIGNATURES_NAME_MAPPING, TaskSignature

    signature_classes = [
        cls for cls in rapyer.find_redis_models() if issubclass(cls, TaskSignature)
    ]
    SIGNATURES_NAME_MAPPING.update(
        {
            signature_class.__name__: signature_class
            for signature_class in signature_classes
        }
    )


async def register_workflows():
    for reg_task in REGISTERED_TASKS:
        workflow, mageflow_task_name, is_root_task, root_task_config = reg_task
        config_dict = (
            root_task_config.model_dump(mode="json") if root_task_config else None
        )
        hatchet_task = HatchetTaskModel(
            mageflow_task_name=mageflow_task_name,
            task_name=workflow.name,
            input_validator=workflow.input_validator,
            retries=workflow.tasks[0].retries,
            is_root_task=is_root_task,
            root_task_config=config_dict,
        )
        await hatchet_task.save()


async def lifespan_initialize():
    await init_mageflow()
    # yield makes the function usable as a Hatchet lifespan context manager (can also be used for FastAPI):
    # - code before yield runs at startup (init config, register workers, etc.)
    # - code after yield would run at shutdown
    yield
    await teardown_mageflow()
