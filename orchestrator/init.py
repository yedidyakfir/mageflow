import rapyer
import redis
from hatchet_sdk import Hatchet, ClientConfig
from pydantic import BaseModel
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

            config_obj = ClientConfig(token=token, **dict(settings.hatchet))
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

    # TODO - use the redis list
    signature_classes = [TaskSignature]
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


def init_orchestrator_hatchet_tasks(hatchet: Hatchet):
    # Chain tasks
    # hatchet_chain_done = hatchet.task(
    #     name=InfrastructureTasks.on_chain_done,
    #     input_validator=ChainSuccessTaskCommandMessage,
    # )
    # hatchet_chain_error = hatchet.task(name=InfrastructureTasks.on_chain_error)
    # chain_done_task = hatchet_chain_done(chain_end_task)
    # on_chain_error_task = hatchet_chain_error(chain_error_task)
    # register_chain_done = register_task(InfrastructureTasks.on_chain_done)
    # register_chain_error = register_task(InfrastructureTasks.on_chain_error)
    # chain_done_task = register_chain_done(chain_done_task)
    # on_chain_error_task = register_chain_error(on_chain_error_task)

    # Swarm tasks
    # swarm_start = hatchet.task(
    #     name=InfrastructureTasks.on_swarm_start,
    #     input_validator=SwarmTaskCommandMessage,
    # )
    # swarm_done = hatchet.task(
    #     name=InfrastructureTasks.on_swarm_done,
    #     input_validator=SwarmTaskCommandMessage,
    # )
    # swarm_error = hatchet.task(
    #     name=InfrastructureTasks.on_swarm_error,
    #     input_validator=SwarmFailedCommandMessage,
    # )
    # swarm_start = swarm_start(swarm_start_tasks)
    # swarm_done = swarm_done(swarm_item_done)
    # swarm_error = swarm_error(swarm_item_failed)
    # register_swarm_start = register_task(InfrastructureTasks.on_swarm_start)
    # register_swarm_done = register_task(InfrastructureTasks.on_swarm_done)
    # register_swarm_error = register_task(InfrastructureTasks.on_swarm_error)
    # swarm_start = register_swarm_start(swarm_start)
    # swarm_done = register_swarm_done(swarm_done)
    # swarm_error = register_swarm_error(swarm_error)

    return [
        # on_chain_error_task,
        # chain_done_task,
        # swarm_start,
        # swarm_done,
        # swarm_error,
    ]
