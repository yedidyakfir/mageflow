import functools
from typing import TypeVar, Any

import redis
from hatchet_sdk import Hatchet, Worker
from hatchet_sdk.runnables.workflow import BaseWorkflow
from hatchet_sdk.worker.worker import LifespanFn
from redis.asyncio import Redis

from config import settings
from orchestrator.callbacks import AcceptParams, register_task, handle_task_callback
from orchestrator.init import init_orchestrator_hatchet_tasks
from orchestrator.startup import (
    lifespan_initialize,
    orchestrator_config,
    init_orchestrator,
)


async def merge_lifespan(original_lifespan: LifespanFn):
    await init_orchestrator()
    async for res in original_lifespan():
        yield res


class HatchetOrchestrator(Hatchet):
    def __init__(
        self,
        hatchet: Hatchet,
        redis_client: Redis,
        param_config: AcceptParams = AcceptParams.NO_CTX,
    ):
        super().__init__(client=hatchet._client)
        self.hatchet = hatchet
        self.redis = redis_client
        self.param_config = param_config

    def task(self, *, name: str | None = None, **kwargs):
        hatchet_task = super().task(name=name, **kwargs)

        def decorator(func):
            handler_dec = handle_task_callback(self.param_config)
            func = handler_dec(func)
            wf = hatchet_task(func)

            nonlocal name
            task_name = name or func.__name__
            register = register_task(task_name)
            return register(wf)

        return decorator

    def durable_task(self, *, name: str | None = None, **kwargs):
        hatchet_task = super().durable_task(name=name, **kwargs)

        def decorator(func):
            handler_dec = handle_task_callback(self.param_config)
            func = handler_dec(func)
            wf = hatchet_task(func)
            nonlocal name
            task_name = name or func.__name__
            register = register_task(task_name)
            return register(wf)

        return decorator

    def worker(
        self,
        *args,
        workflows: list[BaseWorkflow[Any]] | None = None,
        lifespan: LifespanFn | None = None,
        **kwargs,
    ) -> Worker:
        orchestrator_flows = init_orchestrator_hatchet_tasks(self.hatchet)
        workflows += orchestrator_flows
        if lifespan is None:
            lifespan = lifespan_initialize
        else:
            lifespan = functools.partial(merge_lifespan, lifespan)

        return super().worker(*args, workflows=workflows, lifespan=lifespan, **kwargs)


T = TypeVar("T")


def Orchestrator(
    hatchet_client: T = None,
    redis_client: Redis | str = None,
    param_config: AcceptParams = AcceptParams.NO_CTX,
) -> T:
    if hatchet_client is None:
        hatchet_client = Hatchet()

    # Create a hatchet client with empty namespace for creating wf
    config = hatchet_client._client.config.model_copy(deep=True)
    config.namespace = ""
    hatchet_caller = Hatchet(config=config, debug=hatchet_client._client.debug)
    orchestrator_config.hatchet_client = hatchet_caller

    if redis_client is None:
        redis_client = redis.asyncio.from_url(settings.redis.url)
    if isinstance(redis_client, str):
        redis_client = redis.asyncio.from_url(redis_client, max_connections=10)
    orchestrator_config.redis_client = redis_client
    return HatchetOrchestrator(hatchet_client, redis_client, param_config)
