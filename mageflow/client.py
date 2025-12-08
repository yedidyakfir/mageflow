import functools
import os
from typing import TypeVar, Any, overload, Unpack

import redis
from hatchet_sdk import Hatchet, Worker
from hatchet_sdk.runnables.workflow import BaseWorkflow
from hatchet_sdk.worker.worker import LifespanFn
from redis.asyncio import Redis

from mageflow.callbacks import AcceptParams, register_task, handle_task_callback
from mageflow.chain.creator import chain
from mageflow.init import init_mageflow_hatchet_tasks
from mageflow.signature.creator import sign, TaskSignatureConvertible
from mageflow.signature.model import TaskSignature, TaskInputType
from mageflow.signature.types import HatchetTaskType
from mageflow.startup import (
    lifespan_initialize,
    mageflow_config,
    init_mageflow,
    teardown_mageflow,
)
from mageflow.swarm.creator import swarm, SignatureOptions


async def merge_lifespan(original_lifespan: LifespanFn):
    await init_mageflow()
    async for res in original_lifespan():
        yield res
    await teardown_mageflow()


class HatchetMageflow(Hatchet):
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
        mageflow_flows = init_mageflow_hatchet_tasks(self.hatchet)
        workflows += mageflow_flows
        if lifespan is None:
            lifespan = lifespan_initialize
        else:
            lifespan = functools.partial(merge_lifespan, lifespan)

        return super().worker(*args, workflows=workflows, lifespan=lifespan, **kwargs)

    async def sign(self, task: str | HatchetTaskType, **options: Any) -> TaskSignature:
        return await sign(task, **options)

    async def chain(
        self,
        tasks: list[TaskSignatureConvertible],
        name: str = None,
        error: TaskInputType = None,
        success: TaskInputType = None,
    ):
        return await chain(tasks, name, error, success)

    async def swarm(
        self,
        tasks: list[TaskSignatureConvertible] = None,
        task_name: str = None,
        **kwargs: Unpack[SignatureOptions],
    ):
        return await swarm(tasks, task_name, **kwargs)


T = TypeVar("T")


@overload
def Mageflow(
    hatchet_client: Hatchet, redis_client: Redis | str = None
) -> HatchetMageflow: ...


def Mageflow(
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
    mageflow_config.hatchet_client = hatchet_caller

    if redis_client is None:
        redis_url = os.getenv("REDIS_URL")
        redis_client = redis.asyncio.from_url(redis_url, decode_responses=True)
    if isinstance(redis_client, str):
        redis_client = redis.asyncio.from_url(redis_client, decode_responses=True)
    mageflow_config.redis_client = redis_client
    return HatchetMageflow(hatchet_client, redis_client, param_config)
