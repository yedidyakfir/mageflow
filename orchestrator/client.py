from typing import TypeVar, Any

from hatchet_sdk import Hatchet, Worker
from hatchet_sdk.runnables.workflow import BaseWorkflow
from hatchet_sdk.worker.worker import LifespanFn

from orchestrator.callbacks import AcceptParams, register_task, handle_task_callback
from orchestrator.init import init_orchestrator_hatchet_tasks
from orchestrator.startup import lifespan_initialize


class HatchetOrchestrator(Hatchet):
    def __init__(
        self, hatchet: Hatchet, param_config: AcceptParams = AcceptParams.NO_CTX
    ):
        super().__init__(client=hatchet._client)
        self.hatchet = hatchet
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


T = TypeVar("T")


def Orchestrator(
    hatchet_client: T = None, param_config: AcceptParams = AcceptParams.NO_CTX
) -> T:
    return HatchetOrchestrator(hatchet_client, param_config)
