from typing import TypeVar

from hatchet_sdk import Hatchet

from orchestrator import handle_task_callback
from orchestrator.callbacks import AcceptParams, register_task


class HatchetOrchestrator(Hatchet):
    hatchet = None
    param_config = None  # To support the __getattribute__func

    def __init__(
        self, hatchet: Hatchet, param_config: AcceptParams = AcceptParams.NO_CTX
    ):
        super().__init__(client=hatchet._client)
        self.hatchet = hatchet
        self.param_config = param_config

    def __getattribute__(self, item):
        cls = object.__getattribute__(self, "__class__")
        if item in cls.__dict__:
            return object.__getattribute__(self, item)
        else:
            hatchet = object.__getattribute__(self, "hatchet")
            return getattr(hatchet, item)

    def task(self, *, name: str | None = None, **kwargs):
        hatchet_task = super().task(name=name, **kwargs)

        def decorator(func):
            handler_dec = handle_task_callback(self.param_config)
            func = handler_dec(func)
            wf = hatchet_task(func)
            register = register_task(name)
            return register(wf)

        return decorator


T = TypeVar("T")


def Orchestrator(
    hatchet_client: T = None, param_config: AcceptParams = AcceptParams.NO_CTX
) -> T:
    return HatchetOrchestrator(hatchet_client, param_config)
