import asyncio
import functools
import inspect
from enum import Enum
from typing import Any

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel
from pydantic import BaseModel

from orchestrator.invokers.hatchet import HatchetInvoker
from orchestrator.utils.pythonic import flexible_call


class AcceptParams(Enum):
    JUST_MESSAGE = 1
    NO_CTX = 2
    ALL = 3


class HatchetResult(BaseModel):
    hatchet_results: Any


def handle_task_callback(
    expected_params: AcceptParams = AcceptParams.NO_CTX, wrap_res: bool = True
):
    def task_decorator(func):
        @functools.wraps(func)
        async def wrapper(message: EmptyModel, ctx: Context, *args, **kwargs):
            invoker = HatchetInvoker(message, ctx)
            try:
                if not await invoker.should_run_task():
                    await ctx.aio_cancel()
                    await asyncio.sleep(10)
                    # NOTE: This should not run, the task should cancel, but just in case
                    return {"Error": "Task should have been canceled"}
                await invoker.start_task()
                if expected_params == AcceptParams.JUST_MESSAGE:
                    result = await flexible_call(func, message)
                elif expected_params == AcceptParams.NO_CTX:
                    result = await flexible_call(func, message, *args, **kwargs)
                else:
                    result = await flexible_call(func, message, ctx, *args, **kwargs)
            except Exception:
                await invoker.run_error()
                await invoker.remove_task(with_error=False)
                raise
            else:
                task_results = HatchetResult(hatchet_results=result)
                dumped_results = task_results.model_dump(mode="json")
                await invoker.run_success(dumped_results["hatchet_results"])
                await invoker.remove_task(with_success=False)
                if wrap_res:
                    return task_results
                else:
                    return result

        wrapper.__signature__ = inspect.signature(func)
        return wrapper

    return task_decorator


def register_task(register_name: str):
    from orchestrator.startup import REGISTERED_TASKS

    def decorator(func):
        REGISTERED_TASKS.append((func, register_name))
        return func

    return decorator
