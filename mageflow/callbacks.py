import asyncio
import functools
import inspect
from enum import Enum
from typing import Any

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel
from hatchet_sdk.runnables.workflow import Standalone
from mageflow.invokers.hatchet import HatchetInvoker
from mageflow.task.model import HatchetTaskModel
from mageflow.utils.pythonic import flexible_call
from pydantic import BaseModel


class AcceptParams(Enum):
    JUST_MESSAGE = 1
    NO_CTX = 2
    ALL = 3


class HatchetResult(BaseModel):
    hatchet_results: Any


def handle_task_callback(
    expected_params: AcceptParams = AcceptParams.NO_CTX,
    wrap_res: bool = True,
    send_signature: bool = False,
):
    def task_decorator(func):
        @functools.wraps(func)
        async def wrapper(message: EmptyModel, ctx: Context, *args, **kwargs):
            invoker = HatchetInvoker(message, ctx)
            task_model = await HatchetTaskModel.get(ctx.action.job_name)
            if not await invoker.should_run_task():
                await ctx.aio_cancel()
                await asyncio.sleep(10)
                # NOTE: This should not run, the task should cancel, but just in case
                return {"Error": "Task should have been canceled"}
            try:
                signature = await invoker.start_task()
                if send_signature:
                    kwargs["signature"] = signature
                if expected_params == AcceptParams.JUST_MESSAGE:
                    result = await flexible_call(func, message)
                elif expected_params == AcceptParams.NO_CTX:
                    result = await flexible_call(func, message, *args, **kwargs)
                else:
                    result = await flexible_call(func, message, ctx, *args, **kwargs)
            except (Exception, asyncio.CancelledError) as e:
                if not task_model.should_retry(ctx.attempt_number, e):
                    await invoker.run_error()
                    await invoker.remove_task(with_error=False)
                raise
            else:
                await invoker.end_task()
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


def register_task(
    register_name: str,
    is_root_task: bool = False,
    root_task_config=None,
):
    from mageflow.startup import REGISTERED_TASKS

    def decorator(func: Standalone):
        REGISTERED_TASKS.append((func, register_name, is_root_task, root_task_config))
        return func

    return decorator
