import asyncio

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel
from pydantic import BaseModel

from orchestrator.errors import MissingSwarmItemError
from orchestrator.invokers.hatchet import HatchetInvoker
from orchestrator.signature.consts import TASK_ID_PARAM_NAME
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.status import SignatureStatus
from orchestrator.swarm.consts import (
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
)
from orchestrator.swarm.messages import SwarmResultsMessage
from orchestrator.swarm.model import SwarmTaskSignature


async def swarm_start_tasks(msg: EmptyModel, ctx: Context) -> None:
    try:
        ctx.log(f"Swarm task started {msg}")
        task_data = HatchetInvoker(msg, ctx).task_ctx
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_task = await SwarmTaskSignature.from_id(swarm_task_id)
        if swarm_task.has_swarm_started:
            ctx.log(f"Swarm task started but already running {msg}")
            return
        tasks_ids_to_run = swarm_task.tasks[: swarm_task.config.max_concurrency]
        tasks_left_to_run = swarm_task.tasks[swarm_task.config.max_concurrency :]
        async with swarm_task.pipeline() as swarm_task:
            await swarm_task.tasks_left_to_run.aclear()
            await swarm_task.tasks_left_to_run.aextend(tasks_left_to_run)
        tasks_to_run = await asyncio.gather(
            *[TaskSignature.from_id(task_id) for task_id in tasks_ids_to_run]
        )
        await asyncio.gather(*[task.aio_run_no_wait(msg) for task in tasks_to_run])
        ctx.log(f"Swarm task started with tasks {tasks_ids_to_run} {msg}")
    except Exception:
        ctx.log(f"MAJOR - Error in swarm start tasks")
        raise


async def swarm_item_done(msg: SwarmResultsMessage, ctx: Context) -> None:
    task_data = HatchetInvoker(msg, ctx).task_ctx
    task_id = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_item_id = task_data[SWARM_ITEM_TASK_ID_PARAM_NAME]
        ctx.log(f"Swarm item done {swarm_item_id}")
        # Update swarm tasks
        swarm_task = await SwarmTaskSignature.from_id(swarm_task_id)
        res = msg.results
        async with swarm_task.lock(save_at_end=False) as swarm_task:
            await swarm_task.finished_tasks.aappend(swarm_item_id)
            await swarm_task.tasks_results.aappend(res)
            await handle_finish_tasks(swarm_task, ctx, msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm start item done")
        raise
    finally:
        await TaskSignature.try_remove(task_id)


async def swarm_item_failed(msg: EmptyModel, ctx: Context) -> None:
    task_data = HatchetInvoker(msg, ctx).task_ctx
    task_id = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_item_id = task_data[SWARM_ITEM_TASK_ID_PARAM_NAME]
        ctx.log(f"Swarm item failed {swarm_item_id}")
        # Check if the swarm should end
        swarm_task = await SwarmTaskSignature.from_id(swarm_task_id)
        async with swarm_task.lock(save_at_end=False) as swarm_task:
            await swarm_task.add_to_failed_tasks(swarm_item_id)
            should_stop_after_failures = (
                swarm_task.config.stop_after_n_failures is not None
            )
            stop_after_n_failures = swarm_task.config.stop_after_n_failures or 0
            too_many_errors = len(swarm_task.failed_tasks) >= stop_after_n_failures
            if should_stop_after_failures and too_many_errors:
                ctx.log(
                    f"Swarm item failed - stopping swarm {swarm_task.id} after {len(swarm_task.failed_tasks)} failures"
                )
                await swarm_task.change_status(SignatureStatus.CANCELED)
                await swarm_task.activate_error(EmptyModel())
                await swarm_task.remove(with_error=False)
                ctx.log(f"Swarm item failed - stopped swarm {swarm_task.id}")
                return

            await handle_finish_tasks(swarm_task, ctx, msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm item failed")
        raise
    finally:
        await TaskSignature.try_remove(task_id)


async def handle_finish_tasks(
    swarm_task: SwarmTaskSignature, ctx: Context, msg: BaseModel
):
    next_task = await swarm_task.pop_task_to_run()
    await swarm_task.decrease_running_tasks_count()
    if next_task:
        next_task_signature = await TaskSignature.from_id(next_task)
        if next_task_signature is None:
            raise MissingSwarmItemError(
                f"swarm item {next_task} was deleted before swarm is done"
            )
        # The message is already stored in the task signature
        await next_task_signature.aio_run_no_wait(EmptyModel())
        ctx.log(f"Swarm item started new task {next_task}/{swarm_task.id}")
    else:
        ctx.log(f"Swarm item no new task to run in {swarm_task.id}")

    # Check if the swarm should end
    if await swarm_task.is_swarm_done():
        ctx.log(f"Swarm item done - closing swarm {swarm_task.id}")
        await swarm_task.activate_success(msg)
        ctx.log(f"Swarm item done - closed swarm {swarm_task.id}")
