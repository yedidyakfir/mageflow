import asyncio

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel
from pydantic import BaseModel

from mageflow.errors import MissingSwarmItemError
from mageflow.invokers.hatchet import HatchetInvoker
from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.swarm.consts import (
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
)
from mageflow.swarm.messages import SwarmResultsMessage
from mageflow.swarm.model import SwarmTaskSignature


async def swarm_start_tasks(msg: EmptyModel, ctx: Context):
    try:
        ctx.log(f"Swarm task started {msg}")
        task_data = HatchetInvoker(msg, ctx).task_ctx
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_task = await SwarmTaskSignature.get_safe(swarm_task_id)

        if swarm_task is None:
            ctx.log(f"MAJOR - Swarm {swarm_task_id} not found in Redis!")
            raise MissingSwarmItemError(f"Swarm {swarm_task_id} not found")

        ctx.log(
            f"Swarm state: total_tasks={len(swarm_task.tasks)} "
            f"max_concurrency={swarm_task.config.max_concurrency} "
            f"is_closed={swarm_task.is_swarm_closed}"
        )

        if swarm_task.has_swarm_started:
            ctx.log(f"Swarm task started but already running {msg}")
            return

        tasks_ids_to_run = swarm_task.tasks[: swarm_task.config.max_concurrency]
        tasks_left_to_run = swarm_task.tasks[swarm_task.config.max_concurrency :]

        ctx.log(
            f"Initial batch: {len(tasks_ids_to_run)} tasks to run immediately, "
            f"{len(tasks_left_to_run)} tasks queued"
        )

        async with swarm_task.pipeline() as swarm_task:
            await swarm_task.tasks_left_to_run.aclear()
            await swarm_task.tasks_left_to_run.aextend(tasks_left_to_run)

        tasks_to_run = await asyncio.gather(
            *[TaskSignature.get_safe(task_id) for task_id in tasks_ids_to_run]
        )

        # Check for missing tasks
        missing_ids = [
            task_id for task_id, task in zip(tasks_ids_to_run, tasks_to_run)
            if task is None
        ]
        if missing_ids:
            ctx.log(f"WARN: {len(missing_ids)} initial tasks not found: {missing_ids}")

        valid_tasks = [t for t in tasks_to_run if t is not None]
        ctx.log(f"Publishing {len(valid_tasks)} initial tasks")

        await asyncio.gather(*[task.aio_run_no_wait(msg) for task in valid_tasks])
        ctx.log(f"Swarm task started with {len(valid_tasks)} tasks {msg}")
    except Exception:
        ctx.log(f"MAJOR - Error in swarm start tasks")
        raise


async def swarm_item_done(msg: SwarmResultsMessage, ctx: Context):
    task_data = HatchetInvoker(msg, ctx).task_ctx
    task_id = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_item_id = task_data[SWARM_ITEM_TASK_ID_PARAM_NAME]
        ctx.log(f"Swarm item done: item={swarm_item_id} swarm={swarm_task_id}")

        # Update swarm tasks
        swarm_task = await SwarmTaskSignature.get_safe(swarm_task_id)
        if swarm_task is None:
            ctx.log(f"MAJOR - Swarm {swarm_task_id} not found! Item {swarm_item_id} completion lost!")
            raise MissingSwarmItemError(f"Swarm {swarm_task_id} not found for item {swarm_item_id}")

        res = msg.results
        async with swarm_task.lock(save_at_end=False) as swarm_task:
            ctx.log(f"Swarm item done {swarm_item_id} - saving results (lock acquired)")
            await swarm_task.finished_tasks.aappend(swarm_item_id)
            await swarm_task.tasks_results.aappend(res)
            ctx.log(f"Swarm item {swarm_item_id} added to finished_tasks")
            await handle_finish_tasks(swarm_task, ctx, msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm item done: {type(e).__name__}: {e}")
        raise
    finally:
        await TaskSignature.try_remove(task_id)


async def swarm_item_failed(msg: EmptyModel, ctx: Context):
    task_data = HatchetInvoker(msg, ctx).task_ctx
    task_key = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_key = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_item_key = task_data[SWARM_ITEM_TASK_ID_PARAM_NAME]
        ctx.log(f"Swarm item failed: item={swarm_item_key} swarm={swarm_task_key}")

        # Check if the swarm should end
        swarm_task = await SwarmTaskSignature.get_safe(swarm_task_key)
        if swarm_task is None:
            ctx.log(f"MAJOR - Swarm {swarm_task_key} not found! Item {swarm_item_key} failure lost!")
            raise MissingSwarmItemError(f"Swarm {swarm_task_key} not found for failed item {swarm_item_key}")

        async with swarm_task.lock(save_at_end=False) as swarm_task:
            await swarm_task.add_to_failed_tasks(swarm_item_key)
            ctx.log(f"Swarm item {swarm_item_key} added to failed_tasks (total failed: {len(swarm_task.failed_tasks)})")

            should_stop_after_failures = (
                swarm_task.config.stop_after_n_failures is not None
            )
            stop_after_n_failures = swarm_task.config.stop_after_n_failures or 0
            too_many_errors = len(swarm_task.failed_tasks) >= stop_after_n_failures
            if should_stop_after_failures and too_many_errors:
                ctx.log(
                    f"Swarm item failed - stopping swarm {swarm_task.key} after {len(swarm_task.failed_tasks)} failures"
                )
                await swarm_task.change_status(SignatureStatus.CANCELED)
                await swarm_task.activate_error(EmptyModel())
                await swarm_task.remove(with_error=False)
                ctx.log(f"Swarm item failed - stopped swarm {swarm_task.key}")
                return

            await handle_finish_tasks(swarm_task, ctx, msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm item failed: {type(e).__name__}: {e}")
        raise
    finally:
        await TaskSignature.try_remove(task_key)


async def handle_finish_tasks(
    swarm_task: SwarmTaskSignature, ctx: Context, msg: BaseModel
):
    # Log current state before decrementing
    ctx.log(
        f"handle_finish_tasks: swarm={swarm_task.key} "
        f"running={swarm_task.current_running_tasks} "
        f"finished={len(swarm_task.finished_tasks)} "
        f"failed={len(swarm_task.failed_tasks)} "
        f"queued={len(swarm_task.tasks_left_to_run)} "
        f"total={len(swarm_task.tasks)} "
        f"closed={swarm_task.is_swarm_closed}"
    )

    await swarm_task.decrease_running_tasks_count()
    num_task_started = await swarm_task.fill_running_tasks(logger=ctx.log)
    if num_task_started:
        ctx.log(f"Swarm item started new task {num_task_started}/{swarm_task.key}")
    else:
        ctx.log(f"Swarm item no new task to run in {swarm_task.key}")

    # Check if the swarm should end
    is_done = await swarm_task.is_swarm_done()
    ctx.log(f"is_swarm_done check: {is_done} (closed={swarm_task.is_swarm_closed})")

    if is_done:
        ctx.log(f"Swarm item done - closing swarm {swarm_task.key}")
        await swarm_task.activate_success(msg)
        ctx.log(f"Swarm item done - closed swarm {swarm_task.key}")
    else:
        # Log why swarm is not done yet
        done_tasks = set(swarm_task.finished_tasks) | set(swarm_task.failed_tasks)
        missing = set(swarm_task.tasks) - done_tasks
        ctx.log(
            f"Swarm not done yet: {len(missing)} tasks remaining. "
            f"Missing task IDs: {list(missing)[:5]}{'...' if len(missing) > 5 else ''}"
        )
