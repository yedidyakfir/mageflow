import asyncio

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel

from mageflow.invokers.hatchet import HatchetInvoker
from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.swarm.consts import (
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
    SWARM_FILL_TASK,
)
from mageflow.swarm.messages import SwarmResultsMessage, SwarmMessage
from mageflow.swarm.model import (
    SwarmTaskSignature,
    BatchItemTaskSignature,
    DONE_AND_UPDATED_SWARM,
)


async def swarm_start_tasks(msg: EmptyModel, ctx: Context):
    try:
        ctx.log(f"Swarm task started {msg}")
        task_data = HatchetInvoker(msg, ctx).task_ctx
        swarm_task_id = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_task = await SwarmTaskSignature.get_safe(swarm_task_id)
        if swarm_task.has_swarm_started:
            ctx.log(f"Swarm task started but already running {msg}")
            return
        tasks_ids_to_run = swarm_task.tasks[: swarm_task.config.max_concurrency]
        tasks_left_to_run = swarm_task.tasks[swarm_task.config.max_concurrency :]
        async with swarm_task.apipeline() as swarm_task:
            await swarm_task.tasks_left_to_run.aclear()
            await swarm_task.tasks_left_to_run.aextend(tasks_left_to_run)
        tasks_to_run = await asyncio.gather(
            *[TaskSignature.get_safe(task_id) for task_id in tasks_ids_to_run]
        )
        await asyncio.gather(*[task.aio_run_no_wait(msg) for task in tasks_to_run])
        ctx.log(f"Swarm task started with tasks {tasks_ids_to_run} {msg}")
    except Exception:
        ctx.log(f"MAJOR - Error in swarm start tasks")
        raise


async def swarm_item_done(msg: SwarmResultsMessage, ctx: Context):
    invoker = HatchetInvoker(msg, ctx)
    task_data = invoker.task_ctx
    task_id = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_id = msg.swarm_task_id
        swarm_item_id = msg.swarm_item_id
        ctx.log(f"Swarm item done {swarm_item_id}")
        # Update swarm tasks
        swarm_task, batch_task = await asyncio.gather(
            SwarmTaskSignature.get_safe(swarm_task_id),
            BatchItemTaskSignature.get_safe(swarm_item_id),
        )
        res = msg.results
        async with swarm_task.apipeline() as swarm_task:
            ctx.log(f"Swarm item done {swarm_item_id} - saving results")
            swarm_task.finished_tasks.append(swarm_item_id)
            swarm_task.tasks_results.append(res)
            swarm_task.current_running_tasks -= 1
            batch_task.item_status = DONE_AND_UPDATED_SWARM
        # await
        fill_swarm_msg = SwarmMessage(swarm_task_id=swarm_task_id)
        await invoker.wait_task(SWARM_FILL_TASK, fill_swarm_msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm start item done")
        raise
    finally:
        await TaskSignature.try_remove(task_id)


async def swarm_item_failed(msg: EmptyModel, ctx: Context):
    task_data = HatchetInvoker(msg, ctx).task_ctx
    task_key = task_data[TASK_ID_PARAM_NAME]
    try:
        swarm_task_key = task_data[SWARM_TASK_ID_PARAM_NAME]
        swarm_item_key = task_data[SWARM_ITEM_TASK_ID_PARAM_NAME]
        ctx.log(f"Swarm item failed {swarm_item_key}")
        # Check if the swarm should end
        swarm_task = await SwarmTaskSignature.get_safe(swarm_task_key)
        async with swarm_task.lock(save_at_end=False) as swarm_task:
            await swarm_task.add_to_failed_tasks(swarm_item_key)
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

            await fill_swarm_running_tasks(swarm_task, ctx, msg)
    except Exception as e:
        ctx.log(f"MAJOR - Error in swarm item failed")
        raise
    finally:
        await TaskSignature.try_remove(task_key)


async def fill_swarm_running_tasks(msg: SwarmMessage, ctx: Context):
    swarm_task = await SwarmTaskSignature.aget(msg.swarm_task_id)
    num_task_started = await swarm_task.fill_running_tasks()
    if num_task_started:
        ctx.log(f"Swarm item started new task {num_task_started}/{swarm_task.key}")
    else:
        ctx.log(f"Swarm item no new task to run in {swarm_task.key}")

    # Check if the swarm should end
    if await swarm_task.is_swarm_done() and swarm_task.has_published_callback():
        ctx.log(f"Swarm item done - closing swarm {swarm_task.key}")
        await swarm_task.activate_success(msg)
        ctx.log(f"Swarm item done - closed swarm {swarm_task.key}")
