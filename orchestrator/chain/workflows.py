import asyncio

from hatchet_sdk import Context
from hatchet_sdk.runnables.types import EmptyModel

from orchestrator.chain.consts import CHAIN_TASK_ID_NAME
from orchestrator.chain.messages import ChainSuccessTaskCommandMessage
from orchestrator.chain.model import ChainTaskSignature
from orchestrator.invokers.hatchet import HatchetInvoker
from orchestrator.signature.consts import TASK_ID_PARAM_NAME
from orchestrator.signature.model import TaskSignature


async def chain_end_task(msg: ChainSuccessTaskCommandMessage, ctx: Context) -> None:
    try:
        task_data = HatchetInvoker(msg, ctx).task_ctx
        chain_task_id = task_data[CHAIN_TASK_ID_NAME]
        current_task_id = task_data[TASK_ID_PARAM_NAME]

        chain_task_signature, current_task = await asyncio.gather(
            ChainTaskSignature.from_id(chain_task_id),
            TaskSignature.from_id(current_task_id),
        )
        ctx.log(f"Chain task done {chain_task_signature.task_name}")

        # Calling error callback from a chain task - This is done before deletion because a deletion error should not disturb the workflow
        await chain_task_signature.activate_success(msg.chain_results)
        ctx.log(f"Chain task success {chain_task_signature.task_name}")

        # Remove tasks
        await asyncio.gather(
            chain_task_signature.remove(with_success=False), current_task.remove()
        )
    except Exception as e:
        ctx.log(f"MAJOR - infrastructure error in chain end task: {e}")
        raise


# This task needs to be added as a workflow
async def chain_error_task(msg: EmptyModel, ctx: Context) -> None:
    try:
        task_data = HatchetInvoker(msg, ctx).task_ctx
        chain_task_id = task_data[CHAIN_TASK_ID_NAME]
        current_task_id = task_data[TASK_ID_PARAM_NAME]
        chain_packed_task, current_task = await asyncio.gather(
            ChainTaskSignature.from_id(chain_task_id),
            TaskSignature.from_id(current_task_id),
        )
        ctx.log(
            f"Chain task failed {chain_packed_task.task_name} on task id - {current_task_id}"
        )

        # Calling error callback from chain task
        await chain_packed_task.activate_error(msg)
        ctx.log(f"Chain task error {chain_packed_task.task_name}")

        # Remove tasks
        await chain_packed_task.delete_chain_tasks()
        await asyncio.gather(
            chain_packed_task.remove(with_error=False), current_task.remove()
        )
        ctx.log(f"Clean redis from chain tasks {chain_packed_task.task_name}")
    except Exception as e:
        ctx.log(f"MAJOR - infrastructure error in chain error task: {e}")
        raise
