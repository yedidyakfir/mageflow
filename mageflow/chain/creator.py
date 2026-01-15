import asyncio

from mageflow.chain.consts import ON_CHAIN_END, ON_CHAIN_ERROR
from mageflow.chain.messages import ChainSuccessTaskCommandMessage
from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.creator import (
    TaskSignatureConvertible,
    resolve_signature_key,
)
from mageflow.signature.model import (
    TaskIdentifierType,
    TaskSignature,
    TaskInputType,
)


async def chain(
    tasks: list[TaskSignatureConvertible],
    name: str = None,
    error: TaskInputType = None,
    success: TaskInputType = None,
) -> ChainTaskSignature:
    tasks = [await resolve_signature_key(task) for task in tasks]

    # Create a chain task that will be deleted only at the end of the chain
    first_task = tasks[0]
    chain_task_signature = ChainTaskSignature(
        task_name=f"chain-task:{name or first_task.task_name}",
        success_callbacks=[success] if success else [],
        error_callbacks=[error] if error else [],
        tasks=tasks,
    )
    await chain_task_signature.asave()

    callback_kwargs = dict(chain_task_id=chain_task_signature.key)
    on_chain_error = TaskSignature(
        task_name=ON_CHAIN_ERROR,
        task_identifiers=callback_kwargs,
        model_validators=ChainSuccessTaskCommandMessage,
    )
    on_chain_success = TaskSignature(
        task_name=ON_CHAIN_END,
        task_identifiers=callback_kwargs,
        model_validators=ChainSuccessTaskCommandMessage,
    )
    await _chain_task_to_previous_success(tasks, on_chain_error, on_chain_success)
    return chain_task_signature


async def _chain_task_to_previous_success(
    tasks: list[TaskSignature], error: TaskSignature, success: TaskSignature
) -> TaskIdentifierType:
    """
    Take a list of tasks and connect each one to the previous one.
    """
    if len(tasks) < 2:
        raise ValueError(
            "Chained tasks must contain at least two tasks. "
            "If you want to run a single task, use `create_workflow` instead."
        )

    total_tasks = tasks + [success]
    error_tasks = await error.duplicate_many(len(tasks))
    store_errors = [error.asave() for error in error_tasks]

    # Store tasks
    await asyncio.gather(success.asave(), *store_errors)
    update_tasks = [
        task.add_callbacks(success=[total_tasks[i + 1]], errors=[error_tasks[i]])
        for i, task in enumerate(tasks)
    ]
    chained_tasks = await asyncio.gather(*update_tasks)
    return chained_tasks[0]
