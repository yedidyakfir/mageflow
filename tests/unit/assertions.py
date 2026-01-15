from typing import TypeVar

from mageflow.chain.consts import ON_CHAIN_ERROR, ON_CHAIN_END
from mageflow.signature.model import TaskSignature
from mageflow.signature.types import TaskIdentifierType

T = TypeVar("T", bound=TaskSignature)


async def assert_single_success_callback(
    task: TaskSignature,
    expected_callback_key: TaskIdentifierType,
) -> None:
    assert len(task.success_callbacks) == 1, (
        f"Expected 1 success callback, got {len(task.success_callbacks)}"
    )
    assert task.success_callbacks[0] == expected_callback_key, (
        f"Expected {expected_callback_key}, got {task.success_callbacks[0]}"
    )


async def assert_single_error_callback_is_chain_error(
    task: TaskSignature,
) -> TaskSignature:
    assert len(task.error_callbacks) == 1, (
        f"Expected 1 error callback, got {len(task.error_callbacks)}"
    )
    error_task = await TaskSignature.get_safe(task.error_callbacks[0])
    assert error_task is not None, f"Error callback {task.error_callbacks[0]} not found"
    assert error_task.task_name == ON_CHAIN_ERROR, (
        f"Expected {ON_CHAIN_ERROR}, got {error_task.task_name}"
    )
    return error_task


async def assert_task_reloaded_as_type(
    task_key: TaskIdentifierType,
    expected_type: type[T],
) -> T:
    reloaded = await TaskSignature.get_safe(task_key)
    assert reloaded is not None, f"Task {task_key} not found"
    assert isinstance(reloaded, expected_type), (
        f"Expected {expected_type.__name__}, got {type(reloaded).__name__}"
    )
    return reloaded


async def assert_success_callback_is_chain_end(
    task: TaskSignature,
) -> TaskSignature:
    assert len(task.success_callbacks) >= 1, "No success callbacks found"
    chain_end_task = await TaskSignature.get_safe(task.success_callbacks[0])
    assert chain_end_task is not None, (
        f"Success callback {task.success_callbacks[0]} not found"
    )
    assert chain_end_task.task_name == ON_CHAIN_END, (
        f"Expected {ON_CHAIN_END}, got {chain_end_task.task_name}"
    )
    return chain_end_task


async def assert_all_error_callbacks_are_chain_error(
    error_callback_ids: list[TaskIdentifierType],
) -> list[TaskSignature]:
    error_tasks = []
    for error_id in error_callback_ids:
        error_task = await TaskSignature.get_safe(error_id)
        assert error_task is not None, f"Error callback {error_id} not found"
        assert error_task.task_name == ON_CHAIN_ERROR, (
            f"Expected {ON_CHAIN_ERROR}, got {error_task.task_name}"
        )
        error_tasks.append(error_task)
    return error_tasks


def assert_callback_contains(
    task: TaskSignature,
    success_key: TaskIdentifierType,
    error_key: TaskIdentifierType,
) -> None:
    assert success_key in task.success_callbacks, (
        f"{success_key} not in success_callbacks"
    )
    assert error_key in task.error_callbacks, f"{error_key} not in error_callbacks"


async def assert_tasks_not_exists(tasks_ids: list[str]):
    for task_id in tasks_ids:
        reloaded_signature = await TaskSignature.get_safe(task_id)
        assert reloaded_signature is None


async def assert_tasks_changed_status(
    tasks_ids: list[str | TaskSignature], status: str, old_status: str = None
):
    tasks_ids = tasks_ids if isinstance(tasks_ids, list) else [tasks_ids]
    all_tasks = []
    for task_key in tasks_ids:
        task_key = task_key.key if isinstance(task_key, TaskSignature) else task_key
        reloaded_signature = await TaskSignature.get_safe(task_key)
        all_tasks.append(reloaded_signature)
        assert reloaded_signature.task_status.status == status
        if old_status:
            assert reloaded_signature.task_status.last_status == old_status
    return all_tasks


async def assert_redis_keys_do_not_contain_sub_task_ids(redis_client, sub_task_ids):
    all_keys = await redis_client.keys("*")
    all_keys_str = [
        key.decode() if isinstance(key, bytes) else str(key) for key in all_keys
    ]

    for sub_task_id in sub_task_ids:
        sub_task_id_str = str(sub_task_id)
        keys_containing_sub_task = [
            key for key in all_keys_str if sub_task_id_str in key
        ]
        assert (
            not keys_containing_sub_task
        ), f"Found Redis keys containing deleted sub-task ID {sub_task_id}: {keys_containing_sub_task}"
