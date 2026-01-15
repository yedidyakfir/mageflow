import pytest
from hatchet_sdk.runnables.types import EmptyModel

from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from mageflow.swarm.workflows import fill_swarm_running_tasks
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test_handle_finish_tasks_sanity_starts_next_task(
    mock_context, mock_fill_running_tasks, mock_activate_success, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=2,
        is_swarm_closed=False,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    await swarm_task.tasks_left_to_run.aappend(task.key)

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await fill_swarm_running_tasks(locked_swarm, mock_context, msg)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert reloaded_swarm.current_running_tasks == 1

    mock_activate_success.assert_not_awaited()

    mock_fill_running_tasks.assert_called_once_with()


@pytest.mark.asyncio
async def test_handle_finish_tasks_sanity_swarm_completes(
    mock_context, mock_activate_success, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
        is_swarm_closed=True,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    await swarm_task.tasks.aappend(task.key)
    await swarm_task.finished_tasks.aappend(task.key)

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await fill_swarm_running_tasks(locked_swarm, mock_context, msg)

    # Assert
    mock_activate_success.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_handle_finish_tasks_no_tasks_left_edge_case(mock_context, publish_state):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=False,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await fill_swarm_running_tasks(locked_swarm, mock_context, msg)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert reloaded_swarm.current_running_tasks == 0

    mock_context.log.assert_any_call(
        f"Swarm item no new task to run in {swarm_task.key}"
    )


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_decrease_edge_case(
    mock_context, mock_redis_int_increase_error, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(RuntimeError):
        async with swarm_task.lock(save_at_end=False) as locked_swarm:
            await fill_swarm_running_tasks(locked_swarm, mock_context, msg)

    mock_redis_int_increase_error.assert_called_once_with(-1)


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_activate_success_edge_case(
    mock_context, mock_activate_success_error, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
        is_swarm_closed=True,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    await swarm_task.tasks.aappend(task.key)
    await swarm_task.finished_tasks.aappend(task.key)

    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(RuntimeError):
        async with swarm_task.lock(save_at_end=False) as locked_swarm:
            await fill_swarm_running_tasks(locked_swarm, mock_context, msg)

    mock_activate_success_error.assert_awaited_once_with(msg)
