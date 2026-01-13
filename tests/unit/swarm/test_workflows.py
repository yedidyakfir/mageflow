import asyncio
from unittest.mock import ANY, call
import pytest
from hatchet_sdk.runnables.types import EmptyModel

from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.swarm.consts import (
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
)
from mageflow.swarm.messages import SwarmResultsMessage
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from mageflow.swarm.workflows import (
    swarm_start_tasks,
    swarm_item_done,
    swarm_item_failed,
    handle_finish_tasks,
)
from tests.integration.hatchet.models import ContextMessage


# ============================================================================
# swarm_start_tasks - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_start_tasks_sanity_basic_flow(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(5)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act
    await swarm_start_tasks(msg, ctx)

    # Assert
    assert mock_task_aio_run_no_wait.await_count == 2
    mock_task_aio_run_no_wait.assert_has_awaits([call(msg), call(msg)])

    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert len(reloaded_swarm.tasks_left_to_run) == 3


@pytest.mark.asyncio
async def test_swarm_start_tasks_sanity_all_tasks_start(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=5),
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act
    await swarm_start_tasks(msg, ctx)

    # Assert
    assert mock_task_aio_run_no_wait.await_count == 3
    mock_task_aio_run_no_wait.assert_has_awaits([call(msg), call(msg), call(msg)])

    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert len(reloaded_swarm.tasks_left_to_run) == 0


# ============================================================================
# swarm_start_tasks - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_start_tasks_already_started_edge_case(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act
    await swarm_start_tasks(msg, ctx)

    # Assert
    mock_task_aio_run_no_wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_swarm_start_tasks_max_concurrency_zero_edge_case(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=0),
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act
    await swarm_start_tasks(msg, ctx)

    # Assert
    mock_task_aio_run_no_wait.assert_not_awaited()

    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert len(reloaded_swarm.tasks_left_to_run) == 3


@pytest.mark.asyncio
async def test_swarm_start_tasks_empty_tasks_list_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act & Assert
    await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_missing_swarm_task_id_edge_case(mock_context):
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {"task_data": {}}
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_swarm_not_found_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    ctx = create_mock_context_with_metadata(swarm_task_id="nonexistent_swarm")
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(Exception):
        await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_task_not_found_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    await swarm_task.tasks.aextend(["nonexistent_task_1", "nonexistent_task_2"])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(Exception):
        await swarm_start_tasks(msg, ctx)


# ============================================================================
# swarm_item_done - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_sanity_basic_flow(
    create_mock_context_with_metadata, mock_fill_running_tasks
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.tasks_left_to_run.aextend([tasks[1].key, tasks[2].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[0].key,
    )
    msg = SwarmResultsMessage(results={"status": "success", "value": 42})

    # Act
    await swarm_item_done(msg, ctx)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert tasks[0].key in reloaded_swarm.finished_tasks
    assert len(reloaded_swarm.finished_tasks) == 1

    assert len(reloaded_swarm.tasks_results) == 1
    assert reloaded_swarm.tasks_results[0] == msg.results

    mock_fill_running_tasks.assert_called_once_with()


@pytest.mark.asyncio
async def test_swarm_item_done_sanity_last_item_completes(
    create_mock_context_with_metadata, mock_activate_success
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=True,
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(2)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.finished_tasks.aappend(tasks[0].key)

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[1].key,
    )
    msg = SwarmResultsMessage(results={"status": "complete"})

    # Act
    await swarm_item_done(msg, ctx)

    # Assert
    mock_activate_success.assert_awaited_once_with(msg)


# ============================================================================
# swarm_item_done - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_missing_swarm_task_id_edge_case(mock_context):
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {
        "task_data": {
            TASK_ID_PARAM_NAME: "some_task",
        }
    }
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_item_done(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_item_done_missing_swarm_item_id_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm", model_validators=ContextMessage
    )
    await swarm_task.save()

    ctx = create_mock_context_with_metadata(
        task_id="some_task",
        swarm_task_id=swarm_task.key,
    )
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_item_done(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_item_done_swarm_not_found_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id="nonexistent_swarm",
        swarm_item_id="some_item",
    )
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(Exception):
        await swarm_item_done(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_item_done_exception_during_handle_finish_edge_case(
    create_mock_context_with_metadata, mock_handle_finish_tasks_error
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key, swarm_task_id=swarm_task.key, swarm_item_id=task.key
    )
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(RuntimeError, match="Finish tasks error"):
        await swarm_item_done(msg, ctx)

    mock_handle_finish_tasks_error.assert_called_once_with(ANY, ctx, msg)


# ============================================================================
# swarm_item_failed - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_failed_sanity_continue_after_failure(
    create_mock_context_with_metadata, mock_fill_running_tasks
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.tasks_left_to_run.aextend([tasks[1].key, tasks[2].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[0].key,
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert tasks[0].key in reloaded_swarm.failed_tasks
    assert len(reloaded_swarm.failed_tasks) == 1

    assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED

    mock_fill_running_tasks.assert_called_once_with()


@pytest.mark.asyncio
async def test_swarm_item_failed_sanity_stop_after_threshold(
    create_mock_context_with_metadata, mock_activate_error, mock_swarm_remove
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.failed_tasks.aappend(tasks[0].key)

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[1].key,
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    if reloaded_swarm:
        assert reloaded_swarm.task_status.status == SignatureStatus.CANCELED

    mock_activate_error.assert_awaited_once()
    mock_swarm_remove.assert_awaited_once_with(with_error=False)


# ============================================================================
# swarm_item_failed - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_n_failures_none_edge_case(
    create_mock_context_with_metadata, mock_activate_error, mock_fill_running_tasks_zero
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=None),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.save()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.failed_tasks.aextend([tasks[0].key, tasks[1].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[2].key,
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    mock_activate_error.assert_not_awaited()

    mock_fill_running_tasks_zero.assert_called_once_with()

    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_n_failures_zero_edge_case(
    create_mock_context_with_metadata, mock_activate_error, mock_swarm_remove
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=0),
        current_running_tasks=1,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    await swarm_task.tasks.aextend([task.key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key, swarm_task_id=swarm_task.key, swarm_item_id=task.key
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    mock_activate_error.assert_awaited_once()
    mock_swarm_remove.assert_awaited_once_with(with_error=False)


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_one_failure_edge_case(
    create_mock_context_with_metadata, mock_activate_error
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=1),
        current_running_tasks=1,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    await swarm_task.tasks.aextend([task.key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key, swarm_task_id=swarm_task.key, swarm_item_id=task.key
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    mock_activate_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_swarm_item_failed_below_threshold_edge_case(
    create_mock_context_with_metadata, mock_activate_error, mock_fill_running_tasks_zero
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=3),
        current_running_tasks=1,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key, swarm_task_id=swarm_task.key, swarm_item_id=task.key
    )
    msg = EmptyModel()

    # Act
    await swarm_item_failed(msg, ctx)

    # Assert
    mock_activate_error.assert_not_awaited()
    mock_fill_running_tasks_zero.assert_called_once_with()


@pytest.mark.asyncio
async def test_swarm_item_failed_missing_task_key_edge_case(mock_context):
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {
        "task_data": {
            SWARM_TASK_ID_PARAM_NAME: "some_swarm",
            SWARM_ITEM_TASK_ID_PARAM_NAME: "some_item",
        }
    }
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_item_failed(msg, ctx)


# ============================================================================
# handle_finish_tasks - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_handle_finish_tasks_sanity_starts_next_task(
    mock_context, mock_fill_running_tasks, mock_activate_success
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=2,
        is_swarm_closed=False,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    await swarm_task.tasks_left_to_run.aappend(task.key)

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await handle_finish_tasks(locked_swarm, mock_context, msg)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert reloaded_swarm.current_running_tasks == 1

    mock_activate_success.assert_not_awaited()

    mock_fill_running_tasks.assert_called_once_with()


@pytest.mark.asyncio
async def test_handle_finish_tasks_sanity_swarm_completes(
    mock_context, mock_activate_success
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
        is_swarm_closed=True,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    await swarm_task.tasks.aappend(task.key)
    await swarm_task.finished_tasks.aappend(task.key)

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await handle_finish_tasks(locked_swarm, mock_context, msg)

    # Assert
    mock_activate_success.assert_awaited_once_with(msg)


# ============================================================================
# handle_finish_tasks - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_handle_finish_tasks_no_tasks_left_edge_case(mock_context):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=False,
    )
    await swarm_task.save()

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await handle_finish_tasks(locked_swarm, mock_context, msg)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert reloaded_swarm.current_running_tasks == 0

    mock_context.log.assert_any_call(
        f"Swarm item no new task to run in {swarm_task.key}"
    )


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_decrease_edge_case(
    mock_context, mock_redis_int_increase_error
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
    )
    await swarm_task.save()

    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(RuntimeError):
        async with swarm_task.lock(save_at_end=False) as locked_swarm:
            await handle_finish_tasks(locked_swarm, mock_context, msg)

    mock_redis_int_increase_error.assert_called_once_with(-1)


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_activate_success_edge_case(
    mock_context, mock_activate_success_error
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
        is_swarm_closed=True,
    )
    await swarm_task.save()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.save()

    await swarm_task.tasks.aappend(task.key)
    await swarm_task.finished_tasks.aappend(task.key)

    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(RuntimeError):
        async with swarm_task.lock(save_at_end=False) as locked_swarm:
            await handle_finish_tasks(locked_swarm, mock_context, msg)

    mock_activate_success_error.assert_awaited_once_with(msg)


# ============================================================================
# Concurrency and Race Condition Tests
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_concurrent_completions_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=3),
        current_running_tasks=3,
    )
    await swarm_task.save()

    tasks = []
    item_tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

        item_task = TaskSignature(
            task_name=f"item_task_{i}", model_validators=ContextMessage
        )
        await item_task.save()
        item_tasks.append(item_task)

    await swarm_task.tasks.aextend([t.key for t in tasks])

    contexts = [
        create_mock_context_with_metadata(
            task_id=item_tasks[i].key,
            swarm_task_id=swarm_task.key,
            swarm_item_id=tasks[i].key,
        )
        for i in range(3)
    ]

    msgs = [
        SwarmResultsMessage(results={"task": i, "status": "done"}) for i in range(3)
    ]

    # Act
    await asyncio.gather(
        *[swarm_item_done(msgs[i], contexts[i]) for i in range(3)],
        return_exceptions=True,
    )

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert len(reloaded_swarm.finished_tasks) == 3
    for task in tasks:
        assert task.key in reloaded_swarm.finished_tasks

    assert len(reloaded_swarm.tasks_results) == 3

    assert reloaded_swarm.current_running_tasks == 0


@pytest.mark.asyncio
async def test_swarm_item_failed_concurrent_failures_edge_case(
    create_mock_context_with_metadata,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=3, stop_after_n_failures=5),
        current_running_tasks=3,
    )
    await swarm_task.save()

    tasks = []
    item_tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

        item_task = TaskSignature(
            task_name=f"item_task_{i}", model_validators=ContextMessage
        )
        await item_task.save()
        item_tasks.append(item_task)

    await swarm_task.tasks.aextend([t.key for t in tasks])

    contexts = [
        create_mock_context_with_metadata(
            task_id=item_tasks[i].key,
            swarm_task_id=swarm_task.key,
            swarm_item_id=tasks[i].key,
        )
        for i in range(3)
    ]

    msgs = [EmptyModel() for _ in range(3)]

    # Act
    await asyncio.gather(
        *[swarm_item_failed(msgs[i], contexts[i]) for i in range(3)],
        return_exceptions=True,
    )

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    assert len(reloaded_swarm.failed_tasks) == 3
    for task in tasks:
        assert task.key in reloaded_swarm.failed_tasks

    assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED


# ============================================================================
# Integration-style Tests (Testing Full Workflow)
# ============================================================================


@pytest.mark.asyncio
async def test_full_workflow_start_complete_success():
    pass


@pytest.mark.asyncio
async def test_full_workflow_start_failure_stop():
    pass
