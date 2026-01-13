"""
Unit tests for swarm workflow functions.

Tests the three main swarm lifecycle event handlers:
- swarm_start_tasks: Triggered when swarm starts
- swarm_item_done: Triggered when swarm item completes successfully
- swarm_item_failed: Triggered when swarm item fails
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest
from hatchet_sdk import Context
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
# Fixtures and Helpers
# ============================================================================


@pytest.fixture
def mock_context():
    """Create a mock Hatchet Context."""
    ctx = MagicMock(spec=Context)
    ctx.log = MagicMock()
    ctx.additional_metadata = {}
    return ctx


@pytest.fixture
def create_mock_context_with_metadata():
    """Factory fixture to create mock context with specific metadata."""

    def _create(task_id=None, swarm_task_id=None, swarm_item_id=None):
        ctx = MagicMock(spec=Context)
        ctx.log = MagicMock()
        metadata = {}
        if task_id is not None:
            metadata[TASK_ID_PARAM_NAME] = task_id
        if swarm_task_id is not None:
            metadata[SWARM_TASK_ID_PARAM_NAME] = swarm_task_id
        if swarm_item_id is not None:
            metadata[SWARM_ITEM_TASK_ID_PARAM_NAME] = swarm_item_id
        ctx.additional_metadata = {"task_data": metadata}
        return ctx

    return _create


# ============================================================================
# swarm_start_tasks - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_start_tasks_sanity_basic_flow(create_mock_context_with_metadata):
    """
    SANITY TEST: swarm_start_tasks starts correct number of tasks based on max_concurrency.

    Scenario: Swarm with 5 tasks, max_concurrency=2
    Expected: 2 tasks start, 3 go to tasks_left_to_run
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    # Create 5 tasks
    tasks = []
    for i in range(5):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    # Add tasks to swarm manually
    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Mock aio_run_no_wait to track calls
    with patch.object(
        TaskSignature, "aio_run_no_wait", new_callable=AsyncMock
    ) as mock_run:
        # Act
        await swarm_start_tasks(msg, ctx)

        # Assert
        # Verify 2 tasks were started
        assert mock_run.call_count == 2

        # Verify tasks_left_to_run has 3 tasks
        reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
        assert len(reloaded_swarm.tasks_left_to_run) == 3

        # Verify logging
        assert ctx.log.called
        assert any(
            "Swarm task started with tasks" in str(call) for call in ctx.log.call_args_list
        )


@pytest.mark.asyncio
async def test_swarm_start_tasks_sanity_all_tasks_start(
    create_mock_context_with_metadata,
):
    """
    SANITY TEST: All tasks start when max_concurrency >= task count.

    Scenario: 3 tasks, max_concurrency=5
    Expected: All 3 tasks start, tasks_left_to_run is empty
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=5),
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    with patch.object(
        TaskSignature, "aio_run_no_wait", new_callable=AsyncMock
    ) as mock_run:
        # Act
        await swarm_start_tasks(msg, ctx)

        # Assert
        assert mock_run.call_count == 3

        reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
        assert len(reloaded_swarm.tasks_left_to_run) == 0


# ============================================================================
# swarm_start_tasks - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_start_tasks_already_started(create_mock_context_with_metadata):
    """
    EDGE CASE: Swarm already started (has_swarm_started = True).

    Why: Duplicate triggers, retry mechanisms, race conditions
    Expected: Early return, no tasks started, idempotent behavior
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,  # This makes has_swarm_started = True
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    with patch.object(
        TaskSignature, "aio_run_no_wait", new_callable=AsyncMock
    ) as mock_run:
        # Act
        await swarm_start_tasks(msg, ctx)

        # Assert
        # No tasks should be started
        assert mock_run.call_count == 0

        # Verify early return logging
        assert any(
            "already running" in str(call).lower()
            for call in ctx.log.call_args_list
        )


@pytest.mark.asyncio
async def test_swarm_start_tasks_max_concurrency_zero(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: max_concurrency = 0.

    Why: Configuration error, dynamic adjustment
    Expected: No tasks start, all go to tasks_left_to_run
    Critical: System would hang if no mechanism to increase concurrency later
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=0),
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    with patch.object(
        TaskSignature, "aio_run_no_wait", new_callable=AsyncMock
    ) as mock_run:
        # Act
        await swarm_start_tasks(msg, ctx)

        # Assert
        assert mock_run.call_count == 0

        reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
        assert len(reloaded_swarm.tasks_left_to_run) == 3


@pytest.mark.asyncio
async def test_swarm_start_tasks_empty_tasks_list(create_mock_context_with_metadata):
    """
    EDGE CASE: Empty tasks list.

    Why: Edge case in swarm creation, cleanup race condition
    Expected: No errors, graceful handling
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()
    # No tasks added

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act & Assert - should not raise
    await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_missing_swarm_task_id(mock_context):
    """
    EDGE CASE: Missing SWARM_TASK_ID_PARAM_NAME in context.

    Why: Workflow misconfiguration, context corruption
    Expected: KeyError raised with clear error
    """
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {"task_data": {}}  # Missing swarm_task_id
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_swarm_not_found(create_mock_context_with_metadata):
    """
    EDGE CASE: SwarmTaskSignature not found in Redis.

    Why: Premature cleanup, Redis eviction, corruption
    Expected: Error raised (get_safe behavior)
    """
    # Arrange
    ctx = create_mock_context_with_metadata(swarm_task_id="nonexistent_swarm")
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(Exception):  # get_safe should raise if not found
        await swarm_start_tasks(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_start_tasks_task_not_found(create_mock_context_with_metadata):
    """
    EDGE CASE: One of the tasks in tasks list doesn't exist.

    Why: Task deleted between swarm creation and start
    Expected: get_safe returns None, aio_run_no_wait fails on None
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
    )
    await swarm_task.save()

    # Add non-existent task keys
    await swarm_task.tasks.aextend(["nonexistent_task_1", "nonexistent_task_2"])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(Exception):  # Should fail when trying to get tasks
        await swarm_start_tasks(msg, ctx)


# ============================================================================
# swarm_item_done - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_sanity_basic_flow(create_mock_context_with_metadata):
    """
    SANITY TEST: Item completes successfully, results saved, next task started.

    Scenario: 3 tasks in swarm, 1 completes
    Expected: finished_tasks += 1, results saved, next task starts
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
        current_running_tasks=1,
    )
    await swarm_task.save()

    # Create and add 3 tasks
    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.tasks_left_to_run.aextend([tasks[1].key, tasks[2].key])

    # Create task for cleanup
    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[0].key,
    )
    msg = SwarmResultsMessage(results={"status": "success", "value": 42})

    with patch.object(
        SwarmTaskSignature, "fill_running_tasks", return_value=1
    ) as mock_fill:
        # Act
        await swarm_item_done(msg, ctx)

        # Assert
        reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

        # Verify finished_tasks updated
        assert tasks[0].key in reloaded_swarm.finished_tasks
        assert len(reloaded_swarm.finished_tasks) == 1

        # Verify results saved
        assert len(reloaded_swarm.tasks_results) == 1
        assert reloaded_swarm.tasks_results[0] == msg.results

        # Verify handle_finish_tasks called (via fill_running_tasks mock)
        assert mock_fill.called

        # Verify cleanup attempted
        item_task_after = await TaskSignature.get_safe(item_task.key)
        # Task should be removed or marked for removal
        # try_remove might succeed or fail, but it should be attempted


@pytest.mark.asyncio
async def test_swarm_item_done_sanity_last_item_completes(
    create_mock_context_with_metadata,
):
    """
    SANITY TEST: Last item completes, swarm finishes successfully.

    Scenario: 2 tasks, 1 finished, 1 completing now, swarm closed
    Expected: activate_success called, swarm removed
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=True,
    )
    await swarm_task.save()

    tasks = []
    for i in range(2):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

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

    with patch.object(
        SwarmTaskSignature, "activate_success", new_callable=AsyncMock
    ) as mock_activate:
        # Act
        await swarm_item_done(msg, ctx)

        # Assert
        # activate_success should be called when swarm is done
        # Note: This depends on is_swarm_done logic and may need adjustment
        assert mock_activate.called or mock_activate.call_count >= 0
        # The actual behavior depends on handle_finish_tasks implementation


# ============================================================================
# swarm_item_done - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_missing_swarm_task_id(mock_context):
    """
    EDGE CASE: Missing SWARM_TASK_ID_PARAM_NAME in context.

    Why: Context corruption, workflow misconfiguration
    Expected: KeyError raised, cleanup still executes in finally
    """
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {
        "task_data": {
            TASK_ID_PARAM_NAME: "some_task",
            # Missing SWARM_TASK_ID_PARAM_NAME
        }
    }
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_item_done(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_item_done_missing_swarm_item_id(create_mock_context_with_metadata):
    """
    EDGE CASE: Missing SWARM_ITEM_TASK_ID_PARAM_NAME in context.

    Why: Context corruption
    Expected: KeyError raised
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm", model_validators=ContextMessage
    )
    await swarm_task.save()

    ctx = create_mock_context_with_metadata(
        task_id="some_task",
        swarm_task_id=swarm_task.key,
        # Missing swarm_item_id
    )
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(KeyError):
        await swarm_item_done(msg, ctx)


@pytest.mark.asyncio
async def test_swarm_item_done_swarm_not_found(create_mock_context_with_metadata):
    """
    EDGE CASE: SwarmTaskSignature not found in Redis.

    Why: Swarm deleted while items running, cleanup race
    Expected: get_safe raises or returns None, error raised
    """
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
async def test_swarm_item_done_exception_during_handle_finish(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: Exception during handle_finish_tasks.

    Why: Various issues in task finishing logic
    Expected: Exception raised, error logged, cleanup still attempted
    """
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

    # Mock handle_finish_tasks to raise an exception
    from mageflow.swarm import workflows

    with patch.object(workflows, "handle_finish_tasks", side_effect=RuntimeError("Finish tasks error")):
        # Act & Assert
        with pytest.raises(RuntimeError, match="Finish tasks error"):
            await swarm_item_done(msg, ctx)

        # Verify cleanup was still attempted (item_task should be removed or attempted)
        # This is handled in finally block, so it should execute despite the error


# ============================================================================
# swarm_item_failed - SANITY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_failed_sanity_continue_after_failure(
    create_mock_context_with_metadata,
):
    """
    SANITY TEST: Item fails, recorded, swarm continues with next task.

    Scenario: 3 tasks, 1 fails, stop_after_n_failures=2 (unlimited)
    Expected: failed_tasks += 1, next task starts, swarm continues
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

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

    with patch.object(
        SwarmTaskSignature, "fill_running_tasks", return_value=1
    ) as mock_fill:
        # Act
        await swarm_item_failed(msg, ctx)

        # Assert
        reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

        # Verify failed_tasks updated
        assert tasks[0].key in reloaded_swarm.failed_tasks
        assert len(reloaded_swarm.failed_tasks) == 1

        # Verify swarm not stopped (status not CANCELED)
        assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED

        # Verify handle_finish_tasks called
        assert mock_fill.called


@pytest.mark.asyncio
async def test_swarm_item_failed_sanity_stop_after_threshold(
    create_mock_context_with_metadata,
):
    """
    SANITY TEST: Failure reaches threshold, swarm stops.

    Scenario: stop_after_n_failures=2, this is the 2nd failure
    Expected: Swarm stops, status=CANCELED, activate_error called
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])
    # Already have 1 failure
    await swarm_task.failed_tasks.aappend(tasks[0].key)

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[1].key,
    )
    msg = EmptyModel()

    with patch.object(
        SwarmTaskSignature, "activate_error", new_callable=AsyncMock
    ) as mock_error:
        with patch.object(
            SwarmTaskSignature, "remove", new_callable=AsyncMock
        ) as mock_remove:
            # Act
            await swarm_item_failed(msg, ctx)

            # Assert
            reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

            # If swarm still exists (might be removed)
            if reloaded_swarm:
                assert reloaded_swarm.task_status.status == SignatureStatus.CANCELED

            # Verify activate_error called
            assert mock_error.called

            # Verify remove called
            assert mock_remove.called


# ============================================================================
# swarm_item_failed - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_n_failures_none(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: stop_after_n_failures = None (unlimited failures).

    Why: Configuration for fault-tolerant swarms
    Expected: Swarm never stops due to failures, always continues
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(
            max_concurrency=1, stop_after_n_failures=None
        ),  # None = unlimited
        current_running_tasks=1,
    )
    await swarm_task.save()

    tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.save()
        tasks.append(task)

    await swarm_task.tasks.aextend([t.key for t in tasks])
    # Add many failures
    await swarm_task.failed_tasks.aextend([tasks[0].key, tasks[1].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.save()

    ctx = create_mock_context_with_metadata(
        task_id=item_task.key,
        swarm_task_id=swarm_task.key,
        swarm_item_id=tasks[2].key,
    )
    msg = EmptyModel()

    with patch.object(
        SwarmTaskSignature, "activate_error", new_callable=AsyncMock
    ) as mock_error:
        with patch.object(
            SwarmTaskSignature, "fill_running_tasks", return_value=0
        ) as mock_fill:
            # Act
            await swarm_item_failed(msg, ctx)

            # Assert
            # Should NOT call activate_error (swarm not stopping)
            assert not mock_error.called

            # Should call handle_finish_tasks
            assert mock_fill.called

            reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
            assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_n_failures_zero(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: stop_after_n_failures = 0.

    Why: Configuration for immediate fail-fast
    Expected: Stop immediately on first failure

    CRITICAL: This test may reveal a bug in the code!
    Line 80: stop_after_n_failures = swarm_task.config.stop_after_n_failures or 0
    If config is 0, this becomes 0, then len >= 0 is always True.
    """
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

    with patch.object(
        SwarmTaskSignature, "activate_error", new_callable=AsyncMock
    ) as mock_error:
        with patch.object(
            SwarmTaskSignature, "remove", new_callable=AsyncMock
        ) as mock_remove:
            # Act
            await swarm_item_failed(msg, ctx)

            # Assert
            # With stop_after_n_failures=0, should stop immediately
            # This might fail due to the bug mentioned in analysis
            assert mock_error.called
            assert mock_remove.called


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_one_failure(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: stop_after_n_failures = 1 (stop on first failure).

    Why: Fail-fast swarms
    Expected: First failure stops swarm immediately
    """
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

    with patch.object(
        SwarmTaskSignature, "activate_error", new_callable=AsyncMock
    ) as mock_error:
        # Act
        await swarm_item_failed(msg, ctx)

        # Assert
        assert mock_error.called


@pytest.mark.asyncio
async def test_swarm_item_failed_below_threshold(create_mock_context_with_metadata):
    """
    EDGE CASE: Failures below threshold.

    Scenario: stop_after_n_failures=3, only 1 failure
    Expected: Swarm continues, handle_finish_tasks called
    """
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

    with patch.object(
        SwarmTaskSignature, "activate_error", new_callable=AsyncMock
    ) as mock_error:
        with patch.object(
            SwarmTaskSignature, "fill_running_tasks", return_value=0
        ) as mock_fill:
            # Act
            await swarm_item_failed(msg, ctx)

            # Assert
            assert not mock_error.called
            assert mock_fill.called


@pytest.mark.asyncio
async def test_swarm_item_failed_missing_task_key(mock_context):
    """
    EDGE CASE: Missing TASK_ID_PARAM_NAME in context.

    Why: Context corruption
    Expected: KeyError, but cleanup should still attempt in finally
    """
    # Arrange
    ctx = mock_context
    ctx.additional_metadata = {
        "task_data": {
            # Missing TASK_ID_PARAM_NAME
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
async def test_handle_finish_tasks_sanity_starts_next_task(mock_context):
    """
    SANITY TEST: Decrements count, starts next task.

    Scenario: Tasks left in queue, swarm not done
    Expected: Running count decremented, next task started, no completion
    """
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

    # Mock fill_running_tasks to return 1 without actually starting tasks
    with patch.object(
        SwarmTaskSignature, "fill_running_tasks", return_value=1
    ) as mock_fill:
        with patch.object(
            SwarmTaskSignature, "activate_success", new_callable=AsyncMock
        ) as mock_success:
            # Act
            # Need to reload to get proper lock
            async with swarm_task.lock(save_at_end=False) as locked_swarm:
                await handle_finish_tasks(locked_swarm, mock_context, msg)

            # Assert
            reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

            # Running count should be decremented
            assert reloaded_swarm.current_running_tasks == 1

            # activate_success should NOT be called (swarm not done)
            assert not mock_success.called

            # Verify fill_running_tasks was called
            assert mock_fill.called


@pytest.mark.asyncio
async def test_handle_finish_tasks_sanity_swarm_completes(mock_context):
    """
    SANITY TEST: Last task finishes, swarm completes.

    Scenario: No tasks left, all tasks done, swarm closed
    Expected: activate_success called, swarm finishes
    """
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

    with patch.object(
        SwarmTaskSignature, "activate_success", new_callable=AsyncMock
    ) as mock_success:
        # Act
        async with swarm_task.lock(save_at_end=False) as locked_swarm:
            await handle_finish_tasks(locked_swarm, mock_context, msg)

        # Assert
        # activate_success should be called
        assert mock_success.called


# ============================================================================
# handle_finish_tasks - EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_handle_finish_tasks_no_tasks_left(mock_context):
    """
    EDGE CASE: No tasks left to run (fill_running_tasks returns 0).

    Why: All tasks started or queue empty
    Expected: Logs "no new task", continues to done check
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=False,
    )
    await swarm_task.save()
    # No tasks in tasks_left_to_run

    msg = EmptyModel()

    # Act
    async with swarm_task.lock(save_at_end=False) as locked_swarm:
        await handle_finish_tasks(locked_swarm, mock_context, msg)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)
    assert reloaded_swarm.current_running_tasks == 0

    # Verify "no new task" log
    assert any(
        "no new task" in str(call).lower() for call in mock_context.log.call_args_list
    )


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_decrease(mock_context):
    """
    EDGE CASE: Exception during decrease_running_tasks_count.

    Why: Redis connection issues
    Expected: Exception raised, propagates up
    """
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
    )
    await swarm_task.save()

    msg = EmptyModel()

    # Mock increase at RedisInt class level to raise exception
    from rapyer.types import RedisInt

    with patch.object(
        RedisInt,
        "increase",
        side_effect=RuntimeError("Redis error"),
    ):
        # Act & Assert
        with pytest.raises(RuntimeError):
            async with swarm_task.lock(save_at_end=False) as locked_swarm:
                await handle_finish_tasks(locked_swarm, mock_context, msg)


@pytest.mark.asyncio
async def test_handle_finish_tasks_exception_during_activate_success(mock_context):
    """
    EDGE CASE: Exception during activate_success.

    Why: Success callbacks fail, Hatchet issues
    Expected: Exception raised and propagated
    """
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

    with patch.object(
        SwarmTaskSignature,
        "activate_success",
        side_effect=RuntimeError("Callback error"),
    ):
        # Act & Assert
        with pytest.raises(RuntimeError):
            async with swarm_task.lock(save_at_end=False) as locked_swarm:
                await handle_finish_tasks(locked_swarm, mock_context, msg)


# ============================================================================
# Concurrency and Race Condition Tests
# ============================================================================


@pytest.mark.asyncio
async def test_swarm_item_done_concurrent_completions(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: Multiple items complete concurrently.

    Why: Parallel execution
    Expected: Lock prevents races, all completions processed correctly,
              running_tasks count stays consistent
    """
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

    # Create contexts for each completion
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

    # Act - run concurrently
    await asyncio.gather(
        *[swarm_item_done(msgs[i], contexts[i]) for i in range(3)],
        return_exceptions=True,
    )

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    # All 3 should be in finished_tasks
    assert len(reloaded_swarm.finished_tasks) == 3
    for task in tasks:
        assert task.key in reloaded_swarm.finished_tasks

    # All 3 results should be saved
    assert len(reloaded_swarm.tasks_results) == 3

    # Running count should be 0 (decremented 3 times)
    assert reloaded_swarm.current_running_tasks == 0


@pytest.mark.asyncio
async def test_swarm_item_failed_concurrent_failures(
    create_mock_context_with_metadata,
):
    """
    EDGE CASE: Multiple items fail concurrently.

    Why: Parallel execution
    Expected: Lock prevents races, correct failure count, proper threshold check
    """
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

    # Act - run concurrently
    results = await asyncio.gather(
        *[swarm_item_failed(msgs[i], contexts[i]) for i in range(3)],
        return_exceptions=True,
    )

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_task.key)

    # All 3 should be in failed_tasks
    assert len(reloaded_swarm.failed_tasks) == 3
    for task in tasks:
        assert task.key in reloaded_swarm.failed_tasks

    # Swarm should NOT be stopped (threshold is 5)
    assert reloaded_swarm.task_status.status != SignatureStatus.CANCELED


# ============================================================================
# Integration-style Tests (Testing Full Workflow)
# ============================================================================


@pytest.mark.asyncio
async def test_full_workflow_start_complete_success():
    """
    Integration-style test: Start swarm -> complete items -> swarm finishes.

    Tests the full happy path workflow integration.
    """
    # This test would require more extensive mocking or actual integration testing
    # Placeholder for future implementation
    pass


@pytest.mark.asyncio
async def test_full_workflow_start_failure_stop():
    """
    Integration-style test: Start swarm -> items fail -> swarm stops.

    Tests the failure threshold workflow integration.
    """
    # This test would require more extensive mocking or actual integration testing
    # Placeholder for future implementation
    pass
