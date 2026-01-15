from unittest.mock import patch

import pytest
import rapyer

from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig, BatchItemTaskSignature
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_ids", [["task_1"], ["task_1", "task_2"], ["task_1", "task_2", "task_3"]]
)
async def test_add_to_finished_tasks_sanity(task_ids, publish_state):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        kwargs={},
        model_validators=ContextMessage,
        finished_tasks=[],
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()
    original_finished_tasks = swarm_signature.finished_tasks.copy()

    # Act
    for task_id in task_ids:
        await swarm_signature.add_to_finished_tasks(task_id)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_signature.key)
    finished_tasks = original_finished_tasks + task_ids
    assert reloaded_swarm.finished_tasks == finished_tasks
    assert swarm_signature.finished_tasks == finished_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_ids", [["task_1"], ["task_1", "task_2"], ["task_1", "task_2", "task_3"]]
)
async def test_add_to_failed_tasks_sanity(task_ids, publish_state):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        kwargs={},
        model_validators=ContextMessage,
        failed_tasks=[],
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()
    original_failed_tasks = swarm_signature.failed_tasks.copy()

    # Act
    for task_id in task_ids:
        await swarm_signature.add_to_failed_tasks(task_id)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_signature.key)
    failed_tasks = original_failed_tasks + task_ids
    assert reloaded_swarm.failed_tasks == failed_tasks
    assert swarm_signature.failed_tasks == failed_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["max_concurrency", "current_running", "expected_can_run"],
    [[5, 3, True], [5, 4, True], [5, 5, False], [1, 1, False], [10, 0, True]],
)
async def test_add_to_running_tasks_sanity(
    max_concurrency, current_running, expected_can_run, publish_state
):
    # Arrange
    task_signature = TaskSignature(
        task_name="test_task", model_validators=ContextMessage
    )
    await task_signature.asave()

    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=current_running,
        config=SwarmConfig(max_concurrency=max_concurrency),
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()
    original_tasks_left_to_run = swarm_signature.tasks_left_to_run.copy()

    # Act
    can_run = await swarm_signature.add_to_running_tasks(task_signature)

    # Assert
    assert can_run == expected_can_run
    reloaded_swarm = await SwarmTaskSignature.get_safe(swarm_signature.key)

    if expected_can_run:
        assert (
            reloaded_swarm.current_running_tasks
            == current_running + 1
            == swarm_signature.current_running_tasks
        )
        assert (
            reloaded_swarm.tasks_left_to_run
            == original_tasks_left_to_run
            == swarm_signature.tasks_left_to_run
        )
    else:
        assert (
            reloaded_swarm.current_running_tasks
            == current_running
            == swarm_signature.current_running_tasks
        )
        new_tasks_left = original_tasks_left_to_run + [task_signature.key]
        assert (
            reloaded_swarm.tasks_left_to_run
            == new_tasks_left
            == reloaded_swarm.tasks_left_to_run
        )


@pytest.mark.asyncio
async def test_add_task_reaches_max_and_closes_swarm(mock_close_swarm, publish_state):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        config=SwarmConfig(max_task_allowed=2),
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()

    task_signature_1 = TaskSignature(task_name="test_task_1")
    await task_signature_1.asave()

    task_signature_2 = TaskSignature(task_name="test_task_2")
    await task_signature_2.asave()

    # Act
    mock_close_swarm.return_value = swarm_signature
    await swarm_signature.add_task(task_signature_1, close_on_max_task=True)
    await swarm_signature.add_task(task_signature_2, close_on_max_task=True)

    # Assert
    mock_close_swarm.assert_called_once_with()


@pytest.mark.asyncio
async def test_add_task_not_reaching_max(mock_close_swarm, publish_state):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        config=SwarmConfig(max_task_allowed=3),
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()

    task_signature = TaskSignature(task_name="test_task")
    await task_signature.asave()

    # Act
    await swarm_signature.add_task(task_signature, close_on_max_task=True)

    # Assert
    mock_close_swarm.assert_not_called()


@pytest.mark.asyncio
async def test_add_task_reaches_max_but_no_close(mock_close_swarm, publish_state):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        config=SwarmConfig(max_task_allowed=2),
        publishing_state_id=publish_state.key,
    )
    await swarm_signature.asave()

    task_signature_1 = TaskSignature(task_name="test_task_1")
    await task_signature_1.asave()

    task_signature_2 = TaskSignature(task_name="test_task_2")
    await task_signature_2.asave()

    # Act
    await swarm_signature.add_task(task_signature_1, close_on_max_task=False)
    await swarm_signature.add_task(task_signature_2, close_on_max_task=False)

    # Assert
    mock_close_swarm.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["num_tasks_left", "current_running", "max_concurrency", "expected_started"],
    [
        [3, 2, 5, 3],  # Can start all 3 remaining tasks (5 - 2 = 3 available slots)
        [5, 0, 3, 3],  # Can only start 3 tasks (limited by max_concurrency)
    ],
)
async def test_fill_running_tasks_sanity(
    num_tasks_left, current_running, max_concurrency, expected_started, publish_state
):
    # Arrange
    # Create original task signatures using list comprehension
    original_tasks = [
        TaskSignature(task_name=f"original_task_{i}", model_validators=ContextMessage)
        for i in range(num_tasks_left + 2)  # Create extra tasks for the swarm
    ]

    # Create swarm with config
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=current_running,
        config=SwarmConfig(max_concurrency=max_concurrency),
        publishing_state_id=publish_state.key,
    )
    await rapyer.ainsert(swarm_signature, *original_tasks)

    # Add tasks to swarm to create BatchItemTaskSignatures using list comprehension
    batch_tasks = [
        await swarm_signature.add_task(original_task)
        for original_task in original_tasks
    ]

    # Populate tasks_left_to_run with batch task IDs using aextend
    tasks_to_queue = batch_tasks[:num_tasks_left]
    task_keys_to_queue = [task.key for task in tasks_to_queue]
    await swarm_signature.tasks_left_to_run.aextend(task_keys_to_queue)

    # Act
    # Track which instances the method was called on
    called_instances = []

    async def track_calls(self, *args, **kwargs):
        called_instances.append(self)
        return None  # Return what the original method would return

    with patch.object(BatchItemTaskSignature, "aio_run_no_wait", new=track_calls):
        await swarm_signature.fill_running_tasks()

    # Assert
    assert len(called_instances) == expected_started

    # Verify the instances have the correct IDs and no duplicates
    called_task_ids = [instance.key for instance in called_instances]

    # Check all IDs are from our expected tasks (tasks that were queued)
    for task_id in called_task_ids:
        assert (
            task_id in task_keys_to_queue
        ), f"Unexpected task ID: {task_id} not in queued tasks"

    # Check for duplicates
    assert len(called_task_ids) == len(
        set(called_task_ids)
    ), f"Duplicate task IDs found: {called_task_ids}"

    # Verify we called exactly the expected number of unique tasks
    assert len(set(called_task_ids)) == expected_started
