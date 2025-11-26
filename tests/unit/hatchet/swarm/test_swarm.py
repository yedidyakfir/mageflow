import pytest

from orchestrator.signature.model import TaskSignature
from orchestrator.swarm.model import SwarmTaskSignature, SwarmConfig
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_ids", [["task_1"], ["task_1", "task_2"], ["task_1", "task_2", "task_3"]]
)
async def test_add_to_finished_tasks_sanity(task_ids):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        kwargs={},
        model_validators=ContextMessage,
        finished_tasks=[],
    )
    await swarm_signature.save()
    original_finished_tasks = swarm_signature.finished_tasks.copy()

    # Act
    for task_id in task_ids:
        await swarm_signature.add_to_finished_tasks(task_id)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.from_id(swarm_signature.id)
    finished_tasks = original_finished_tasks + task_ids
    assert reloaded_swarm.finished_tasks == finished_tasks
    assert swarm_signature.finished_tasks == finished_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_ids", [["task_1"], ["task_1", "task_2"], ["task_1", "task_2", "task_3"]]
)
async def test_add_to_failed_tasks_sanity(task_ids):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        kwargs={},
        model_validators=ContextMessage,
        failed_tasks=[],
    )
    await swarm_signature.save()
    original_failed_tasks = swarm_signature.failed_tasks.copy()

    # Act
    for task_id in task_ids:
        await swarm_signature.add_to_failed_tasks(task_id)

    # Assert
    reloaded_swarm = await SwarmTaskSignature.from_id(swarm_signature.id)
    failed_tasks = original_failed_tasks + task_ids
    assert reloaded_swarm.failed_tasks == failed_tasks
    assert swarm_signature.failed_tasks == failed_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(["initial_count"], [[5], [10], [3]])
async def test_decrease_running_tasks_count_sanity(initial_count):
    # Arrange
    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        kwargs={},
        model_validators=ContextMessage,
        current_running_tasks=initial_count,
    )
    await swarm_signature.save()

    # Act
    await swarm_signature.decrease_running_tasks_count()

    # Assert
    reloaded_swarm = await SwarmTaskSignature.from_id(swarm_signature.id)
    final_count = initial_count - 1
    assert reloaded_swarm.current_running_tasks == final_count
    assert swarm_signature.current_running_tasks == final_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["max_concurrency", "current_running", "expected_can_run"],
    [[5, 3, True], [5, 4, True], [5, 5, False], [1, 1, False], [10, 0, True]],
)
async def test_add_to_running_tasks_sanity(
    max_concurrency, current_running, expected_can_run
):
    # Arrange
    task_signature = TaskSignature(
        task_name="test_task", model_validators=ContextMessage
    )
    await task_signature.save()

    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=current_running,
        config=SwarmConfig(max_concurrency=max_concurrency),
    )
    await swarm_signature.save()
    original_tasks_left_to_run = swarm_signature.tasks_left_to_run.copy()

    # Act
    can_run = await swarm_signature.add_to_running_tasks(task_signature)

    # Assert
    assert can_run == expected_can_run
    reloaded_swarm = await SwarmTaskSignature.from_id(swarm_signature.id)

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
        new_tasks_left = original_tasks_left_to_run + [task_signature.id]
        assert (
            reloaded_swarm.tasks_left_to_run
            == new_tasks_left
            == reloaded_swarm.tasks_left_to_run
        )
