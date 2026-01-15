from unittest.mock import call

import pytest
from hatchet_sdk.runnables.types import EmptyModel

from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from mageflow.swarm.workflows import swarm_start_tasks
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test_swarm_start_tasks_sanity_basic_flow(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        publishing_state_id=publish_state.key,
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
    create_mock_context_with_metadata, mock_task_aio_run_no_wait, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=5),
        publishing_state_id=publish_state.key,
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


@pytest.mark.asyncio
async def test_swarm_start_tasks_already_started_edge_case(
    create_mock_context_with_metadata, mock_task_aio_run_no_wait, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        current_running_tasks=1,
        config=SwarmConfig(max_concurrency=2),
        publishing_state_id=publish_state.key,
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
    create_mock_context_with_metadata, mock_task_aio_run_no_wait, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=0),
        publishing_state_id=publish_state.key,
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
    create_mock_context_with_metadata, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        publishing_state_id=publish_state.key,
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
    create_mock_context_with_metadata, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        publishing_state_id=publish_state.key,
    )
    await swarm_task.save()

    await swarm_task.tasks.aextend(["nonexistent_task_1", "nonexistent_task_2"])

    ctx = create_mock_context_with_metadata(swarm_task_id=swarm_task.key)
    msg = EmptyModel()

    # Act & Assert
    with pytest.raises(Exception):
        await swarm_start_tasks(msg, ctx)
