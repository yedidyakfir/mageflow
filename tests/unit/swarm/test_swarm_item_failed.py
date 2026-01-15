import asyncio

import pytest
from hatchet_sdk.runnables.types import EmptyModel

from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from mageflow.swarm.consts import (
    SWARM_TASK_ID_PARAM_NAME,
    SWARM_ITEM_TASK_ID_PARAM_NAME,
)
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from mageflow.swarm.workflows import swarm_item_failed
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test_swarm_item_failed_sanity_continue_after_failure(
    create_mock_context_with_metadata, mock_fill_running_tasks, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.asave()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.tasks_left_to_run.aextend([tasks[1].key, tasks[2].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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
    create_mock_context_with_metadata,
    mock_activate_error,
    mock_swarm_remove,
    publish_state,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=2),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.asave()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.failed_tasks.aappend(tasks[0].key)

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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


@pytest.mark.asyncio
async def test_swarm_item_failed_stop_after_n_failures_none_edge_case(
    create_mock_context_with_metadata,
    mock_activate_error,
    mock_fill_running_tasks_zero,
    publish_state,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=None),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(3)
    ]
    for task in tasks:
        await task.asave()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.failed_tasks.aextend([tasks[0].key, tasks[1].key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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
    create_mock_context_with_metadata,
    mock_activate_error,
    mock_swarm_remove,
    publish_state,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=0),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    await swarm_task.tasks.aextend([task.key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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
    create_mock_context_with_metadata, mock_activate_error, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=1),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    await swarm_task.tasks.aextend([task.key])

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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
    create_mock_context_with_metadata,
    mock_activate_error,
    mock_fill_running_tasks_zero,
    publish_state,
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1, stop_after_n_failures=3),
        current_running_tasks=1,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    task = TaskSignature(task_name="test_task", model_validators=ContextMessage)
    await task.asave()

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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


@pytest.mark.asyncio
async def test_swarm_item_failed_concurrent_failures_edge_case(
    create_mock_context_with_metadata, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=3, stop_after_n_failures=5),
        current_running_tasks=3,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    tasks = []
    item_tasks = []
    for i in range(3):
        task = TaskSignature(
            task_name=f"test_task_{i}", model_validators=ContextMessage
        )
        await task.asave()
        tasks.append(task)

        item_task = TaskSignature(
            task_name=f"item_task_{i}", model_validators=ContextMessage
        )
        await item_task.asave()
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
