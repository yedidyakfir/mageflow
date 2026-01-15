import asyncio
from unittest.mock import ANY

import pytest

from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.signature.model import TaskSignature
from mageflow.swarm.messages import SwarmResultsMessage
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from mageflow.swarm.workflows import swarm_item_done
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test_swarm_item_done_sanity_basic_flow(
    create_mock_context_with_metadata, mock_fill_running_tasks, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=1),
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
    create_mock_context_with_metadata, mock_activate_success, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=2),
        current_running_tasks=1,
        is_swarm_closed=True,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

    tasks = [
        TaskSignature(task_name=f"test_task_{i}", model_validators=ContextMessage)
        for i in range(2)
    ]
    for task in tasks:
        await task.asave()

    await swarm_task.tasks.aextend([t.key for t in tasks])
    await swarm_task.finished_tasks.aappend(tasks[0].key)

    item_task = TaskSignature(task_name="item_task", model_validators=ContextMessage)
    await item_task.asave()

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
    create_mock_context_with_metadata, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        publishing_state_id=publish_state.key,
    )
    await swarm_task.asave()

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
    await item_task.asave()

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
    create_mock_context_with_metadata, mock_handle_finish_tasks_error, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
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
    msg = SwarmResultsMessage(results={})

    # Act & Assert
    with pytest.raises(RuntimeError, match="Finish tasks error"):
        await swarm_item_done(msg, ctx)

    mock_handle_finish_tasks_error.assert_called_once_with(ANY, ctx, msg)


@pytest.mark.asyncio
async def test_swarm_item_done_concurrent_completions_edge_case(
    create_mock_context_with_metadata, publish_state
):
    # Arrange
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        config=SwarmConfig(max_concurrency=3),
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
