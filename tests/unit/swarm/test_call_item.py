from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

import mageflow
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmTaskSignature
from tests.integration.hatchet.models import ContextMessage


@pytest_asyncio.fixture
async def test_message():
    return ContextMessage(base_data={"message_param": "message_value"})


@pytest_asyncio.fixture
async def test_swarm():
    """Create a test swarm with standard kwargs"""
    swarm_kwargs = {"swarm_param": "swarm_value"}
    swarm_signature = await mageflow.swarm(
        task_name="test_swarm",
        kwargs=swarm_kwargs,
        model_validators=ContextMessage,
    )
    return swarm_signature, swarm_kwargs


async def create_swarm_with_kwargs(task_name, kwargs):
    return await mageflow.swarm(
        task_name=task_name,
        kwargs=kwargs,
        model_validators=ContextMessage,
    )


async def run_batch_item_with_mock(batch_item, message):
    """Run batch item with mocked add_to_running_tasks returning False"""
    with patch.object(
        SwarmTaskSignature, "add_to_running_tasks", new_callable=AsyncMock
    ) as mock_add_to_running:
        mock_add_to_running.return_value = False
        await batch_item.aio_run_no_wait(message)


def assert_kwargs_merged(actual_kwargs, expected_parts, message):
    """Assert that kwargs are properly merged from expected parts and message"""
    message_data = message.model_dump(mode="json")
    expected_kwargs = {}
    for part in expected_parts:
        expected_kwargs.update(part)
    expected_kwargs.update(message_data)
    assert actual_kwargs == expected_kwargs


@pytest.mark.asyncio
async def test_simple_task_kwargs_saved_sanity(test_message, test_swarm):
    # Arrange
    # Create original task
    task_kwargs = {"task_param": "task_value"}
    original_task = TaskSignature(
        task_name="simple_task",
        kwargs=task_kwargs,
        model_validators=ContextMessage,
    )
    await original_task.asave()

    swarm_signature, swarm_kwargs = test_swarm
    batch_item = await swarm_signature.add_task(original_task)

    # Act
    await run_batch_item_with_mock(batch_item, test_message)

    # Assert
    reloaded_original_task = await TaskSignature.get_safe(original_task.key)
    assert_kwargs_merged(
        reloaded_original_task.kwargs,
        [task_kwargs, batch_item.kwargs, swarm_kwargs],
        test_message,
    )


@pytest.mark.asyncio
async def test_swarm_item_kwargs_saved_in_swarm_object_sanity(test_message):
    # Arrange
    inner_swarm_kwargs = {"inner_swarm_param": "inner_swarm_value"}
    inner_swarm = await create_swarm_with_kwargs("inner_swarm", inner_swarm_kwargs)

    outer_swarm_kwargs = {"outer_swarm_param": "outer_swarm_value"}
    outer_swarm = await create_swarm_with_kwargs("outer_swarm", outer_swarm_kwargs)

    batch_item = await outer_swarm.add_task(inner_swarm)

    # Act
    await run_batch_item_with_mock(batch_item, test_message)

    # Assert - kwargs should be saved in the swarm object
    reloaded_inner_swarm = await SwarmTaskSignature.get_safe(inner_swarm.key)
    assert_kwargs_merged(
        reloaded_inner_swarm.kwargs,
        [inner_swarm_kwargs, batch_item.kwargs, outer_swarm_kwargs],
        test_message,
    )


@pytest.mark.asyncio
async def test_nested_chain_kwargs_saved_in_first_task_of_first_chain_sanity(
    test_message, test_swarm
):
    # Arrange
    # Create first chain
    first_chain_task1_kwargs = {"first_chain_task1_param": "first_chain_task1_value"}
    first_chain_task1 = TaskSignature(
        task_name="first_chain_task1",
        kwargs=first_chain_task1_kwargs,
        model_validators=ContextMessage,
    )
    await first_chain_task1.asave()

    first_chain_task2_kwargs = {"first_chain_task2_param": "first_chain_task2_value"}
    first_chain_task2 = TaskSignature(
        task_name="first_chain_task2",
        kwargs=first_chain_task2_kwargs,
        model_validators=ContextMessage,
    )
    await first_chain_task2.asave()

    first_chain = await mageflow.chain([first_chain_task1.key, first_chain_task2.key])

    # Create second chain
    second_chain_task1_kwargs = {"second_chain_task1_param": "second_chain_task1_value"}
    second_chain_task1 = TaskSignature(
        task_name="second_chain_task1",
        kwargs=second_chain_task1_kwargs,
        model_validators=ContextMessage,
    )
    await second_chain_task1.asave()

    second_chain_task2_kwargs = {"second_chain_task2_param": "second_chain_task2_value"}
    second_chain_task2 = TaskSignature(
        task_name="second_chain_task2",
        kwargs=second_chain_task2_kwargs,
        model_validators=ContextMessage,
    )
    await second_chain_task2.asave()

    second_chain = await mageflow.chain(
        [second_chain_task1.key, second_chain_task2.key]
    )

    # Create chain containing the two chains
    nested_chain = await mageflow.chain([first_chain.key, second_chain.key])

    swarm_signature, swarm_kwargs = test_swarm
    batch_item = await swarm_signature.add_task(nested_chain)

    # Act
    await run_batch_item_with_mock(batch_item, test_message)

    # Assert - kwargs should be saved in the first task of the first chain
    reloaded_first_chain_task1 = await TaskSignature.get_safe(first_chain_task1.key)
    assert_kwargs_merged(
        reloaded_first_chain_task1.kwargs,
        [first_chain_task1_kwargs, batch_item.kwargs, swarm_kwargs],
        test_message,
    )

    # Verify other tasks don't get the message kwargs
    reloaded_first_chain_task2 = await TaskSignature.get_safe(first_chain_task2.key)
    assert reloaded_first_chain_task2.kwargs == first_chain_task2_kwargs

    reloaded_second_chain_task1 = await TaskSignature.get_safe(second_chain_task1.key)
    assert reloaded_second_chain_task1.kwargs == second_chain_task1_kwargs

    reloaded_second_chain_task2 = await TaskSignature.get_safe(second_chain_task2.key)
    assert reloaded_second_chain_task2.kwargs == second_chain_task2_kwargs
