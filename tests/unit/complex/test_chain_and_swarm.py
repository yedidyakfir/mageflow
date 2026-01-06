import pytest

import mageflow
from mageflow.chain.consts import ON_CHAIN_END, ON_CHAIN_ERROR
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import BatchItemTaskSignature, SwarmTaskSignature
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test_chain_with_swarm_task_creates_callbacks_correctly_edge_case(
    hatchet_mock,
):
    # Arrange
    # Create a swarm task
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm_task",
        kwargs={"swarm_arg": "swarm_value"},
        batch_items=[],
        task_kwargs={},
        model_validators=ContextMessage,
    )
    await swarm_task.save()

    simple_task = TaskSignature(
        task_name="simple_task",
        kwargs={"param": "value"},
        model_validators=ContextMessage,
    )
    await simple_task.save()

    # Act
    chain_signature = await mageflow.chain([swarm_task.key, simple_task.key])

    # Assert
    # Verify swarm task was loaded and updated correctly
    reloaded_swarm = await TaskSignature.get_safe(swarm_task.key)
    assert isinstance(reloaded_swarm, SwarmTaskSignature)

    # Verify callbacks were added correctly
    assert len(reloaded_swarm.success_callbacks) == 1
    assert reloaded_swarm.success_callbacks[0] == simple_task.key

    assert len(reloaded_swarm.error_callbacks) == 1
    error_task = await TaskSignature.get_safe(reloaded_swarm.error_callbacks[0])
    assert error_task.task_name == ON_CHAIN_ERROR


@pytest.mark.asyncio
async def test_chain_with_batch_item_task_creates_callbacks_correctly_edge_case(
    hatchet_mock,
):
    # Arrange
    # Create a parent swarm for the batch item
    parent_swarm = SwarmTaskSignature(
        task_name="parent_swarm",
        kwargs={},
        batch_items=[],
        task_kwargs={},
        model_validators=ContextMessage,
    )
    await parent_swarm.save()

    # Create an original task for a batch item
    original_task = TaskSignature(
        task_name="original_task",
        kwargs={},
        model_validators=ContextMessage,
    )
    await original_task.save()

    # Create batch item task
    batch_item_task = BatchItemTaskSignature(
        task_name="batch_item_task",
        kwargs={"batch_arg": "batch_value"},
        swarm_id=parent_swarm.key,
        original_task_id=original_task.key,
        model_validators=ContextMessage,
    )
    await batch_item_task.save()

    simple_task = TaskSignature(
        task_name="simple_task_after_batch",
        kwargs={},
        model_validators=ContextMessage,
    )
    await simple_task.save()

    # Act
    chain_signature = await mageflow.chain([batch_item_task.key, simple_task.key])

    # Assert
    reloaded_batch_item = await TaskSignature.get_safe(batch_item_task.key)
    assert isinstance(reloaded_batch_item, BatchItemTaskSignature)

    # Verify callbacks were added correctly
    assert len(reloaded_batch_item.success_callbacks) == 1
    assert reloaded_batch_item.success_callbacks[0] == simple_task.key

    assert len(reloaded_batch_item.error_callbacks) == 1
    error_task = await TaskSignature.get_safe(reloaded_batch_item.error_callbacks[0])
    assert error_task.task_name == ON_CHAIN_ERROR


@pytest.mark.asyncio
async def test_chain_with_mixed_task_types_loads_and_chains_correctly_sanity(
    hatchet_mock,
):
    # Arrange
    # Create various task types
    simple_task = TaskSignature(
        task_name="simple_task",
        kwargs={"simple_arg": "simple_value"},
        model_validators=ContextMessage,
    )
    await simple_task.save()

    # Create a swarm task
    swarm_task = SwarmTaskSignature(
        task_name="swarm_task",
        kwargs={"swarm_arg": "swarm_value"},
        batch_items=[],
        task_kwargs={},
        model_validators=ContextMessage,
    )
    await swarm_task.save()

    # Create another simple task
    final_task = TaskSignature(
        task_name="final_task",
        kwargs={"final_arg": "final_value"},
        model_validators=ContextMessage,
    )
    await final_task.save()

    # Act
    chain_signature = await mageflow.chain(
        [simple_task.key, swarm_task.key, final_task.key]
    )

    # Assert
    # Verify all tasks can be loaded from Redis
    loaded_simple = await TaskSignature.get_safe(simple_task.key)
    loaded_swarm = await TaskSignature.get_safe(swarm_task.key)
    loaded_final = await TaskSignature.get_safe(final_task.key)

    assert isinstance(loaded_simple, TaskSignature)
    assert isinstance(loaded_swarm, SwarmTaskSignature)
    assert isinstance(loaded_final, TaskSignature)

    # Verify chain structure
    assert loaded_simple.success_callbacks[0] == swarm_task.key
    assert loaded_swarm.success_callbacks[0] == final_task.key

    # Verify the final task connects to the chain end
    chain_end_task = await TaskSignature.get_safe(loaded_final.success_callbacks[0])
    assert chain_end_task.task_name == ON_CHAIN_END

    # Verify all tasks have unique error callbacks
    error_task_ids = [
        loaded_simple.error_callbacks[0],
        loaded_swarm.error_callbacks[0],
        loaded_final.error_callbacks[0],
    ]
    assert len(set(error_task_ids)) == 3  # All unique

    # Verify all error tasks are chain error tasks
    for error_id in error_task_ids:
        error_task = await TaskSignature.get_safe(error_id)
        assert error_task.task_name == ON_CHAIN_ERROR


@pytest.mark.asyncio
async def test_chain_creation_with_custom_name_and_callbacks_sanity(hatchet_mock):
    # Arrange
    # Create custom success and error callbacks
    custom_success = TaskSignature(
        task_name="custom_success_callback",
        kwargs={},
        model_validators=ContextMessage,
    )
    await custom_success.save()

    custom_error = TaskSignature(
        task_name="custom_error_callback",
        kwargs={},
        model_validators=ContextMessage,
    )
    await custom_error.save()

    # Create tasks for a chain
    task1 = TaskSignature(
        task_name="task1",
        kwargs={},
        model_validators=ContextMessage,
    )
    await task1.save()

    task2 = TaskSignature(
        task_name="task2",
        kwargs={},
        model_validators=ContextMessage,
    )
    await task2.save()

    # Act
    chain_signature = await mageflow.chain(
        [task1.key, task2.key],
        name="custom_chain_name",
        success=custom_success.key,
        error=custom_error.key,
    )

    # Assert
    # Verify chain has custom name and callbacks
    loaded_chain = await TaskSignature.get_safe(chain_signature.key)
    assert loaded_chain.task_name == "chain-task:custom_chain_name"
    assert custom_success.key in loaded_chain.success_callbacks
    assert custom_error.key in loaded_chain.error_callbacks
