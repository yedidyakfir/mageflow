import pytest

import mageflow
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import BatchItemTaskSignature, SwarmTaskSignature
from tests.integration.hatchet.models import ContextMessage
from tests.unit.assertions import (
    assert_single_success_callback,
    assert_single_error_callback_is_chain_error,
    assert_task_reloaded_as_type,
    assert_success_callback_is_chain_end,
    assert_all_error_callbacks_are_chain_error,
    assert_callback_contains,
)


@pytest.mark.asyncio
async def test_chain_with_swarm_task_creates_callbacks_correctly_edge_case(
    hatchet_mock,
):
    # Arrange
    # Create a swarm task
    swarm_task = SwarmTaskSignature(
        task_name="test_swarm_task",
        kwargs={"swarm_arg": "swarm_value"},
        model_validators=ContextMessage,
        publishing_state_id="",
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
    reloaded_swarm = await assert_task_reloaded_as_type(swarm_task.key, SwarmTaskSignature)
    await assert_single_success_callback(reloaded_swarm, simple_task.key)
    await assert_single_error_callback_is_chain_error(reloaded_swarm)


@pytest.mark.asyncio
async def test_chain_with_batch_item_task_creates_callbacks_correctly_edge_case(
    hatchet_mock,
):
    # Arrange
    parent_swarm = SwarmTaskSignature(
        task_name="parent_swarm",
        kwargs={},
        model_validators=ContextMessage,
        publishing_state_id="",
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
    reloaded_batch_item = await assert_task_reloaded_as_type(
        batch_item_task.key, BatchItemTaskSignature
    )
    await assert_single_success_callback(reloaded_batch_item, simple_task.key)
    await assert_single_error_callback_is_chain_error(reloaded_batch_item)


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

    swarm_task = SwarmTaskSignature(
        task_name="swarm_task",
        kwargs={"swarm_arg": "swarm_value"},
        model_validators=ContextMessage,
        publishing_state_id="",
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
    loaded_simple = await assert_task_reloaded_as_type(simple_task.key, TaskSignature)
    loaded_swarm = await assert_task_reloaded_as_type(swarm_task.key, SwarmTaskSignature)
    loaded_final = await assert_task_reloaded_as_type(final_task.key, TaskSignature)

    await assert_single_success_callback(loaded_simple, swarm_task.key)
    await assert_single_success_callback(loaded_swarm, final_task.key)
    await assert_success_callback_is_chain_end(loaded_final)

    error_task_ids = [
        loaded_simple.error_callbacks[0],
        loaded_swarm.error_callbacks[0],
        loaded_final.error_callbacks[0],
    ]
    assert len(set(error_task_ids)) == 3
    await assert_all_error_callbacks_are_chain_error(error_task_ids)


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
    loaded_chain = await assert_task_reloaded_as_type(chain_signature.key, TaskSignature)
    assert loaded_chain.task_name == "chain-task:custom_chain_name"
    assert_callback_contains(loaded_chain, custom_success.key, custom_error.key)
