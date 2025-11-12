import pytest

import orchestrator
from orchestrator.chain.workflows import ON_CHAIN_ERROR, ON_CHAIN_END
from orchestrator.signature.model import TaskSignature
from orchestrator.chain.model import ChainTaskSignature
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
async def test__chain_signature_create_save_load__input_output_same__sanity(
    hatchet_mock,
):
    # Arrange
    @hatchet_mock.task(name="test_task_1")
    def test_task_1(msg):
        return msg

    @hatchet_mock.task(name="test_task_2")
    def test_task_2(msg):
        return msg

    # Create individual task signatures
    task1_signature = await TaskSignature.from_task(test_task_1, arg1="value1")
    task2_signature = await TaskSignature.from_task(test_task_2, arg2="value2")

    workflow_params = {"param1": "value1", "param2": "value2"}
    kwargs = {"arg1": "test", "arg2": 123}
    tasks = [task1_signature.id, task2_signature.id]

    # Act
    original_chain_signature = ChainTaskSignature(
        task_name="test_chain_task",
        kwargs=kwargs,
        workflow_params=workflow_params,
        tasks=tasks,
    )
    await original_chain_signature.save()
    loaded_chain_signature = await TaskSignature.from_id(original_chain_signature.id)

    # Assert
    assert original_chain_signature == loaded_chain_signature


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_signatures",
    [
        [
            TaskSignature(
                task_name="simple_task_0",
                kwargs={"arg": "value_0"},
                model_validators=ContextMessage,
            ),
            TaskSignature(
                task_name="another_task_1",
                kwargs={"param": "param_value_1"},
                model_validators=ContextMessage,
            ),
        ],
        [
            TaskSignature(
                task_name="simple_task_0",
                kwargs={"arg": "value_0"},
                model_validators=ContextMessage,
            ),
            TaskSignature(
                task_name="another_task_1",
                kwargs={"param": "param_value_1"},
                model_validators=ContextMessage,
            ),
            TaskSignature(
                task_name="third_task_2",
                kwargs={"data": "data_value_2"},
                model_validators=ContextMessage,
            ),
        ],
        [
            TaskSignature(
                task_name="existing_success_callback",
                kwargs={},
                model_validators=ContextMessage,
            ),
            TaskSignature(
                task_name="existing_error_callback",
                kwargs={},
                model_validators=ContextMessage,
            ),
            TaskSignature(
                task_name="task_with_callbacks_0",
                kwargs={"callback_arg": "callback_value_0"},
                success_callbacks=["existing_success_task.id"],
                error_callbacks=["existing_error_task.id"],
                model_validators=ContextMessage,
            ),
        ],
    ],
)
async def test_chain_creation_with_various_task_types_loads_correctly_from_redis_sanity(
    hatchet_mock, task_signatures
):
    # Arrange
    tasks = []
    for task_signature in task_signatures:
        await task_signature.save()
        tasks.append(task_signature)

    # Act
    chain_signature = await orchestrator.chain([task.id for task in tasks])

    # Assert
    loaded_chain = await TaskSignature.from_id(chain_signature.id)
    assert isinstance(loaded_chain, ChainTaskSignature)
    assert loaded_chain.tasks == [task.id for task in tasks]

    for i, task in enumerate(tasks):
        loaded_task = await TaskSignature.from_id(task.id)
        assert loaded_task.id == task.id
        assert loaded_task.task_name == task.task_name

        task_success = set(task.success_callbacks)
        loaded_success = set(loaded_task.success_callbacks)
        task_errors = set(task.error_callbacks)
        loaded_errors = set(loaded_task.error_callbacks)
        assert task_success < loaded_success
        assert task_errors < loaded_errors

        chain_success = loaded_success - task_success
        assert len(chain_success) == 1
        chain_success_id = chain_success.pop()
        chain_success_task = await TaskSignature.from_id(chain_success_id)
        next_task_name = tasks[i + 1].task_name if i < len(tasks) - 1 else ON_CHAIN_END
        assert chain_success_task.task_name == next_task_name

        chain_error = loaded_errors - task_errors
        assert len(chain_error) == 1
        chain_error_id = chain_error.pop()
        chain_error_task = await TaskSignature.from_id(chain_error_id)
        assert chain_error_task.task_name == ON_CHAIN_ERROR


@pytest.mark.asyncio
async def test_chain_success_callbacks_contain_next_task_ids_sanity(
    hatchet_mock,
):
    # Arrange
    task1 = TaskSignature(
        task_name="first_task",
        kwargs={"arg1": "value1"},
        model_validators=ContextMessage,
    )
    await task1.save()

    task2 = TaskSignature(
        task_name="second_task",
        kwargs={"arg2": "value2"},
        model_validators=ContextMessage,
    )
    await task2.save()

    task3 = TaskSignature(
        task_name="third_task",
        kwargs={"arg3": "value3"},
        model_validators=ContextMessage,
    )
    await task3.save()

    # Act
    chain_signature = await orchestrator.chain([task1.id, task2.id, task3.id])

    # Assert
    # Reload tasks from Redis to check updated callbacks
    reloaded_task1 = await TaskSignature.from_id(task1.id)
    reloaded_task2 = await TaskSignature.from_id(task2.id)
    reloaded_task3 = await TaskSignature.from_id(task3.id)

    # Task 1 should have Task 2 as a success callback
    assert len(reloaded_task1.success_callbacks) == 1
    assert reloaded_task1.success_callbacks[0] == task2.id

    # Task 2 should have Task 3 as a success callback
    assert len(reloaded_task2.success_callbacks) == 1
    assert reloaded_task2.success_callbacks[0] == task3.id

    # Task 3 should have a chain end task as success callback
    assert len(reloaded_task3.success_callbacks) == 1
    chain_end_task = await TaskSignature.from_id(reloaded_task3.success_callbacks[0])
    assert chain_end_task.task_name == ON_CHAIN_END


@pytest.mark.asyncio
async def test_chain_error_callbacks_contain_unique_chain_error_task_ids_sanity(
    hatchet_mock,
):
    # Arrange
    task1 = TaskSignature(
        task_name="first_task",
        kwargs={"arg1": "value1"},
        model_validators=ContextMessage,
    )
    await task1.save()

    task2 = TaskSignature(
        task_name="second_task",
        kwargs={"arg2": "value2"},
        model_validators=ContextMessage,
    )
    await task2.save()

    # Act
    chain_signature = await orchestrator.chain([task1.id, task2.id])

    # Assert
    # Reload tasks from Redis to check error callbacks
    reloaded_task1 = await TaskSignature.from_id(task1.id)
    reloaded_task2 = await TaskSignature.from_id(task2.id)

    # Both tasks should have error callbacks pointing to chain error tasks
    assert len(reloaded_task1.error_callbacks) == 1
    assert len(reloaded_task2.error_callbacks) == 1

    # Verify error callback tasks exist and are unique
    error_task1 = await TaskSignature.from_id(reloaded_task1.error_callbacks[0])
    error_task2 = await TaskSignature.from_id(reloaded_task2.error_callbacks[0])

    assert error_task1.task_name == ON_CHAIN_ERROR
    assert error_task2.task_name == ON_CHAIN_ERROR
    assert error_task1.id != error_task2.id  # Unique error tasks


@pytest.mark.asyncio
async def test_chain_with_existing_callbacks_preserves_and_adds_new_ones_edge_case(
    hatchet_mock,
):
    # Arrange
    # Create existing callback tasks
    existing_success = TaskSignature(
        task_name="existing_success",
        kwargs={},
        model_validators=ContextMessage,
    )
    await existing_success.save()

    existing_error = TaskSignature(
        task_name="existing_error",
        kwargs={},
        model_validators=ContextMessage,
    )
    await existing_error.save()

    # Create task with existing callbacks
    task_with_callbacks = TaskSignature(
        task_name="task_with_existing_callbacks",
        kwargs={"arg": "value"},
        success_callbacks=[existing_success.id],
        error_callbacks=[existing_error.id],
        model_validators=ContextMessage,
    )
    await task_with_callbacks.save()

    simple_task = TaskSignature(
        task_name="simple_task",
        kwargs={"param": "param_value"},
        model_validators=ContextMessage,
    )
    await simple_task.save()

    # Act
    chain_signature = await orchestrator.chain([task_with_callbacks.id, simple_task.id])

    # Assert
    reloaded_task = await TaskSignature.from_id(task_with_callbacks.id)

    # Should have both existing success callback and new chain success callback
    assert len(reloaded_task.success_callbacks) == 2
    assert existing_success.id in reloaded_task.success_callbacks
    assert simple_task.id in reloaded_task.success_callbacks

    # Should have both existing error callback and new chain error callback
    assert len(reloaded_task.error_callbacks) == 2
    assert existing_error.id in reloaded_task.error_callbacks

    # Verify new error callback is a chain error task
    new_error_callbacks = [
        cb for cb in reloaded_task.error_callbacks if cb != existing_error.id
    ]
    assert len(new_error_callbacks) == 1
    new_error_task = await TaskSignature.from_id(new_error_callbacks[0])
    assert new_error_task.task_name == ON_CHAIN_ERROR
