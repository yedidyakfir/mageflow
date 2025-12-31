import pytest

import mageflow
from mageflow.chain.consts import ON_CHAIN_ERROR, ON_CHAIN_END
from mageflow.signature.model import TaskSignature
from mageflow.chain.model import ChainTaskSignature
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

    kwargs = {"arg1": "test", "arg2": 123}
    tasks = [task1_signature.key, task2_signature.key]

    # Act
    original_chain_signature = ChainTaskSignature(
        task_name="test_chain_task",
        kwargs=kwargs,
        tasks=tasks,
    )
    await original_chain_signature.save()
    loaded_chain_signature = await TaskSignature.get_safe(original_chain_signature.key)

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
                success_callbacks=["existing_success_task.key"],
                error_callbacks=["existing_error_task.key"],
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
    chain_signature = await mageflow.chain([task.key for task in tasks])

    # Assert
    loaded_chain = await TaskSignature.get_safe(chain_signature.key)
    assert isinstance(loaded_chain, ChainTaskSignature)
    assert loaded_chain.tasks == [task.key for task in tasks]

    for i, task in enumerate(tasks):
        loaded_task = await TaskSignature.get_safe(task.key)
        assert loaded_task.key == task.key
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
        chain_success_task = await TaskSignature.get_safe(chain_success_id)
        next_task_name = tasks[i + 1].task_name if i < len(tasks) - 1 else ON_CHAIN_END
        assert chain_success_task.task_name == next_task_name

        chain_error = loaded_errors - task_errors
        assert len(chain_error) == 1
        chain_error_id = chain_error.pop()
        chain_error_task = await TaskSignature.get_safe(chain_error_id)
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
    chain_signature = await mageflow.chain([task1.key, task2.key, task3.key])

    # Assert
    # Reload tasks from Redis to check updated callbacks
    reloaded_task1 = await TaskSignature.get_safe(task1.key)
    reloaded_task2 = await TaskSignature.get_safe(task2.key)
    reloaded_task3 = await TaskSignature.get_safe(task3.key)

    # Task 1 should have Task 2 as a success callback
    assert len(reloaded_task1.success_callbacks) == 1
    assert reloaded_task1.success_callbacks[0] == task2.key

    # Task 2 should have Task 3 as a success callback
    assert len(reloaded_task2.success_callbacks) == 1
    assert reloaded_task2.success_callbacks[0] == task3.key

    # Task 3 should have a chain end task as success callback
    assert len(reloaded_task3.success_callbacks) == 1
    chain_end_task = await TaskSignature.get_safe(reloaded_task3.success_callbacks[0])
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
    chain_signature = await mageflow.chain([task1.key, task2.key])

    # Assert
    # Reload tasks from Redis to check error callbacks
    reloaded_task1 = await TaskSignature.get_safe(task1.key)
    reloaded_task2 = await TaskSignature.get_safe(task2.key)

    # Both tasks should have error callbacks pointing to chain error tasks
    assert len(reloaded_task1.error_callbacks) == 1
    assert len(reloaded_task2.error_callbacks) == 1

    # Verify error callback tasks exist and are unique
    error_task1 = await TaskSignature.get_safe(reloaded_task1.error_callbacks[0])
    error_task2 = await TaskSignature.get_safe(reloaded_task2.error_callbacks[0])

    assert error_task1.task_name == ON_CHAIN_ERROR
    assert error_task2.task_name == ON_CHAIN_ERROR
    assert error_task1.key != error_task2.key  # Unique error tasks


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
        success_callbacks=[existing_success.key],
        error_callbacks=[existing_error.key],
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
    chain_signature = await mageflow.chain([task_with_callbacks.key, simple_task.key])

    # Assert
    reloaded_task = await TaskSignature.get_safe(task_with_callbacks.key)

    # Should have both existing success callback and new chain success callback
    assert len(reloaded_task.success_callbacks) == 2
    assert existing_success.key in reloaded_task.success_callbacks
    assert simple_task.key in reloaded_task.success_callbacks

    # Should have both existing error callback and new chain error callback
    assert len(reloaded_task.error_callbacks) == 2
    assert existing_error.key in reloaded_task.error_callbacks

    # Verify new error callback is a chain error task
    new_error_callbacks = [
        cb for cb in reloaded_task.error_callbacks if cb != existing_error.key
    ]
    assert len(new_error_callbacks) == 1
    new_error_task = await TaskSignature.get_safe(new_error_callbacks[0])
    assert new_error_task.task_name == ON_CHAIN_ERROR
