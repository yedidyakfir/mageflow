import pytest

import mageflow
from mageflow.chain.consts import ON_CHAIN_ERROR, ON_CHAIN_END
from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.model import TaskSignature
from tests.integration.hatchet.models import ContextMessage
from tests.unit.assertions import (
    assert_single_success_callback,
    assert_single_error_callback_is_chain_error,
    assert_success_callback_is_chain_end,
    assert_callback_contains,
)


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
    await original_chain_signature.asave()
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
        await task_signature.asave()
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
    await task1.asave()

    task2 = TaskSignature(
        task_name="second_task",
        kwargs={"arg2": "value2"},
        model_validators=ContextMessage,
    )
    await task2.asave()

    task3 = TaskSignature(
        task_name="third_task",
        kwargs={"arg3": "value3"},
        model_validators=ContextMessage,
    )
    await task3.asave()

    # Act
    chain_signature = await mageflow.chain([task1.key, task2.key, task3.key])

    # Assert
    reloaded_task1 = await TaskSignature.get_safe(task1.key)
    reloaded_task2 = await TaskSignature.get_safe(task2.key)
    reloaded_task3 = await TaskSignature.get_safe(task3.key)

    await assert_single_success_callback(reloaded_task1, task2.key)
    await assert_single_success_callback(reloaded_task2, task3.key)
    await assert_success_callback_is_chain_end(reloaded_task3)


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
    await task1.asave()

    task2 = TaskSignature(
        task_name="second_task",
        kwargs={"arg2": "value2"},
        model_validators=ContextMessage,
    )
    await task2.asave()

    # Act
    chain_signature = await mageflow.chain([task1.key, task2.key])

    # Assert
    reloaded_task1 = await TaskSignature.get_safe(task1.key)
    reloaded_task2 = await TaskSignature.get_safe(task2.key)

    error_task1 = await assert_single_error_callback_is_chain_error(reloaded_task1)
    error_task2 = await assert_single_error_callback_is_chain_error(reloaded_task2)
    assert error_task1.key != error_task2.key


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
    await existing_success.asave()

    existing_error = TaskSignature(
        task_name="existing_error",
        kwargs={},
        model_validators=ContextMessage,
    )
    await existing_error.asave()

    # Create task with existing callbacks
    task_with_callbacks = TaskSignature(
        task_name="task_with_existing_callbacks",
        kwargs={"arg": "value"},
        success_callbacks=[existing_success.key],
        error_callbacks=[existing_error.key],
        model_validators=ContextMessage,
    )
    await task_with_callbacks.asave()

    simple_task = TaskSignature(
        task_name="simple_task",
        kwargs={"param": "param_value"},
        model_validators=ContextMessage,
    )
    await simple_task.asave()

    # Act
    chain_signature = await mageflow.chain([task_with_callbacks.key, simple_task.key])

    # Assert
    reloaded_task = await TaskSignature.get_safe(task_with_callbacks.key)

    assert len(reloaded_task.success_callbacks) == 2
    assert len(reloaded_task.error_callbacks) == 2
    assert_callback_contains(
        reloaded_task,
        [existing_success.key, simple_task.key],
        [existing_error.key],
    )

    new_error_callbacks = [
        cb for cb in reloaded_task.error_callbacks if cb != existing_error.key
    ]
    assert len(new_error_callbacks) == 1
    new_error_task = await TaskSignature.get_safe(new_error_callbacks[0])
    assert new_error_task.task_name == ON_CHAIN_ERROR
