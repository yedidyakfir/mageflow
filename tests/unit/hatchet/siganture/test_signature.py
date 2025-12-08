import pytest

from mageflow.signature.model import TaskSignature


@pytest.mark.asyncio
async def test__await_task__stored_in_redis__sanity(hatchet_mock, redis_client):
    # Arrange
    @hatchet_mock.task(name="test_task")
    def test_task(msg):
        return msg

    # Act
    task = await TaskSignature.from_task(test_task)

    # Assert
    assert await redis_client.exists(task.key)


@pytest.mark.asyncio
async def test__signature_create_save_load__input_output_same__sanity(hatchet_mock):
    # Arrange
    @hatchet_mock.task(name="test_task")
    def test_task(msg):
        return msg

    kwargs = {"arg1": "test", "arg2": 123}

    # Act
    original_signature = await TaskSignature.from_task(test_task, **kwargs)
    loaded_signature = await TaskSignature.from_id(original_signature.id)

    # Assert
    assert original_signature == loaded_signature


@pytest.mark.asyncio
async def test__from_signature__create_signature_from_existing__all_data_same_except_pk__sanity(
    hatchet_mock,
):
    # Arrange
    @hatchet_mock.task(name="test_task")
    def test_task(msg):
        return msg

    kwargs = {"arg1": "test", "arg2": 123}
    success_callbacks = ["callback1", "callback2"]
    error_callbacks = ["error_callback1"]

    original_signature = await TaskSignature.from_task(
        test_task,
        success_callbacks=success_callbacks,
        error_callbacks=error_callbacks,
        **kwargs,
    )

    # Act
    new_signature = await original_signature.duplicate()

    # Assert
    original_data = original_signature.model_dump(exclude={"pk"})
    new_data = new_signature.model_dump(exclude={"pk"})
    assert original_data == new_data
    assert new_signature.pk != original_signature.pk
