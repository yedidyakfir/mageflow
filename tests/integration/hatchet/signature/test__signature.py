import asyncio

import pytest

import orchestrator
from tests.integration.hatchet.assertions import (
    assert_task_done,
    assert_redis_is_clean,
    assert_signature_done,
    get_runs,
    assert_signature_not_called,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.worker import (
    task1,
    task1_callback,
    task2,
    error_callback,
    fail_task,
    CommandMessageWithResult,
    ContextMessage,
    task1_test_reg_name,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_creation_and_execution_with_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    # Act
    signature = await orchestrator.sign(task1)
    await signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(3)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, signature, base_data=test_ctx)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_with_success_callbacks_execution_and_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    error_callback_signature = await orchestrator.sign(error_callback)
    callback_signature1 = await orchestrator.sign(
        task1_callback, base_data={"callback_data": 1}
    )
    callback_signature2 = await orchestrator.sign(task1_callback)
    success_callbacks = [callback_signature1, callback_signature2]
    main_signature = await orchestrator.sign(
        task2,
        success_callbacks=success_callbacks,
        error_callbacks=[error_callback_signature],
    )

    # Act
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(7)
    runs = await get_runs(hatchet, ctx_metadata)
    success_tasks = {task.id: task for task in success_callbacks}
    for success_id in main_signature.success_callbacks:
        task = success_tasks[success_id]
        input_values = {task.return_value_field(): message.model_dump(mode="json")}
        input_values.update(task.kwargs)
        assert_signature_done(runs, success_id, **input_values)
    for error_id in main_signature.error_callbacks:
        assert_signature_not_called(runs, error_id)
    assert_signature_done(runs, main_signature, base_data=test_ctx)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_with_error_callbacks_execution_and_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    error_msg = "This is error"
    error_callback_signature1 = await orchestrator.sign(error_callback, error=error_msg)
    error_callback_signature2 = await orchestrator.sign(error_callback)
    error_callbacks = [error_callback_signature1, error_callback_signature2]
    callback_signature = await orchestrator.sign(task1_callback)
    error_sign = await orchestrator.sign(
        fail_task,
        error_callbacks=error_callbacks,
        success_callbacks=[callback_signature],
    )

    # Act
    await error_sign.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(7)
    runs = await get_runs(hatchet, ctx_metadata)
    error_tasks = {task.id: task for task in error_callbacks}
    for success_id in error_sign.error_callbacks:
        task = error_tasks[success_id]
        assert_signature_done(runs, success_id, **task.kwargs)
    for error_id in error_sign.success_callbacks:
        assert_signature_not_called(runs, error_id)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_from_registered_task_name_execution_and_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    # Act
    signature = await orchestrator.sign(
        task1_test_reg_name, model_validators=ContextMessage
    )
    await signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(3)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, signature, base_data=test_ctx)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_task_with_success_callback_execution_and_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    success_callback_signature = await orchestrator.sign(
        task1_callback, base_data=test_ctx
    )
    message = ContextMessage(base_data=test_ctx)
    task = await orchestrator.sign(
        task2, success_callbacks=[success_callback_signature.id], base_data=test_ctx
    )

    # Act
    await task.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(3)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(
        runs, success_callback_signature, task_result=dict(base_data=test_ctx)
    )
    assert_signature_done(runs, task, base_data=test_ctx)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_task_with_failure_callback_execution_and_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    error_callback_signature = await orchestrator.sign(error_callback)
    message = ContextMessage(base_data=test_ctx)
    task = await orchestrator.sign(
        fail_task, error_callbacks=[error_callback_signature], base_data=test_ctx
    )

    # Act
    await task.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(10)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, task, base_data=test_ctx, allow_fails=True)
    assert_signature_done(runs, error_callback_signature, base_data=test_ctx)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test__call_signed_task_with_normal_workflow__check_task_is_done(
    hatchet_client_init: HatchetInitData, ctx_metadata, trigger_options, test_ctx
):
    # Arrange
    hatchet = hatchet_client_init.hatchet
    message = CommandMessageWithResult(task_result=test_ctx)

    # Act
    await task1_callback.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(10)
    runs = await get_runs(hatchet, ctx_metadata)

    assert_task_done(runs, task1_callback, results=message.model_dump())
