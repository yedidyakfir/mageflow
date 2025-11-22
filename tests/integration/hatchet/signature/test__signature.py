import asyncio

import pytest

import orchestrator
from tests.integration.hatchet.assertions import (
    assert_task_done,
    assert_redis_is_clean,
    assert_signature_done,
    get_runs,
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

    callback_signature = await orchestrator.sign(task1_callback)
    main_signature = await orchestrator.sign(
        task2, success_callbacks=[callback_signature]
    )

    # Act
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(5)
    runs = await get_runs(hatchet, ctx_metadata)
    for success_id in main_signature.success_callbacks:
        assert_signature_done(runs, success_id)
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

    error_callback_signature = await orchestrator.sign(error_callback)

    # Act
    await error_callback_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(5)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, error_callback_signature, base_data=test_ctx)
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
