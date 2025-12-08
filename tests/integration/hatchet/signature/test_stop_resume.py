import asyncio

import pytest

import mageflow
from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    assert_task_was_paused,
    get_runs,
    assert_signature_done,
    assert_signature_not_called,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.worker import (
    task1,
    sleep_task,
    callback_with_redis,
    ContextMessage,
    task1_callback,
)


@pytest.mark.asyncio(loop_scope="session")
async def test__paused_signature_dont_trigger_callbacks(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    callback_signature = await mageflow.sign(callback_with_redis)
    second_callback_signature = await mageflow.sign(callback_with_redis)
    error_callback = await mageflow.sign(task1_callback)
    main_signature = await mageflow.sign(
        sleep_task,
        success_callbacks=[callback_signature, second_callback_signature],
        error_callbacks=[error_callback],
    )
    message = ContextMessage(base_data=test_ctx)

    # Act
    await main_signature.suspend()
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(5)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_not_called(runs, callback_signature)
    assert_signature_not_called(runs, second_callback_signature)
    assert_signature_not_called(runs, error_callback)


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_pause_with_callback_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    callback_signature = await mageflow.sign(callback_with_redis)
    main_signature = await mageflow.sign(task1, success_callbacks=[callback_signature])

    # Act
    await callback_signature.pause_task()
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(10)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, main_signature, base_data=test_ctx)
    await assert_task_was_paused(runs, callback_signature)
    # Remove to check all beside this
    await callback_signature.remove()
    await assert_redis_is_clean(redis_client)
