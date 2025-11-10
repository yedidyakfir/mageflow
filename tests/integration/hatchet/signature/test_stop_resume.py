import asyncio

import pytest

import orchestrator
from orchestrator.signature.status import SignatureStatus

from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    assert_task_was_paused,
    get_runs,
    assert_signature_done,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.worker import (
    task1,
    sleep_task,
    callback_with_redis,
    ContextMessage,
)


@pytest.mark.asyncio(loop_scope="session")
async def test__set_result_in_return_value(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    callback_signature = await orchestrator.sign(callback_with_redis)
    second_callback_signature = await orchestrator.sign(callback_with_redis)
    main_signature = await orchestrator.sign(
        sleep_task, success_callbacks=[callback_signature, second_callback_signature]
    )
    message = ContextMessage(context=test_ctx)

    # Act
    await callback_signature.change_status(SignatureStatus.SUSPENDED)
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(5)
    runs = await hatchet.runs.aio_list(additional_metadata=ctx_metadata)
    runs_with_callback = [
        wf for wf in runs.rows if wf.workflow_name == callback_with_redis.name
    ]
    assert len(runs_with_callback) == 2, "Callback workflow was not executed"


@pytest.mark.asyncio(loop_scope="session")
async def test_signature_pause_with_callback_redis_cleanup_sanity(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(context=test_ctx)

    callback_signature = await orchestrator.sign(callback_with_redis)
    main_signature = await orchestrator.sign(
        task1, success_callbacks=[callback_signature]
    )

    # Act
    await callback_signature.pause_task()
    await main_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(10)
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, main_signature, test_ctx)
    await assert_task_was_paused(runs, callback_signature)
    # Remove to check all beside this
    await callback_signature.remove()
    await assert_redis_is_clean(redis_client)
