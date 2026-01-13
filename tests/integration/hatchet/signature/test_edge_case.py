import asyncio
from datetime import datetime

import mageflow
import pytest
from tests.integration.hatchet.assertions import (
    get_runs,
    assert_signature_done,
    assert_signature_not_called,
    assert_signature_failed,
    map_wf_by_id,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import (
    timeout_task,
    error_callback,
    retry_once,
    retry_to_failure,
    task1_callback,
    cancel_retry,
)


@pytest.mark.asyncio(loop_scope="session")
async def test__timeout_task__call_error_callback(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    error_sign = await mageflow.sign(error_callback)
    timeout_sign = await mageflow.sign(timeout_task, error_callbacks=[error_sign])
    message = ContextMessage(base_data=test_ctx)

    # Act
    await timeout_sign.aio_run_no_wait(message, options=trigger_options)
    await asyncio.sleep(10)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, error_sign, **message.model_dump(mode="json"))


@pytest.mark.asyncio(loop_scope="session")
async def test__retry_once_with_callbacks__success_callback_called_error_callback_not_edge_case(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    error_callback_sign = await mageflow.sign(error_callback)
    success_callback_sign = await mageflow.sign(task1_callback)
    retry_once_sign = await mageflow.sign(
        retry_once,
        error_callbacks=[error_callback_sign],
        success_callbacks=[success_callback_sign],
    )
    message = ContextMessage(base_data=test_ctx)

    # Act
    await retry_once_sign.aio_run_no_wait(message, options=trigger_options)
    await asyncio.sleep(10)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, retry_once_sign, base_data=test_ctx)
    assert_signature_done(runs, success_callback_sign, task_result="Nice")
    assert_signature_not_called(runs, error_callback_sign)


@pytest.mark.asyncio(loop_scope="session")
async def test__retry_to_failure_with_error_callback__error_callback_called_once_after_retries_edge_case(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    message = ContextMessage(base_data=test_ctx)
    error_callback_sign = await mageflow.sign(error_callback)
    retry_to_failure_sign = await mageflow.sign(
        retry_to_failure, error_callbacks=[error_callback_sign]
    )

    # Act
    await retry_to_failure_sign.aio_run_no_wait(message, options=trigger_options)
    await asyncio.sleep(15)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    failed_summary = assert_signature_failed(runs, retry_to_failure_sign)
    assert failed_summary.retry_count == 3
    assert_signature_done(runs, error_callback_sign, base_data=test_ctx)

    # Verify error callback was called only once after all retries
    finish_retry_time = await redis_client.get(f"finish-{retry_to_failure_sign.key}")
    finish_retry_time = datetime.fromisoformat(finish_retry_time)
    wf_by_task_id = map_wf_by_id(runs, also_not_done=True)
    error_callback_run = wf_by_task_id[error_callback_sign.key]
    callback_start_time = error_callback_run.started_at
    finish_retry_time = finish_retry_time.astimezone(callback_start_time.tzinfo)
    assert (
        callback_start_time > finish_retry_time
    ), "Error callback should be called after retry task starts"


@pytest.mark.asyncio(loop_scope="session")
async def test__retry_but_override_with_exception__check_error_callback_is_called(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    error_callback_sign = await mageflow.sign(error_callback)
    message = ContextMessage(base_data=test_ctx)
    cancel_retry_sign = await mageflow.sign(
        cancel_retry, error_callbacks=[error_callback_sign]
    )

    # Act
    await cancel_retry_sign.aio_run_no_wait(message, options=trigger_options)
    await asyncio.sleep(7)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    failed_summary = assert_signature_failed(runs, cancel_retry_sign)
    assert failed_summary.retry_count == 0
    assert_signature_done(runs, error_callback_sign, base_data=test_ctx)
