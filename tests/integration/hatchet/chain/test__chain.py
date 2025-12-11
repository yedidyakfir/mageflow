import asyncio

import mageflow
import pytest
from mageflow.signature.model import TaskSignature
from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    assert_chain_done,
    map_wf_by_id,
    get_runs,
    assert_signature_done,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import (
    task2,
    task3,
    chain_callback,
    fail_task,
    error_callback,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_chain_integration(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
    sign_callback1,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    signature2 = await mageflow.sign(task2, success_callbacks=[sign_callback1])
    chain_success_error_callback = await mageflow.sign(error_callback)
    success_chain_signature = await mageflow.sign(
        chain_callback, error_callbacks=[chain_success_error_callback]
    )

    # Act
    chain_signature = await mageflow.chain(
        [sign_task1, signature2.id, task3],
        success=success_chain_signature,
    )
    chain_tasks = await TaskSignature.afind()

    await chain_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(15)
    runs = await get_runs(hatchet, ctx_metadata)

    assert_chain_done(runs, chain_signature, chain_tasks + [success_chain_signature])

    # Check redis is clean
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_chain_fail(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    chain_success_error_callback = await mageflow.sign(error_callback)
    success_chain_signature = await mageflow.sign(chain_callback)

    # Act
    chain_signature = await mageflow.chain(
        [sign_task1, fail_task, task3],
        success=success_chain_signature,
        error=chain_success_error_callback,
    )

    await chain_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(15)
    runs = await get_runs(hatchet, ctx_metadata)
    runs_task_ids = [wf.task_id for wf in runs]

    # Check task was not called
    assert task3.id not in runs_task_ids

    # Check that callback was called
    assert_signature_done(runs, chain_success_error_callback)

    # Check redis is clean
    await assert_redis_is_clean(redis_client)
