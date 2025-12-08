import asyncio

import mageflow
import pytest
from mageflow.signature.model import TaskSignature
from tests.integration.hatchet.assertions import (
    assert_signature_not_called,
    assert_task_was_paused,
    assert_redis_is_clean,
    assert_chain_done,
    get_runs,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import sleep_task
from tests.integration.hatchet.worker import task2_with_result


@pytest.mark.asyncio(loop_scope="session")
async def test__chain_soft_paused_data_is_saved_in_redis__then_resume_check_finish(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
    sign_task3,
    sign_callback1,
    sign_chain_callback,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    sleep_time = 10
    sleep_task_sign = await mageflow.sign(sleep_task, sleep_time=sleep_time)

    task_res_sign = await mageflow.sign(task2_with_result)
    chain_signature = await mageflow.chain(
        tasks=[sign_task1, sleep_task_sign, task_res_sign, sign_task3],
        success=sign_callback1,
        error=sign_chain_callback,
    )
    message = ContextMessage(base_data=test_ctx)
    all_tasks = await TaskSignature.afind()

    # Act - stage 1
    await chain_signature.aio_run_no_wait(message, options=trigger_options)
    # await asyncio.sleep(1000)
    await asyncio.sleep(sleep_time - 2)
    await chain_signature.pause_task()
    await asyncio.sleep(10)
    # await asyncio.sleep(1000)

    # Assert - stage 1
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_not_called(runs, sign_chain_callback)
    assert_signature_not_called(runs, sign_callback1)
    await assert_task_was_paused(runs, task_res_sign)

    # Act - stage 2
    from hatchet_sdk.runnables.contextvars import ctx_additional_metadata

    add_metadata = ctx_additional_metadata.get() or {}
    add_metadata.update(ctx_metadata)
    ctx_additional_metadata.set(add_metadata)

    await chain_signature.resume()
    await asyncio.sleep(15)

    # Assert - stage 2
    runs = await get_runs(hatchet, ctx_metadata)
    assert_chain_done(runs, chain_signature, all_tasks)
    await assert_redis_is_clean(redis_client)
