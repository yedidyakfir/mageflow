import asyncio

import pytest

import mageflow
from tests.integration.hatchet.assertions import get_runs, assert_signature_done
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import timeout_task, error_callback


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
    await timeout_sign.aio_run_no_wait(message)
    await asyncio.sleep(10)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    assert_signature_done(runs, error_sign, **message.model_dump(mode="json"))
