import asyncio

import mageflow
import pytest
from tests.integration.hatchet.assertions import (
    get_runs,
    assert_signature_done,
    assert_signature_not_called,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import (
    timeout_task,
    error_callback,
    chain_callback,
)


@pytest.mark.asyncio(loop_scope="session")
async def test__chain__task_timeout__chain_call_error_callback(
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
    success_callback_sign = await mageflow.sign(chain_callback)
    error_callback_sign = await mageflow.sign(error_callback)
    chain_signature = await mageflow.chain(
        [sign_task1, timeout_task],
        success=success_callback_sign,
        error=error_callback_sign,
    )

    # Act
    await chain_signature.aio_run_no_wait(ContextMessage(), options=trigger_options)
    await asyncio.sleep(15)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)

    assert_signature_done(runs, error_callback_sign)
    assert_signature_not_called(runs, success_callback_sign)
