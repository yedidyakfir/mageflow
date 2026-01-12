import asyncio

import mageflow
import pytest
from mageflow.callbacks import HatchetResult
from rapyer.types.base import REDIS_DUMP_FLAG_NAME
from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    get_runs,
    assert_signature_done,
    map_wf_by_id,
    assert_signature_not_called,
    assert_swarm_task_done,
    assert_chain_done,
)
from tests.integration.hatchet.conftest import (
    HatchetInitData,
    convert_signature_mapping_to_list,
)
from tests.integration.hatchet.models import ContextMessage, SavedSignaturesResults
from tests.integration.hatchet.worker import (
    root_with_chain_and_swarm,
    simple_root_task,
    task1_callback,
    error_callback,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_simple_root_task__sanity(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    root_callback = await mageflow.sign(task1_callback)
    error_sign = await mageflow.sign(error_callback)
    root_signature = await mageflow.sign(
        simple_root_task,
        success_callbacks=[root_callback],
        error_callbacks=[error_sign],
    )

    # Act
    await root_signature.aio_run_no_wait(message, options=trigger_options)

    # Assert
    await asyncio.sleep(11)
    runs = await get_runs(hatchet, ctx_metadata)

    assert_signature_done(runs, root_callback)
    assert_signature_not_called(runs, error_sign)

    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_root_task_with_chain_and_swarm__callback_called_after_both_done__sanity(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    message = ContextMessage(base_data=test_ctx)

    root_callback = await mageflow.sign(task1_callback)
    root_signature = await mageflow.sign(
        root_with_chain_and_swarm, success_callbacks=[root_callback]
    )

    # Act
    root_res: HatchetResult = await root_signature.aio_run(
        message, options=trigger_options
    )
    signatures = root_res["root_with_chain_and_swarm"]["hatchet_results"]
    saved_signs = SavedSignaturesResults.model_validate(
        signatures, context={REDIS_DUMP_FLAG_NAME: True}
    )

    # Take swarm and chain created by tasks
    swarms, chains, signatures, batch_items = (
        saved_signs.swarms,
        saved_signs.chains,
        saved_signs.signatures,
        saved_signs.batch_items,
    )
    signatures = convert_signature_mapping_to_list(signatures)
    chains = convert_signature_mapping_to_list(chains)
    swarms = convert_signature_mapping_to_list(swarms)
    batch_items = convert_signature_mapping_to_list(batch_items)

    # Assert
    await asyncio.sleep(30)
    runs = await get_runs(hatchet, ctx_metadata)
    wf_map = map_wf_by_id(runs)

    # check callback was executed
    assert_signature_done(runs, root_callback)

    # Check callback was executed last
    latest_start_time = max([wf.started_at for wf in runs])
    assert wf_map[root_callback.key].started_at >= latest_start_time

    # Check inner tasks were called and done, also check input are good
    assert len(swarms) == 2
    assert len(chains) == 1
    for swarm in swarms:
        if swarm.task_name.startswith("root-swarm"):
            continue
        assert_swarm_task_done(runs, swarm, batch_items, signatures)

    assert_chain_done(runs, chains[0], signatures)

    await assert_redis_is_clean(redis_client)


# Test - check if to many fails in the total swarm
