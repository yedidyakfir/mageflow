import asyncio

import pytest

from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import BatchItemTaskSignature, SwarmConfig
from tests.integration.hatchet.assertions import (
    assert_chain_done,
    assert_redis_is_clean,
    get_runs,
    assert_signature_not_called,
    assert_swarm_task_done,
    assert_signature_done,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import CommandMessageWithResult
from tests.integration.hatchet.worker import (
    task1_callback,
    task_with_data,
    task2_with_result,
    task2,
    task3,
    error_callback,
    retry_once,
    cancel_retry,
)


@pytest.mark.asyncio(loop_scope="session")
async def test__swarm_with_swarms_and_chains__sanity(
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
    chain_tasks: list[ChainTaskSignature] = []
    field_int_val = 13
    for i in range(4):
        error_signature = await hatchet.sign(task2_with_result)
        task_sign = await hatchet.sign(
            task_with_data, field_int=field_int_val, field_list=[]
        )
        chain = await hatchet.chain(
            [task1_callback, retry_once, task_sign], error=error_signature
        )
        chain_tasks.append(chain)
    triggered_error = await hatchet.sign(error_callback)
    not_triggered_success = await hatchet.sign(task1_callback)
    failed_chain = await hatchet.chain(
        [task3, cancel_retry], error=triggered_error, success=not_triggered_success
    )

    base_swarm = await hatchet.swarm(tasks=[task2, task3], is_swarm_closed=True)
    final_swarm_success = await hatchet.sign(task2_with_result)

    main_swarm = await hatchet.swarm(
        tasks=chain_tasks + [failed_chain, base_swarm],
        is_swarm_closed=True,
        success_callbacks=[final_swarm_success],
        config=SwarmConfig(max_concurrency=2),
    )
    tasks = await TaskSignature.afind()
    tasks_map = {task.key: task for task in tasks}
    batch_items = await BatchItemTaskSignature.afind()
    batch_items_map = {batch_item.key: batch_item for batch_item in batch_items}

    task_res_param = {"2": "chain"}
    msg = CommandMessageWithResult(base_data=test_ctx, task_result=task_res_param)

    # Act
    await main_swarm.aio_run_no_wait(msg, options=trigger_options)

    # Assert
    await asyncio.sleep(120)
    runs = await get_runs(hatchet, ctx_metadata)

    # Check good chains were successful
    for chain in chain_tasks:
        # basic chain check
        assert_chain_done(runs, chain, tasks, check_callbacks=False)

        # Check kwargs for an inner task was called
        signed_task = tasks_map[chain.tasks[-1]]
        assert_signature_done(runs, signed_task, field_int=field_int_val)

        # Check the first task is called with msg params
        first_task = tasks_map[chain.tasks[0]]
        assert_signature_done(runs, first_task, **msg.model_dump(mode="json"))

        # Check error was not called
        for error in chain.error_callbacks:
            assert_signature_not_called(runs, error)

    # Check error signature triggered for fail chain
    assert_signature_done(runs, triggered_error)
    assert_signature_not_called(runs, not_triggered_success)

    # Check the inner swarm is done
    base_swarm_batch_items = [batch_items_map[key] for key in base_swarm.tasks]
    assert_swarm_task_done(
        runs, base_swarm, base_swarm_batch_items, tasks, check_callbacks=False
    )
    # Assert swarms were called with params
    first_task = tasks_map[batch_items_map[base_swarm.tasks[0]].original_task_id]
    assert_signature_done(runs, first_task, base_data=test_ctx)
    second_task = tasks_map[batch_items_map[base_swarm.tasks[1]].original_task_id]
    assert_signature_done(runs, second_task, **msg.model_dump(mode="json"))

    # Check final success was called
    assert_signature_done(runs, final_swarm_success)

    # Check that Redis is clean except for persistent keys
    await assert_redis_is_clean(redis_client)
