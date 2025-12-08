import asyncio
from datetime import datetime

import pytest

import mageflow
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmConfig, BatchItemTaskSignature
from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    assert_paused,
    assert_task_did_not_repeat,
    assert_swarm_task_done,
    get_runs,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import sleep_task, task3, task2, task1


@pytest.mark.asyncio(loop_scope="session")
async def test__swarm_soft_paused_data_is_saved_in_redis__then_resume_check_finish(
    hatchet_client_init: HatchetInitData, test_ctx, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    sleep_time = 4
    swarm_sleep_task_sign = await mageflow.sign(sleep_task, sleep_time=sleep_time)
    sleep_tasks = await swarm_sleep_task_sign.duplicate_many(3)
    swarm_signature = await mageflow.swarm(
        tasks=[task1, swarm_sleep_task_sign, *sleep_tasks, task1, task2, task3],
        config=SwarmConfig(max_concurrency=3),
    )
    await swarm_signature.close_swarm()
    batch_tasks = await BatchItemTaskSignature.afind()
    message = ContextMessage(base_data=test_ctx)
    tasks = await TaskSignature.afind()

    # Act
    await swarm_signature.aio_run_no_wait(message, options=trigger_options)
    # await asyncio.sleep(1000)
    await asyncio.sleep(3)
    await swarm_signature.pause_task()
    pause_time = datetime.now()
    await asyncio.sleep(10)
    # await asyncio.sleep(1000)

    from hatchet_sdk.runnables.contextvars import ctx_additional_metadata

    add_metadata = ctx_additional_metadata.get() or {}
    add_metadata.update(ctx_metadata)
    ctx_additional_metadata.set(add_metadata)

    resume_time = datetime.now()
    await swarm_signature.resume()
    await asyncio.sleep(70)

    # Assert
    # TODO - check that at least one task was stopped....
    runs = await get_runs(hatchet, ctx_metadata)

    tasks_map = {task.id: task for task in tasks}
    signature_tasks = [tasks_map[batch.original_task_id] for batch in batch_tasks]
    await assert_paused(runs, signature_tasks, pause_time, resume_time)
    assert_task_did_not_repeat(runs)

    assert_swarm_task_done(runs, swarm_signature, batch_tasks, tasks)
    await assert_redis_is_clean(redis_client)
