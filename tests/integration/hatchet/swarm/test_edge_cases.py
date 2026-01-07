import asyncio

import mageflow
import pytest
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import BatchItemTaskSignature, SwarmConfig
from tests.integration.hatchet.assertions import get_runs, assert_swarm_task_done
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import timeout_task, task1


@pytest.mark.asyncio(loop_scope="session")
async def test__task_is_cancelled__swarm_still_finish(
    hatchet_client_init: HatchetInitData, ctx_metadata, trigger_options
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    swarm_tasks = [timeout_task]
    swarm = await mageflow.swarm(
        tasks=swarm_tasks, config=SwarmConfig(max_concurrency=1)
    )
    swarm_items = await BatchItemTaskSignature.afind()
    tasks = await TaskSignature.afind()

    # Act
    regular_message = ContextMessage()
    await asyncio.sleep(10)
    for i in range(2):
        swarm_item = await swarm.add_task(task1)
        swarm_items.append(swarm_item)
        tasks.append(await TaskSignature.get_safe(swarm_item.original_task_id))
    for item in swarm_items:
        await item.aio_run_no_wait(regular_message, options=trigger_options)
    await swarm.close_swarm()
    await asyncio.sleep(30)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)

    # Check swarm callback was called
    assert_swarm_task_done(runs, swarm, swarm_items, tasks)
