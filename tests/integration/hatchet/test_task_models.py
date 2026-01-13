import asyncio

import pytest
from mageflow.task.model import HatchetTaskModel
from tests.integration.hatchet.conftest import HatchetInitData


@pytest.mark.asyncio(loop_scope="session")
async def test_hatchet_task_model_no_ttl_sanity(hatchet_client_init: HatchetInitData):
    # Arrange
    redis_client = hatchet_client_init.redis_client
    # Wait a bit to ensure worker has registered all tasks
    await asyncio.sleep(2)

    # Act
    task_model_keys = await HatchetTaskModel.afind_keys()

    # Assert
    assert (
        len(task_model_keys) > 0
    ), "No HatchetTaskModel keys found in Redis after worker deployment"

    for key in task_model_keys:
        ttl_result = await redis_client.ttl(key)
        assert (
            ttl_result == -1
        ), f"HatchetTaskModel key {key} has TTL {ttl_result}, expected -1 (no TTL)"
