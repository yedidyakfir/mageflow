from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

import mageflow
from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmTaskSignature
from tests.integration.hatchet.worker import ContextMessage


@dataclass
class SwarmTestData:
    task_signatures: list
    swarm_signature: SwarmTaskSignature


@pytest_asyncio.fixture
async def swarm_with_tasks():
    swarm_task_signature_1 = TaskSignature(
        task_name="swarm_task_1", model_validators=ContextMessage
    )
    await swarm_task_signature_1.save()

    swarm_task_signature_2 = TaskSignature(
        task_name="swarm_task_2", model_validators=ContextMessage
    )
    await swarm_task_signature_2.save()

    swarm_task_signature_3 = TaskSignature(
        task_name="swarm_task_3", model_validators=ContextMessage
    )
    await swarm_task_signature_3.save()

    task_signatures = [
        swarm_task_signature_1,
        swarm_task_signature_2,
        swarm_task_signature_3,
    ]

    swarm_signature = SwarmTaskSignature(
        task_name="test_swarm",
        model_validators=ContextMessage,
        tasks=[task.key for task in task_signatures],
    )
    await swarm_signature.save()

    return SwarmTestData(
        task_signatures=task_signatures, swarm_signature=swarm_signature
    )


@pytest.fixture
def mock_close_swarm():
    with patch.object(
        SwarmTaskSignature, "close_swarm", new_callable=AsyncMock
    ) as mock_close:
        yield mock_close
