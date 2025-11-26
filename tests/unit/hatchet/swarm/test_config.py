import pytest

import orchestrator
from orchestrator.errors import TooManyTasksError
from orchestrator.signature.model import TaskSignature
from orchestrator.swarm.model import SwarmConfig
from tests.integration.hatchet.models import ContextMessage


@pytest.mark.asyncio
@pytest.mark.parametrize(["max_task_allowed"], [[2], [1], [5]])
async def test_add_task_exceeds_max_task_allowed_error(max_task_allowed):
    # Arrange
    swarm_signature = await orchestrator.swarm(
        task_name="test_swarm",
        tasks=[
            TaskSignature(task_name=f"test_task_{i}") for i in range(max_task_allowed)
        ],
        model_validators=ContextMessage,
        config=SwarmConfig(max_task_allowed=max_task_allowed),
    )

    # Act & Assert
    with pytest.raises(TooManyTasksError):
        await swarm_signature.add_task(TaskSignature(task_name="test_task_last"))
