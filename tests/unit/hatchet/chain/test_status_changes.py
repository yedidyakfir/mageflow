from unittest.mock import patch, AsyncMock

import pytest

import orchestrator
from orchestrator.signature.model import TaskSignature
from orchestrator.chain.model import ChainTaskSignature
from orchestrator.signature.status import SignatureStatus, TaskStatus
from tests.integration.hatchet.models import ContextMessage
from tests.unit.hatchet.assertions import assert_redis_keys_do_not_contain_sub_task_ids
from tests.unit.hatchet.assertions import (
    assert_tasks_not_exists,
    assert_tasks_changed_status,
)


@pytest.mark.asyncio
async def test_chain_safe_change_status_on_unsaved_signature_does_not_create_redis_entry_sanity():
    # Arrange
    chain_signature = ChainTaskSignature(
        task_name="test_chain_unsaved",
        kwargs={},
        model_validators=ContextMessage,
        tasks=["task_1", "task_2", "task_3"],
    )

    # Act
    result = await ChainTaskSignature.safe_change_status(
        chain_signature.id, SignatureStatus.SUSPENDED
    )

    # Assert
    assert result is False
    reloaded_signature = await TaskSignature.from_id(chain_signature.id)
    assert reloaded_signature is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["task_signatures_to_create", "tasks_to_delete_indices", "new_status"],
    [
        [
            [
                TaskSignature(task_name="task1"),
                TaskSignature(task_name="task2"),
                TaskSignature(task_name="task3"),
            ],
            [],
            SignatureStatus.SUSPENDED,
        ],
        [
            [
                TaskSignature(task_name="task1"),
                TaskSignature(task_name="task2"),
            ],
            [0, 1],
            SignatureStatus.CANCELED,
        ],
        [
            [
                TaskSignature(task_name="task1"),
                TaskSignature(task_name="task2"),
                TaskSignature(task_name="task3"),
            ],
            [0, 2],
            SignatureStatus.ACTIVE,
        ],
    ],
)
async def test_chain_change_status_with_optional_deleted_sub_tasks_edge_case(
    redis_client,
    task_signatures_to_create: list[TaskSignature],
    tasks_to_delete_indices: list[int],
    new_status: SignatureStatus,
):
    # Arrange
    # Save task signatures
    task_signatures = []
    for task_signature in task_signatures_to_create:
        await task_signature.save()
        task_signatures.append(task_signature)

    # Create a chain
    chain_signature = await orchestrator.chain([task.id for task in task_signatures])

    # Delete specified subtasks from Redis (simulate they were removed)
    deleted_task_ids = []
    for idx in tasks_to_delete_indices:
        await task_signatures[idx].remove()
        deleted_task_ids.append(task_signatures[idx].id)

    # Act
    await chain_signature.safe_change_status(chain_signature.id, new_status)

    # Assert
    # Verify chain signature status changed to new status
    reloaded_chain = await TaskSignature.from_id(chain_signature.id)
    assert reloaded_chain.task_status.status == new_status
    assert reloaded_chain.task_status.last_status == SignatureStatus.PENDING

    # Verify deleted sub-tasks are still deleted
    await assert_tasks_not_exists(deleted_task_ids)

    # Verify non-deleted subtasks changed status to new status
    non_deleted_indices = [
        task_signatures[i].id
        for i in range(len(task_signatures))
        if i not in tasks_to_delete_indices
    ]
    await assert_tasks_changed_status(
        non_deleted_indices, new_status, SignatureStatus.PENDING
    )

    # Verify no Redis keys contain the deleted subtask IDs
    await assert_redis_keys_do_not_contain_sub_task_ids(redis_client, deleted_task_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["task_signatures_to_create", "tasks_to_delete_indices"],
    [
        [
            [
                TaskSignature(
                    task_name="task1",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
                TaskSignature(
                    task_name="task2",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
                TaskSignature(
                    task_name="task3",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
            ],
            [],
        ],
        [
            [
                TaskSignature(
                    task_name="task1",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.PENDING,
                    ),
                ),
                TaskSignature(
                    task_name="task2",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.PENDING,
                    ),
                ),
            ],
            [0],
        ],
        [
            [
                TaskSignature(
                    task_name="task1",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
                TaskSignature(
                    task_name="task2",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
                TaskSignature(
                    task_name="task3",
                    task_status=TaskStatus(
                        status=SignatureStatus.SUSPENDED,
                        last_status=SignatureStatus.ACTIVE,
                    ),
                ),
            ],
            [1, 2],
        ],
    ],
)
async def test_chain_resume_with_optional_deleted_sub_tasks_sanity(
    mock_aio_run_no_wait,
    task_signatures_to_create: list[TaskSignature],
    tasks_to_delete_indices: list[int],
):
    # Arrange
    task_signatures = []
    expected_statuses = []
    num_of_aio_run = 0
    for task_signature in task_signatures_to_create:
        await task_signature.save()
        task_signatures.append(task_signature)
        last_status = task_signature.task_status.last_status
        expected_statuses.append(last_status)

    chain_signature = await orchestrator.chain([task.id for task in task_signatures])
    chain_signature.task_status.status = SignatureStatus.SUSPENDED

    deleted_task_ids = []
    for idx in tasks_to_delete_indices:
        await task_signatures[idx].remove()
        deleted_task_ids.append(task_signatures[idx].id)

    # Act
    await chain_signature.resume()

    # Assert
    non_deleted_task_ids = [
        i for i in range(len(task_signatures)) if i not in tasks_to_delete_indices
    ]
    for i in non_deleted_task_ids:
        task = task_signatures[i]
        new_status = expected_statuses[i]
        if new_status == SignatureStatus.ACTIVE:
            new_status = SignatureStatus.PENDING
            num_of_aio_run += 1
        await assert_tasks_changed_status(
            [task.id], new_status, SignatureStatus.SUSPENDED
        )

    await assert_tasks_changed_status(
        [chain_signature.id], SignatureStatus.PENDING, SignatureStatus.SUSPENDED
    )

    await assert_tasks_not_exists(deleted_task_ids)
    assert mock_aio_run_no_wait.call_count == num_of_aio_run


@pytest.mark.asyncio
async def test_chain_suspend_sanity(chain_with_tasks):
    # Arrange
    chain_data = chain_with_tasks

    # Act
    await chain_data.chain_signature.suspend()

    # Assert
    # Verify all tasks changed status to suspend
    await assert_tasks_changed_status(
        [chain_data.chain_signature.id], SignatureStatus.SUSPENDED
    )
    await assert_tasks_changed_status(
        [task.id for task in chain_data.task_signatures], SignatureStatus.SUSPENDED
    )
