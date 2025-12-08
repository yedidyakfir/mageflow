import pytest

from mageflow.signature.model import TaskSignature
from mageflow.signature.status import SignatureStatus
from tests.unit.hatchet.assertions import assert_tasks_changed_status


@pytest.mark.asyncio
async def test__safe_change_status__signature_deleted_from_redis__raises_error__edge_case(
    hatchet_mock, redis_client
):
    # Arrange
    @hatchet_mock.task(name="test_task")
    def test_task(msg):
        return msg

    signature = await TaskSignature.from_task(test_task)
    task_id = signature.id

    # Delete signature from Redis directly
    await redis_client.delete(signature.key)

    # Act & Assert
    await TaskSignature.safe_change_status(task_id, SignatureStatus.ACTIVE)
    keys = await redis_client.keys()
    assert len(keys) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["initial_status", "last_status"],
    [
        [SignatureStatus.SUSPENDED, SignatureStatus.ACTIVE],
        [SignatureStatus.SUSPENDED, SignatureStatus.PENDING],
        [SignatureStatus.CANCELED, SignatureStatus.ACTIVE],
        [SignatureStatus.CANCELED, SignatureStatus.PENDING],
    ],
)
async def test_signature_resume_with_various_statuses_sanity(
    mock_aio_run_no_wait,
    hatchet_mock,
    initial_status,
    last_status,
):
    # Arrange
    @hatchet_mock.task(name="test_task")
    def test_task(msg):
        return msg

    signature = await TaskSignature.from_task(test_task)
    signature.task_status.status = initial_status
    signature.task_status.last_status = last_status
    await signature.save()

    # Act
    await signature.resume()

    # Assert
    if last_status == SignatureStatus.ACTIVE:
        mock_aio_run_no_wait.assert_called_once()
        last_status = SignatureStatus.PENDING
    else:
        mock_aio_run_no_wait.assert_not_called()
    await assert_tasks_changed_status([signature.id], last_status, initial_status)
