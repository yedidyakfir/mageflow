from datetime import datetime
from typing import Optional, Any

import pytest
from pydantic import BaseModel

import mageflow
from mageflow.signature.model import TaskSignature
from mageflow.signature.types import TaskIdentifierType


class SignParamOptions(BaseModel):
    kwargs: Optional[dict[str, Any]] = None
    creation_time: Optional[datetime] = None
    success_callbacks: Optional[list[TaskIdentifierType]] = None
    error_callbacks: Optional[list[TaskIdentifierType]] = None
    task_identifiers: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict[str, Any]:
        result = self.model_dump(exclude_defaults=True, exclude={"kwargs"})
        if self.kwargs:
            result.update(self.kwargs)
        return result


@pytest.fixture
def hatchet_task(orch):
    @orch.task(name="test_task")
    def test_task(msg):
        return msg

    yield test_task


@pytest.fixture
def hatchet_task_name(orch):
    @orch.task(name="test_task")
    def test_task(msg):
        return msg

    return "test_task"


@pytest.fixture
def task(request):
    return request.getfixturevalue(request.param)


@pytest.mark.parametrize("task", ["hatchet_task", "hatchet_task_name"], indirect=True)
@pytest.mark.parametrize(
    ["sign_options", "expected_signature"],
    [
        [
            SignParamOptions(kwargs={"param1": "value1"}),
            TaskSignature(task_name="test_task", kwargs={"param1": "value1"}),
        ],
        [
            SignParamOptions(),
            TaskSignature(task_name="test_task"),
        ],
        [
            SignParamOptions(creation_time=datetime(2023, 1, 1)),
            TaskSignature(task_name="test_task", creation_time=datetime(2023, 1, 1)),
        ],
        [
            SignParamOptions(task_identifiers={"identifier": "test_id"}),
            TaskSignature(
                task_name="test_task", task_identifiers={"identifier": "test_id"}
            ),
        ],
        [
            SignParamOptions(success_callbacks=["TaskSignature:success_task_1"]),
            TaskSignature(
                task_name="test_task",
                success_callbacks=["TaskSignature:success_task_1"],
            ),
        ],
        [
            SignParamOptions(error_callbacks=["TaskSignature:error_task_1"]),
            TaskSignature(
                task_name="test_task", error_callbacks=["TaskSignature:error_task_1"]
            ),
        ],
        [
            SignParamOptions(
                success_callbacks=[
                    "TaskSignature:success_task_1",
                    "TaskSignature:success_task_2",
                ],
                error_callbacks=["TaskSignature:error_task_1"],
            ),
            TaskSignature(
                task_name="test_task",
                success_callbacks=[
                    "TaskSignature:success_task_1",
                    "TaskSignature:success_task_2",
                ],
                error_callbacks=["TaskSignature:error_task_1"],
            ),
        ],
        [
            SignParamOptions(
                kwargs={"param1": "value1", "param2": 42},
                creation_time=datetime(2023, 6, 15),
                task_identifiers={"id1": "test", "id2": "another"},
                success_callbacks=["TaskSignature:complex_success"],
                error_callbacks=[
                    "TaskSignature:complex_error_1",
                    "TaskSignature:complex_error_2",
                ],
            ),
            TaskSignature(
                task_name="test_task",
                kwargs={"param1": "value1", "param2": 42},
                creation_time=datetime(2023, 6, 15),
                success_callbacks=["TaskSignature:complex_success"],
                error_callbacks=[
                    "TaskSignature:complex_error_1",
                    "TaskSignature:complex_error_2",
                ],
                task_identifiers={"id1": "test", "id2": "another"},
            ),
        ],
    ],
)
@pytest.mark.asyncio
async def test__sign_task__sanity(
    task, sign_options: SignParamOptions, expected_signature: TaskSignature
):
    # Arrange
    expected_signature = expected_signature.model_copy()
    sign_params = sign_options.to_dict()
    if not (isinstance(task, str) or expected_signature.model_validators):
        expected_signature.model_validators = task.input_validator

    # Act
    signature = await mageflow.sign(task, **sign_params)

    # Assert
    signature.creation_time = expected_signature.creation_time
    assert signature.model_dump() == expected_signature.model_dump()
