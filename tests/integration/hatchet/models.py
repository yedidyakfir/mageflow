from typing import Any, Annotated

from pydantic import BaseModel, Field

from mageflow.models.message import ReturnValue


class ContextMessage(BaseModel):
    base_data: dict = Field(default_factory=dict)


class MessageWithData(ContextMessage):
    data: Annotated[Any, ReturnValue()]
    field_int: int = 1
    field_str: str = "test"
    field_list: list[int]


class MessageWithResult(BaseModel):
    results: Any


class ErrorMessage(ContextMessage):
    error: str


class CommandMessageWithResult(ContextMessage):
    task_result: Annotated[Any, ReturnValue()]


class SleepTaskMessage(ContextMessage):
    sleep_time: int = 2
    result: Any = None
