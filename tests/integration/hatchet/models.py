from typing import Any, Annotated

from pydantic import BaseModel, Field

from orchestrator.models.message import ReturnValue


class ContextMessage(BaseModel):
    base_data: dict = Field(default_factory=dict)


class MesageWithResult(BaseModel):
    results: Any


class ErrorMessage(ContextMessage):
    error: str


class CommandMessageWithResult(ContextMessage):
    task_result: Annotated[Any, ReturnValue()]


class SleepTaskMessage(ContextMessage):
    sleep_time: int = 2
    result: Any = None
