from typing import Any, Annotated

from mageflow.chain.model import ChainTaskSignature
from mageflow.models.message import ReturnValue
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import BatchItemTaskSignature, SwarmTaskSignature
from pydantic import BaseModel, Field


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


class SavedSignaturesResults(BaseModel):
    signatures: dict[str, TaskSignature]
    batch_items: dict[str, BatchItemTaskSignature]
    swarms: dict[str, SwarmTaskSignature]
    chains: dict[str, ChainTaskSignature]
