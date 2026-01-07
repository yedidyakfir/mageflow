from typing import Any, Annotated

from mageflow.models.message import ReturnValue
from pydantic import BaseModel


class ChainSuccessTaskCommandMessage(BaseModel):
    chain_results: Annotated[Any, ReturnValue()]
