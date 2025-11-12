from typing import Any

from pydantic import BaseModel


class SwarmResultsMessage(BaseModel):
    results: Any
