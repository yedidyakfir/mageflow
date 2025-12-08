from typing import Optional, Annotated, Self

from pydantic import BaseModel
from rapyer import AtomicRedisModel
from rapyer.errors.base import KeyNotFound
from rapyer.fields import Key


class HatchetTaskModel(AtomicRedisModel):
    mageflow_task_name: Annotated[str, Key()]
    task_name: str
    input_validator: Optional[type[BaseModel]] = None

    @classmethod
    async def safe_get(cls, key: str) -> Self | None:
        try:
            return await cls.get(key)
        except KeyNotFound:
            return None
