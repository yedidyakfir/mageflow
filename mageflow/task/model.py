from typing import Optional, Annotated, Self

from hatchet_sdk import NonRetryableException
from pydantic import BaseModel
from rapyer import AtomicRedisModel
from rapyer.errors.base import KeyNotFound
from rapyer.fields import Key


class HatchetTaskModel(AtomicRedisModel):
    mageflow_task_name: Annotated[str, Key()]
    task_name: str
    input_validator: Optional[type[BaseModel]] = None
    retries: Optional[int] = None

    @classmethod
    async def safe_get(cls, key: str) -> Self | None:
        try:
            return await cls.aget(key)
        except KeyNotFound:
            return None

    def should_retry(self, attempt_num: int, e: Exception) -> bool:
        finish_retry = self.retries is not None and attempt_num < self.retries
        return finish_retry and not isinstance(e, NonRetryableException)
