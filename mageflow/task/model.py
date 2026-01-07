from typing import Optional, Self

from hatchet_sdk import NonRetryableException
from pydantic import BaseModel
from rapyer import AtomicRedisModel
from rapyer.errors.base import KeyNotFound
from rapyer.fields import Key


class HatchetTaskModel(AtomicRedisModel):
    mageflow_task_name: Key[str]
    task_name: str
    input_validator: Optional[type[BaseModel]] = None
    retries: Optional[int] = None
    is_root_task: bool = False
    root_task_config: Optional[dict] = None

    @classmethod
    async def safe_get(cls, key: str) -> Self | None:
        try:
            return await cls.get(key)
        except KeyNotFound:
            return None

    def should_retry(self, attempt_num: int, e: Exception) -> bool:
        finish_retry = self.retries is not None and attempt_num < self.retries
        return finish_retry and not isinstance(e, NonRetryableException)
