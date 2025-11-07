from typing import Optional

from pydantic import BaseModel
from rapyer import AtomicRedisModel


class HatchetTaskModel(AtomicRedisModel):
    task_name: str
    input_validator: Optional[type[BaseModel]] = None
