from enum import Enum

from pydantic import BaseModel


class SignatureStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    INTERRUPTED = "interrupted"
    CANCELED = "canceled"


class PauseActionTypes(str, Enum):
    SUSPEND = "soft"
    INTERRUPT = "hard"


class TaskStatus(BaseModel):
    status: SignatureStatus = SignatureStatus.PENDING
    last_status: SignatureStatus = SignatureStatus.PENDING
    worker_task_id: str = ""

    def is_canceled(self):
        return self.status in [SignatureStatus.CANCELED]
