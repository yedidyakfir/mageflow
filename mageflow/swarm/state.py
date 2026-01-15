from pydantic import Field
from rapyer import AtomicRedisModel
from rapyer.types import RedisList

from mageflow.signature.types import TaskIdentifierType


class PublishState(AtomicRedisModel):
    task_ids: RedisList[TaskIdentifierType] = Field(default_factory=list)
