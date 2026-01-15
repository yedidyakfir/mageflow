import abc
from abc import ABC
from typing import Any

from pydantic import BaseModel

from mageflow.signature.model import TaskSignature


class BaseInvoker(ABC):
    @property
    @abc.abstractmethod
    def task_ctx(self) -> dict:
        pass

    @abc.abstractmethod
    async def start_task(self):
        pass

    @abc.abstractmethod
    async def run_success(self, result: Any) -> bool:
        pass

    @abc.abstractmethod
    async def run_error(self) -> bool:
        pass

    @abc.abstractmethod
    async def remove_task(
        self, with_success: bool = True, with_error: bool = True
    ) -> TaskSignature | None:
        pass

    @abc.abstractmethod
    async def should_run_task(self) -> bool:
        pass

    @abc.abstractmethod
    async def wait_task(
        self, task_name: str, msg: BaseModel, validator: type[BaseModel] = None
    ):
        pass
