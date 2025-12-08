from typing import Callable

from hatchet_sdk.runnables.workflow import BaseWorkflow

TaskIdentifierType = str
HatchetTaskType = BaseWorkflow | Callable
