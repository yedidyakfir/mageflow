import asyncio
import json
from typing import Any, Annotated

from dynaconf import Dynaconf
from pydantic import BaseModel

import orchestrator
from hatchet_sdk import Hatchet, ClientConfig, Context
from hatchet_sdk.config import HealthcheckConfig

from orchestrator.callbacks import AcceptParams
from orchestrator.init import lifespan_initialize
from orchestrator.models.message import ReturnValue
from orchestrator.signature.consts import TASK_ID_PARAM_NAME

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=["settings.toml", ".secrets.toml"],
)
config_obj = ClientConfig(
    token=settings.hatchet.api_key,
    **dict(settings.hatchet),
    # tls_config_strategy="none",
    healthcheck=HealthcheckConfig(enabled=True),
)

hatchet = Hatchet(debug=True, config=config_obj)


class ContextMessage(BaseModel):
    context: dict


class CommandMessageWithResult(ContextMessage):
    task_result: Annotated[Any, ReturnValue()]


class SleepTaskMessage(ContextMessage):
    sleep_time: int = 2
    result: Any = None


# > Default priority
DEFAULT_PRIORITY = 1
SLEEP_TIME = 0.25


@orchestrator.register_task("task1-test")
@hatchet.task(name="task1", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def task1(msg):
    return f"msg"


@orchestrator.register_task("task1-callback")
@hatchet.task(name="task1_callback", input_validator=CommandMessageWithResult)
@orchestrator.handle_task_callback()
def task1_callback(msg):
    return msg


@orchestrator.register_task("error-callback")
@hatchet.task(name="error_callback", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def error_callback(msg):
    print(msg)


@orchestrator.register_task("task2-test")
@hatchet.task(name="task2", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def task2(msg):
    return msg


@orchestrator.register_task("task3-test")
@hatchet.task(name="task3", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def task3(msg):
    return 2


@orchestrator.register_task("chain-callback-test")
@hatchet.task(name="chain_callback", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def chain_callback(msg):
    return msg


@orchestrator.register_task("fail-task")
@hatchet.task(name="fail_task", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def fail_task(msg):
    raise ValueError("Test exception")


@orchestrator.register_task("sleep-task")
@hatchet.task(name="sleep_task", input_validator=SleepTaskMessage)
@orchestrator.handle_task_callback()
async def sleep_task(msg: SleepTaskMessage):
    await asyncio.sleep(msg.sleep_time)
    return msg


@orchestrator.register_task("callback-with-redis")
@hatchet.task(name="callback_with_redis", input_validator=CommandMessageWithResult)
@orchestrator.handle_task_callback(expected_params=AcceptParams.ALL)
async def callback_with_redis(msg: CommandMessageWithResult, ctx: Context):
    task_id = ctx.additional_metadata[TASK_ID_PARAM_NAME]

    await settings.redis_client.set(
        f"activated-task-{task_id}", json.dumps(msg.task_result)
    )
    return msg


dono_tasks = orchestrator.init_orchestrator_hatchet_tasks(hatchet)
workflows = [
    task1,
    task2,
    task3,
    chain_callback,
    task1_callback,
    fail_task,
    error_callback,
    sleep_task,
    callback_with_redis,
] + dono_tasks


def main() -> None:
    asyncio.run(orchestrator.init_from_dynaconf(workflows))

    worker = hatchet.worker(
        "dono-infra-test",
        workflows=workflows,
        lifespan=lifespan_initialize,
    )

    worker.start()


if __name__ == "__main__":
    main()
