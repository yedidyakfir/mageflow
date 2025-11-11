import asyncio
import json

from dynaconf import Dynaconf
from hatchet_sdk import Hatchet, ClientConfig, Context
from hatchet_sdk.config import HealthcheckConfig

import orchestrator
from orchestrator.init import lifespan_initialize
from orchestrator.signature.consts import TASK_ID_PARAM_NAME
from tests.integration.hatchet.models import (
    ContextMessage,
    CommandMessageWithResult,
    SleepTaskMessage,
)

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
orch = orchestrator.Orchestrator(hatchet)

# > Default priority
DEFAULT_PRIORITY = 1
SLEEP_TIME = 0.25

task1_test_reg_name = "task1-test"


@orch.task(name=task1_test_reg_name, input_validator=ContextMessage)
def task1(msg):
    return f"msg"


@orch.durable_task(name="task1_callback", input_validator=CommandMessageWithResult)
def task1_callback(msg):
    return msg


@orch.task(name="error_callback", input_validator=ContextMessage)
def error_callback(msg):
    print(msg)


@orch.task(name="task2", input_validator=ContextMessage)
def task2(msg):
    return msg


@orchestrator.register_task("task3-test")
@hatchet.task(name="task3", input_validator=ContextMessage)
@orchestrator.handle_task_callback()
def task3(msg):
    return 2


@orch.task(name="chain_callback", input_validator=ContextMessage)
def chain_callback(msg):
    return msg


@orch.task(name="fail_task", input_validator=ContextMessage)
def fail_task(msg):
    raise ValueError("Test exception")


@orchestrator.register_task("sleep-task")
@hatchet.durable_task(name="sleep_task", input_validator=SleepTaskMessage)
@orchestrator.handle_task_callback()
async def sleep_task(msg: SleepTaskMessage):
    await asyncio.sleep(msg.sleep_time)
    return msg


@orch.task(name="callback_with_redis", input_validator=CommandMessageWithResult)
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
    worker = hatchet.worker("tests", workflows=workflows, lifespan=lifespan_initialize)

    worker.start()


if __name__ == "__main__":
    main()
