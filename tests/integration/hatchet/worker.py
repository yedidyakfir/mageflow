import asyncio
import json

import redis
from dynaconf import Dynaconf
from hatchet_sdk import Hatchet, ClientConfig, Context
from hatchet_sdk.config import HealthcheckConfig

import mageflow
from mageflow.signature.consts import TASK_ID_PARAM_NAME
from mageflow.startup import mageflow_config
from tests.integration.hatchet.models import (
    ContextMessage,
    MessageWithData,
    MessageWithResult,
    CommandMessageWithResult,
    SleepTaskMessage,
)

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=["settings.toml", ".secrets.toml"],
)
config_obj = ClientConfig(
    token=settings.hatchet.api_key,
    **settings.hatchet.to_dict(),
    healthcheck=HealthcheckConfig(enabled=True),
)

redis = redis.asyncio.from_url(settings.redis.url, decode_responses=True)
hatchet = Hatchet(debug=True, config=config_obj)
hatchet = mageflow.Mageflow(hatchet, redis_client=redis)

# > Default priority
DEFAULT_PRIORITY = 1
SLEEP_TIME = 0.25

task1_test_reg_name = "task1-test"


@hatchet.task(name=task1_test_reg_name, input_validator=ContextMessage)
def task1(msg):
    return f"msg"


@hatchet.durable_task(name="task1_callback", input_validator=CommandMessageWithResult)
def task1_callback(msg):
    return msg


@hatchet.task(name="error_callback", input_validator=ContextMessage)
def error_callback(msg):
    print(msg)


@hatchet.task(name="task2", input_validator=ContextMessage)
def task2(msg):
    return msg


@hatchet.task(name="task-with-data", input_validator=MessageWithData)
def task_with_data(msg):
    return msg.data


@hatchet.task(name="task2-with-res", input_validator=MessageWithResult)
def task2_with_result(msg):
    return msg.results


@hatchet.task(name="task3", input_validator=ContextMessage)
def task3(msg):
    return 2


@hatchet.task(name="chain_callback", input_validator=ContextMessage)
def chain_callback(msg):
    return msg


@hatchet.task(name="fail_task", input_validator=ContextMessage)
def fail_task(msg):
    raise ValueError("Test exception")


@hatchet.durable_task(name="sleep_task", input_validator=SleepTaskMessage)
async def sleep_task(msg: SleepTaskMessage):
    await asyncio.sleep(msg.sleep_time)
    return msg


@hatchet.task(name="callback_with_redis", input_validator=CommandMessageWithResult)
async def callback_with_redis(msg: CommandMessageWithResult, ctx: Context):
    task_id = ctx.additional_metadata[TASK_ID_PARAM_NAME]

    await mageflow_config.redis_client.set(
        f"activated-task-{task_id}", json.dumps(msg.task_result)
    )
    return msg


@hatchet.task(name="return-multiple-values", input_validator=MessageWithResult)
def return_multiple_values(msg):
    return [msg, msg, msg]


@hatchet.task(execution_timeout=1)
async def timeout_task(msg):
    await asyncio.sleep(10)


workflows = [
    task1,
    task2,
    task_with_data,
    task2_with_result,
    task3,
    chain_callback,
    task1_callback,
    fail_task,
    error_callback,
    sleep_task,
    callback_with_redis,
    return_multiple_values,
    timeout_task,
]


async def lifespan():
    print("HI")
    yield


def main() -> None:
    worker = hatchet.worker("tests", workflows=workflows, lifespan=lifespan)

    worker.start()


if __name__ == "__main__":
    main()
