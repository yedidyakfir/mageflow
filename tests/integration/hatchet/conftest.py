import asyncio
import dataclasses
import logging
import os
import subprocess
import time
import uuid
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from threading import Thread
from typing import Generator, Callable, AsyncGenerator

import psutil
import pytest
import pytest_asyncio
import redis
import requests
from hatchet_sdk import Hatchet
from hatchet_sdk.clients.admin import TriggerWorkflowOptions
from redis.asyncio.client import Redis

import orchestrator
from orchestrator.startup import orchestrator_config, init_orchestrator
from orchestrator.task.model import HatchetTaskModel
from tests.integration.hatchet.worker import (
    config_obj,
    settings,
    task1,
    task2,
    task3,
    task1_callback,
    fail_task,
    chain_callback,
)

# If redis key starts with one of these, it shouldn't be removed
STATIC_REDIS_PREFIX_KEYS = [HatchetTaskModel.__name__]
pytest.register_assert_rewrite("tests.assertions")


@dataclasses.dataclass
class HatchetInitData:
    redis_client: Redis
    hatchet: Hatchet


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def hatchet() -> AsyncGenerator[Hatchet, None]:
    yield Hatchet(debug=True, config=config_obj)


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def hatchet_client_init(
    real_redis, hatchet
) -> AsyncGenerator[HatchetInitData, None]:
    worker_data = HatchetInitData(redis_client=real_redis, hatchet=hatchet)

    yield worker_data


@pytest_asyncio.fixture(scope="function", loop_scope="session", autouse=True)
async def init_settings(hatchet_client_init: HatchetInitData):
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    orchestrator_config.redis_client = redis_client
    orchestrator_config.hatchet_client = hatchet
    # Load the subclasses of the task signature
    await init_orchestrator()


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def hatchet_worker_deploy(
    redis_client,
) -> AsyncGenerator[subprocess.Popen[bytes], None]:
    await redis_client.flushall()
    current_file = Path(__file__).absolute()
    test_worker_path = current_file.parent / "worker.py"
    command = ["python", str(test_worker_path)]

    with hatchet_worker(command) as proc:
        await asyncio.sleep(10)
        yield proc
    await redis_client.flushall()


def wait_for_worker_health(healthcheck_port: int) -> bool:
    worker_healthcheck_attempts = 0
    max_healthcheck_attempts = 25
    last_error = None

    while True:
        if worker_healthcheck_attempts > max_healthcheck_attempts:
            raise last_error
            raise Exception(
                f"Worker failed to start within {max_healthcheck_attempts} seconds"
            )

        try:
            requests.get(f"http://localhost:{healthcheck_port}/health", timeout=5)

            return True
        except Exception as e:
            last_error = e
            time.sleep(1)

        worker_healthcheck_attempts += 1


def log_output(pipe: BytesIO, log_func: Callable[[str], None]) -> None:
    for line in iter(pipe.readline, b""):
        print(line.decode().strip())


@contextmanager
def hatchet_worker(
    command: list[str],
    healthcheck_port: int = 8001,
) -> Generator[subprocess.Popen[bytes], None, None]:
    logging.info(f"Starting background worker: {' '.join(command)}")

    os.environ["HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT"] = str(healthcheck_port)
    env = os.environ.copy()
    orchestrator_path = Path(__file__).absolute().parent.parent.parent.parent

    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=orchestrator_path,
    )

    # Check if the process is still running
    if proc.poll() is not None:
        raise Exception(f"Worker failed to start with return code {proc.returncode}")

    Thread(target=log_output, args=(proc.stdout, logging.info), daemon=True).start()
    Thread(target=log_output, args=(proc.stderr, logging.error), daemon=True).start()

    wait_for_worker_health(healthcheck_port=healthcheck_port)

    yield proc

    logging.info("Cleaning up background worker")

    parent = psutil.Process(proc.pid)
    children = parent.children(recursive=True)

    for child in children:
        child.terminate()

    parent.terminate()

    _, alive = psutil.wait_procs([parent] + children, timeout=5)

    for p in alive:
        logging.warning(f"Force killing process {p.pid}")
        p.kill()


async def extract_bad_keys_from_redis(redis_client):
    redis_keys = await redis_client.keys()
    non_persistent_keys = [
        key
        for key in redis_keys
        # Ignore all persistent keys
        if all(
            [
                not key.startswith(approve_prefix.encode())
                for approve_prefix in STATIC_REDIS_PREFIX_KEYS
            ]
        )
    ]
    return non_persistent_keys


@pytest.fixture(scope="function")
def ctx_metadata() -> dict:
    return {"test_run_id": uuid.uuid4().hex}


@pytest.fixture(scope="function")
def trigger_options(ctx_metadata):
    return TriggerWorkflowOptions(additional_metadata=ctx_metadata)


@pytest.fixture(scope="function")
def test_ctx():
    return {"test_data": uuid.uuid4().hex}


@pytest_asyncio.fixture(loop_scope="session")
async def sign_task(req):
    task = req.params
    signature = await orchestrator.sign(task)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_task1():
    signature = await orchestrator.sign(task1)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_task2():
    signature = await orchestrator.sign(task2)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_task3():
    signature = await orchestrator.sign(task3)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_callback1():
    signature = await orchestrator.sign(task1_callback)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_fail_task():
    signature = await orchestrator.sign(fail_task)
    return signature


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def sign_chain_callback():
    signature = await orchestrator.sign(chain_callback)
    return signature
