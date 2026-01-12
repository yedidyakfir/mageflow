from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import fakeredis
import mageflow
import pytest
import pytest_asyncio
import rapyer
from hatchet_sdk import Hatchet, ClientConfig
from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.model import TaskSignature
from mageflow.startup import update_register_signature_models, mageflow_config
from tests.integration.hatchet.models import ContextMessage

pytest.register_assert_rewrite("tests.assertions")


@pytest_asyncio.fixture(autouse=True, scope="function")
async def redis_client():
    await update_register_signature_models()
    client = fakeredis.aioredis.FakeRedis()
    mageflow_config.redis_client = redis_client
    await client.flushall()
    try:
        yield client
    finally:
        await client.flushall()
        await client.aclose()


@pytest.fixture(autouse=True, scope="function")
def hatchet_mock():
    config_obj = ClientConfig(
        token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJodHRwczovL2xvY2FsaG9zdCIsImV4cCI6NDkwNTQ3NzYyNiwiZ3JwY19icm9hZGNhc3RfYWRkcmVzcyI6Imh0dHBzOi8vbG9jYWxob3N0IiwiaWF0IjoxNzUxODc3NjI2LCJpc3MiOiJodHRwczovL2xvY2FsaG9zdCIsInNlcnZlcl91cmwiOiJodHRwczovL2xvY2FsaG9zdCIsInN1YiI6IjdlY2U4ZTk4LWNiMjMtNDg3Ny1hZGNlLWFmYTBiNDMxYTgyMyIsInRva2VuX2lkIjoiNjk0MjBkOGMtMTQ4NS00NGRlLWFmY2YtMDlkYzM5NmJiYzI0In0.l2yHtg1ZGJSkge6MnLXj_zGyg1w_6LZ7ZuyyNrWORnc",
        tls_strategy="tls",
    )
    hatchet = Hatchet(config=config_obj)
    mageflow_config.hatchet_client = hatchet

    yield hatchet


@pytest.fixture()
def orch(hatchet_mock, redis_client):
    yield mageflow.Mageflow(hatchet_mock, redis_client)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def init_models(redis_client):
    await rapyer.init_rapyer(redis_client)


@pytest.fixture
def mock_aio_run_no_wait():
    with patch(
        f"{TaskSignature.__module__}.{TaskSignature.__name__}.aio_run_no_wait",
        new_callable=AsyncMock,
    ) as mock_aio_run:
        yield mock_aio_run


@dataclass
class ChainTestData:
    task_signatures: list
    chain_signature: ChainTaskSignature


@pytest_asyncio.fixture
async def chain_with_tasks():
    chain_task_signature_1 = TaskSignature(
        task_name="chain_task_1", model_validators=ContextMessage
    )
    await chain_task_signature_1.save()

    chain_task_signature_2 = TaskSignature(
        task_name="chain_task_2", model_validators=ContextMessage
    )
    await chain_task_signature_2.save()

    chain_task_signature_3 = TaskSignature(
        task_name="chain_task_3", model_validators=ContextMessage
    )
    await chain_task_signature_3.save()

    task_signatures = [
        chain_task_signature_1,
        chain_task_signature_2,
        chain_task_signature_3,
    ]

    chain_signature = await mageflow.chain([task.key for task in task_signatures])

    return ChainTestData(
        task_signatures=task_signatures, chain_signature=chain_signature
    )
