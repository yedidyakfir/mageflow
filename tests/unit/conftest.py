import fakeredis
import pytest
import pytest_asyncio
import rapyer
from hatchet_sdk import Hatchet, ClientConfig

import mageflow
from mageflow.chain.model import ChainTaskSignature
from mageflow.signature.model import SIGNATURES_NAME_MAPPING, TaskSignature
from mageflow.startup import update_register_signature_models, mageflow_config


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
