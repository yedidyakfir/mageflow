import os
from unittest.mock import MagicMock, AsyncMock

import pytest
import pytest_asyncio
import redis.asyncio
from redis import Redis


@pytest.fixture
def redis_mock():
    redis_client = MagicMock(spec=Redis)
    redis_client.get = AsyncMock(return_value=None)
    redis_client.set = AsyncMock()
    redis_client.delete = AsyncMock()

    yield redis_client


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def real_redis():
    redis_url = os.getenv("REDIS__URL")
    redis_client = redis.asyncio.from_url(redis_url)
    current_keys = await redis_client.keys("*")
    yield redis_client
    all_keys = await redis_client.keys("*")
    delete_keys = [key for key in all_keys if key not in current_keys]
    if delete_keys:
        await redis_client.delete(*delete_keys)
    await redis_client.close()
