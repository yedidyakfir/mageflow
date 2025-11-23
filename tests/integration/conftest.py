import pytest_asyncio
import redis.asyncio
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=["settings.toml", ".secrets.toml"],
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
def redis_client():
    redis_client = redis.asyncio.from_url(settings.redis.url, max_connections=10)

    yield redis_client


@pytest_asyncio.fixture(scope="function", loop_scope="session")
async def real_redis(redis_client):
    current_keys = await redis_client.keys("*")
    yield redis_client
    all_keys = await redis_client.keys("*")
    delete_keys = [key for key in all_keys if key not in current_keys]
    if delete_keys:
        await redis_client.delete(*delete_keys)
    await redis_client.close()
