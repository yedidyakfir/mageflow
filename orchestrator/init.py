import redis
from hatchet_sdk import Hatchet, ClientConfig
from pydantic import BaseModel
from redis.asyncio.client import Redis

from config import settings


class ConfigModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class OrchestratorConfigModel(ConfigModel):
    hatchet_client: Hatchet | None = None
    redis_client: Redis | None = None

    def set_from_dynaconf(self):
        if settings.redis and settings.hatchet:
            self.redis_client = redis.asyncio.from_url(settings.redis.url)
            token = settings.hatchet.api_key

            config_obj = ClientConfig(token=token, **dict(settings.hatchet))
            self.hatchet_client = Hatchet(debug=True, config=config_obj)


orchestrator_config = OrchestratorConfigModel()
