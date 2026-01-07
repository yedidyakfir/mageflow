from typing import Optional

import rapyer
from rapyer import AtomicRedisModel
from rapyer.errors.base import KeyNotFound
from typing_extensions import deprecated


def does_task_wants_ctx(func) -> bool:
    return getattr(func, "__user_ctx__", False)


@deprecated("Use this untile rapyer provide safe option for aget")
async def rapyer_aget_safe(redis_key: str) -> Optional[AtomicRedisModel]:
    try:
        return await rapyer.aget(redis_key)
    except KeyNotFound:
        return None
