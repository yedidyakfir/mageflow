from contextvars import ContextVar
from typing import Optional, Self

from hatchet_sdk.runnables.types import EmptyModel
from mageflow.signature.model import TaskSignature
from mageflow.swarm.creator import swarm
from mageflow.swarm.model import SwarmTaskSignature, SwarmConfig
from pydantic import Field

current_root_swarm: ContextVar[Optional["SwarmTaskSignature"]] = ContextVar(
    "current_root_swarm", default=None
)


class RootTaskSignature(TaskSignature):
    swarm_id: Optional[str] = None
    swarm_config: SwarmConfig = Field(default_factory=SwarmConfig)

    async def create_swarm(self) -> SwarmTaskSignature:
        root_swarm = await swarm(
            task_name=f"root-swarm:{self.task_name}",
            config=self.swarm_config,
            success_callbacks=list(self.success_callbacks),
            error_callbacks=list(self.error_callbacks),
        )
        await root_swarm.asave()
        await self.aupdate(
            swarm_id=root_swarm.key, success_callbacks=[], error_callbacks=[]
        )
        return root_swarm

    async def start_task(self) -> Self:
        await super().start_task()
        root_swarm = await self.create_swarm()
        current_root_swarm.set(root_swarm)
        return self

    async def end_task(self, success: bool = True) -> Self:
        current_root_swarm.set(None)
        root_swarm = await SwarmTaskSignature.get_safe(self.swarm_id)
        if root_swarm:
            await root_swarm.close_swarm()
        if not success:
            # TODO - should be interrupt once implemented then set to delete
            await root_swarm.suspend()
            await self.aupdate(error_callbacks=root_swarm.error_callbacks)
        return await super().end_task(success)
