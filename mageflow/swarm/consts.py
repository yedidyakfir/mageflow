# Params
from mageflow.signature.consts import MAGEFLOW_TASK_INITIALS

BATCH_TASK_NAME_INITIALS = "batch-task-"
SWARM_TASK_ID_PARAM_NAME = "swarm_task_id"
SWARM_ITEM_TASK_ID_PARAM_NAME = "swarm_item_id"


# Tasks
ON_SWARM_START = f"{MAGEFLOW_TASK_INITIALS}on_swarm_start"
ON_SWARM_ERROR = f"{MAGEFLOW_TASK_INITIALS}on_swarm_error"
ON_SWARM_END = f"{MAGEFLOW_TASK_INITIALS}on_swarm_done"
