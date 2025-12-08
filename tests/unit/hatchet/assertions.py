from mageflow.signature.model import TaskSignature


async def assert_tasks_not_exists(tasks_ids: list[str]):
    for task_id in tasks_ids:
        reloaded_signature = await TaskSignature.from_id(task_id)
        assert reloaded_signature is None


async def assert_tasks_changed_status(
    tasks_ids: list[str | TaskSignature], status: str, old_status: str = None
):
    tasks_ids = tasks_ids if isinstance(tasks_ids, list) else [tasks_ids]
    all_tasks = []
    for task_id in tasks_ids:
        task_id = task_id.id if isinstance(task_id, TaskSignature) else task_id
        reloaded_signature = await TaskSignature.from_id(task_id)
        all_tasks.append(reloaded_signature)
        assert reloaded_signature.task_status.status == status
        if old_status:
            assert reloaded_signature.task_status.last_status == old_status
    return all_tasks


async def assert_redis_keys_do_not_contain_sub_task_ids(redis_client, sub_task_ids):
    all_keys = await redis_client.keys("*")
    all_keys_str = [
        key.decode() if isinstance(key, bytes) else str(key) for key in all_keys
    ]

    for sub_task_id in sub_task_ids:
        sub_task_id_str = str(sub_task_id)
        keys_containing_sub_task = [
            key for key in all_keys_str if sub_task_id_str in key
        ]
        assert (
            not keys_containing_sub_task
        ), f"Found Redis keys containing deleted sub-task ID {sub_task_id}: {keys_containing_sub_task}"
