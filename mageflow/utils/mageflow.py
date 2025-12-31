def does_task_wants_ctx(func) -> bool:
    return getattr(func, "__user_ctx__", False)