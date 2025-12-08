import pytest


@pytest.mark.asyncio(loop_scope="session")
async def test__timeout_task__call_error_callback():
    pass
