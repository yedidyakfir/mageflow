import asyncio

import pytest
from hatchet_sdk.clients.rest import V1TaskStatus
from hatchet_sdk.runnables.types import EmptyModel

import mageflow
from mageflow.signature.model import TaskSignature
from mageflow.swarm.model import SwarmConfig, BatchItemTaskSignature
from tests.integration.hatchet.assertions import (
    assert_redis_is_clean,
    assert_swarm_task_done,
    get_runs,
    assert_signature_done,
    map_wf_by_id,
    assert_overlaps_leq_k_workflows,
    is_wf_done,
    find_sub_calls_by_signature,
)
from tests.integration.hatchet.conftest import HatchetInitData
from tests.integration.hatchet.models import ContextMessage
from tests.integration.hatchet.worker import (
    callback_with_redis,
    fail_task,
    error_callback,
    task1_callback,
    task_with_data,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_with_three_tasks_integration_sanity(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_callback1,
    sign_task1,
    sign_task3,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    sign_task_with_data = await mageflow.sign(
        task_with_data, data="Hello", field_list=[5, 3]
    )
    await sign_callback1.kwargs.aupdate(base_data={"1": 2})
    swarm_tasks = [sign_task1, sign_task_with_data, sign_task3]
    swarm = await mageflow.swarm(
        tasks=swarm_tasks,
        success_callbacks=[sign_callback1],
        kwargs=dict(base_data={"param1": "nice", "param2": ["test", 2]}),
    )
    batch_tasks = await asyncio.gather(
        *[mageflow.load_signature(batch_id) for batch_id in swarm.tasks]
    )
    await swarm.close_swarm()
    tasks = await TaskSignature.afind()

    # Act
    # Test individual tasks directly to verify they work with the message format
    regular_message = ContextMessage(base_data=test_ctx)
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)

    # Wait for all tasks to complete
    await asyncio.sleep(15)

    # Assert
    # Check that all subtasks were called by checking Hatchet runs
    runs = await get_runs(hatchet, ctx_metadata)

    assert_swarm_task_done(runs, swarm, batch_tasks, tasks, allow_fails=False)
    # Check that Redis is clean except for persistent keys
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_with_mixed_success_failed_tasks_integration_edge_case(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
    sign_task2,
    sign_task3,
    sign_fail_task,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    # Create task signatures: 3 success tasks only
    fail_tasks = await sign_fail_task.duplicate_many(3)
    await sign_fail_task.remove()

    swarm_callback_sig = await mageflow.sign(callback_with_redis)
    swarm_error_callback_sig = await mageflow.sign(error_callback)
    reg_tasks = [sign_task1, sign_task2, sign_task3]
    swarm = await mageflow.swarm(
        tasks=reg_tasks + fail_tasks,
        success_callbacks=[swarm_callback_sig],
        error_callbacks=[swarm_error_callback_sig],
        config=SwarmConfig(max_concurrency=2, stop_after_n_failures=2),
    )
    await swarm.close_swarm()

    # Act
    regular_message = ContextMessage(base_data=test_ctx)
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)

    # Wait for tasks to complete
    await asyncio.sleep(60)

    # Assert
    # Get all workflow runs for this test
    runs = await get_runs(hatchet, ctx_metadata)
    wf_names = set([wf.workflow_name for wf in runs])
    workflows_by_name = {
        wf_name: [wf for wf in runs if wf.workflow_name == wf_name]
        for wf_name in wf_names
    }

    # Check that the success callback was not called
    assert (
        callback_with_redis.name not in workflows_by_name
    ), f"Success callback no have been called"

    # Check error callback was activated using the assert function
    assert_signature_done(runs, swarm_error_callback_sig)

    error_callback_runs = workflows_by_name[error_callback.name]
    assert (
        len(error_callback_runs) == 1
    ), f"Error callback no have been called exactly once"

    # Check no task was activated after the last error
    error_wf = workflows_by_name[fail_task.name]
    last_error_wf = sorted(error_wf, key=lambda wf: wf.started_at)[-1]
    assert any(wf.started_at > last_error_wf.started_at for wf in runs)

    # Check that Redis is clean (success callback sets one key)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_mixed_task_all_done_before_closing_task(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
    sign_task2,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    swarm_callback_sig = await mageflow.sign(task1_callback)
    swarm_error_callback_sig = await mageflow.sign(error_callback)
    reg_tasks = [sign_task1, fail_task]
    swarm = await mageflow.swarm(
        tasks=reg_tasks,
        success_callbacks=[swarm_callback_sig],
        error_callbacks=[swarm_error_callback_sig],
        config=SwarmConfig(stop_after_n_failures=2),
    )

    # We set so all the tasks that were sent from here will use this ctx (task marker for this test)
    from hatchet_sdk.runnables.contextvars import ctx_additional_metadata

    add_metadata = ctx_additional_metadata.get() or {}
    add_metadata.update(ctx_metadata)
    ctx_additional_metadata.set(add_metadata)
    tasks = await TaskSignature.afind()

    # Act
    regular_message = ContextMessage(base_data=test_ctx)
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)
    await asyncio.sleep(20)
    new_task = await swarm.add_task(sign_task2)
    await new_task.aio_run_no_wait(regular_message, options=trigger_options)
    batch_tasks = await asyncio.gather(
        *[TaskSignature.get_safe(batch_id) for batch_id in swarm.tasks]
    )
    # Wait for tasks to complete
    await asyncio.sleep(15)
    await swarm.close_swarm()

    # Wait for tasks to complete
    await asyncio.sleep(15)

    # Assert
    # Get all workflow runs for this test
    runs = await get_runs(hatchet, ctx_metadata)
    assert_swarm_task_done(runs, swarm, batch_tasks, tasks)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_mixed_task__not_enough_fails__swarm_finish_successfully(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task2,
    sign_task3,
    sign_fail_task,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    swarm_callback_sig = await mageflow.sign(task1_callback)
    swarm_error_callback_sig = await mageflow.sign(error_callback)
    reg_tasks = [sign_fail_task, sign_task2, sign_task3]
    swarm = await mageflow.swarm(
        tasks=reg_tasks,
        success_callbacks=[swarm_callback_sig],
        error_callbacks=[swarm_error_callback_sig],
        config=SwarmConfig(stop_after_n_failures=2),
        is_swarm_closed=True,
    )
    batch_items = await asyncio.gather(
        *[TaskSignature.get_safe(batch_id) for batch_id in swarm.tasks]
    )
    tasks = await TaskSignature.afind()

    # Act
    regular_message = ContextMessage(base_data=test_ctx)
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)
    await asyncio.sleep(60)

    # Assert
    # Get all workflow runs for this test
    runs = await get_runs(hatchet, ctx_metadata)

    assert_swarm_task_done(runs, swarm, batch_items, tasks)
    await assert_redis_is_clean(redis_client)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_run_concurrently(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task2,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    swarm_tasks = await sign_task2.duplicate_many(8)
    max_concurrency = 4
    swarm = await mageflow.swarm(
        tasks=swarm_tasks,
        config=SwarmConfig(max_concurrency=max_concurrency),
        is_swarm_closed=True,
    )

    # Act
    regular_message = ContextMessage(base_data=test_ctx)
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)
    await asyncio.sleep(60)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)
    wf_by_task_id = map_wf_by_id(runs, also_not_done=True)

    # Check concurrency of the swarm
    swarm_wf = [wf_by_task_id[task.key] for task in swarm_tasks]
    assert_overlaps_leq_k_workflows(swarm_wf, max_concurrency)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_run_finish_at_fail__still_finish_successfully(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_fail_task,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    swarm_tasks = [fail_task]
    task1_callback_sign = await mageflow.sign(task1_callback, base_data=test_ctx)
    swarm = await mageflow.swarm(
        tasks=swarm_tasks,
        success_callbacks=[task1_callback_sign],
        config=SwarmConfig(stop_after_n_failures=10),
        is_swarm_closed=True,
    )

    # Act
    regular_message = ContextMessage()
    await swarm.aio_run_no_wait(regular_message, options=trigger_options)
    await asyncio.sleep(60)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)

    # Check swarm callback was called
    assert_signature_done(runs, task1_callback_sign, base_data=test_ctx)


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_fill_running_tasks_with_success_task(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_task1,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )
    regular_message = ContextMessage(base_data=test_ctx)

    # Create 4 tasks for the swarm
    swarm_tasks = await sign_task1.duplicate_many(4)

    # Create swarm with max_concurrency=3
    swarm = await mageflow.swarm(
        tasks=swarm_tasks,
        config=SwarmConfig(max_concurrency=3),
        is_swarm_closed=True,  # Close swarm to prevent new tasks
        kwargs=regular_message.model_dump(mode="json"),
    )
    first_swarm_item = await BatchItemTaskSignature.get_safe(swarm.tasks[0])
    original_first_task = await mageflow.load_signature(
        first_swarm_item.original_task_id
    )
    await swarm.tasks_left_to_run.aextend(swarm.tasks[1:])

    # Act
    # Run only the first task directly
    await first_swarm_item.aio_run_no_wait(EmptyModel(), options=trigger_options)
    await asyncio.sleep(13)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)

    tasks_called_by_first_task = find_sub_calls_by_signature(original_first_task, runs)

    # Verify exactly 3 tasks were started by fill_running_tasks
    assert (
        len(tasks_called_by_first_task) == 3
    ), "fill_running_tasks should start exactly 3 tasks"


@pytest.mark.asyncio(loop_scope="session")
async def test_swarm_fill_running_tasks_with_failed_task(
    hatchet_client_init: HatchetInitData,
    test_ctx,
    ctx_metadata,
    trigger_options,
    sign_fail_task,
    sign_task1,
    sign_task2,
    sign_task3,
):
    # Arrange
    redis_client, hatchet = (
        hatchet_client_init.redis_client,
        hatchet_client_init.hatchet,
    )

    # Create swarm with fail_task first and 3 other tasks
    swarm_tasks = [sign_fail_task, sign_task1, sign_task2, sign_task3]

    # Create swarm with max_concurrency=3
    regular_message = ContextMessage(base_data=test_ctx)
    swarm = await mageflow.swarm(
        tasks=swarm_tasks,
        config=SwarmConfig(max_concurrency=3),
        is_swarm_closed=True,  # Close swarm to prevent new tasks
        kwargs=regular_message.model_dump(mode="json"),
    )
    first_swarm_item = await BatchItemTaskSignature.get_safe(swarm.tasks[0])
    original_first_task = await mageflow.load_signature(
        first_swarm_item.original_task_id
    )
    await swarm.tasks_left_to_run.aextend(swarm.tasks[1:])

    # Act
    # Run only the first (failing) task directly
    await first_swarm_item.aio_run_no_wait(regular_message, options=trigger_options)
    await asyncio.sleep(13)

    # Assert
    runs = await get_runs(hatchet, ctx_metadata)

    tasks_called_by_first_task = find_sub_calls_by_signature(original_first_task, runs)

    # Verify exactly 3 tasks were started by fill_running_tasks
    assert (
        len(tasks_called_by_first_task) == 3
    ), "fill_running_tasks should start exactly 3 tasks"
