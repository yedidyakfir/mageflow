import asyncio
from datetime import datetime

from hatchet_sdk import Hatchet
from hatchet_sdk.clients.rest import V1TaskStatus, V1TaskSummary

from orchestrator.chain.model import ChainTaskSignature
from orchestrator.signature.consts import TASK_ID_PARAM_NAME
from orchestrator.signature.model import TaskSignature
from orchestrator.signature.types import TaskIdentifierType
from orchestrator.swarm.model import SwarmTaskSignature, BatchItemTaskSignature
from orchestrator.workflows import TASK_DATA_PARAM_NAME
from tests.integration.hatchet.conftest import extract_bad_keys_from_redis

WF_MAPPING_TYPE = dict[str, V1TaskSummary]
HatchetRuns = list[V1TaskSummary]


async def get_runs(hatchet: Hatchet, ctx_metadata: dict) -> HatchetRuns:
    runs = await hatchet.runs.aio_list(additional_metadata=ctx_metadata)
    runs_by_id = {wf.task_external_id: wf for wf in runs.rows}
    # Retrieve tasks data
    wf_tasks = await asyncio.gather(
        *[hatchet.runs.aio_get_task_run(wf.task_external_id) for wf in runs.rows]
    )
    for wf in wf_tasks:
        wf.workflow_name = runs_by_id[wf.task_external_id].workflow_name
    return wf_tasks


def map_wf_by_id(runs: HatchetRuns, also_not_done: bool = False) -> WF_MAPPING_TYPE:
    return {
        task_id: wf
        for wf in runs
        if (task_id := get_task_param(wf, TASK_ID_PARAM_NAME))
        if also_not_done or is_wf_done(wf)
    }


def is_wf_done(wf: V1TaskSummary) -> bool:
    wf_output = wf.output or {}
    completed = wf.status == V1TaskStatus.COMPLETED
    task_succeeded = completed and "hatchet_results" in wf_output
    return task_succeeded or wf.status == V1TaskStatus.FAILED


def is_task_paused(wf: V1TaskSummary) -> bool:
    wf_output = wf.output or {}
    res = wf_output.get("hatchet_results")
    return res == {}


def get_task_param(wf: V1TaskSummary, param_name: str):
    return wf.additional_metadata.get(TASK_DATA_PARAM_NAME, {}).get(param_name)


def assert_task_done(runs: HatchetRuns, task, input_params=None, results=None):
    __tracebackhide__ = False  # force pytest to show this frame
    workflows_by_name = {wf.workflow_name: wf for wf in runs}
    return _assert_task_done(task.name, workflows_by_name, input_params, results)


def assert_signature_done(
    runs: HatchetRuns,
    task_sign: TaskSignature | TaskIdentifierType,
    results=None,
    check_called_once=True,
    check_finished_once=True,
    allow_fails=False,
    **input_params,
) -> V1TaskSummary:
    if isinstance(task_sign, TaskSignature):
        task_sign = task_sign.id

    if check_called_once or check_finished_once:
        task_id_calls = [
            wf
            for wf in runs
            if get_task_param(wf, TASK_ID_PARAM_NAME) == task_sign
            # If we just want to check that the task was finished once,
            # In this case it is ok if the task was called more than once (For suspended tasks cases)
            if check_called_once or is_wf_done(wf)
        ]
        assert (
            len(task_id_calls) == 1
        ), f"Task {task_sign} was called more than once or not at all: {task_id_calls}"

    wf_by_task_id = map_wf_by_id(runs, also_not_done=True)
    return _assert_task_done(
        task_sign, wf_by_task_id, input_params, results, allow_fails
    )


def _assert_task_done(
    task_id: str,
    wf_map: WF_MAPPING_TYPE,
    input_params: dict = None,
    results=None,
    allow_fails=False,
) -> V1TaskSummary:
    assert task_id in wf_map
    task_workflow = wf_map[task_id]
    if not allow_fails:
        assert (
            task_workflow.status == V1TaskStatus.COMPLETED
        ), f"{task_workflow.workflow_name} didn't finish"
    if input_params is not None:
        assert (
            input_params.keys() <= task_workflow.input["input"].keys()
        ), f"missing params {input_params.keys() - task_workflow.input['input'].keys()}"
        assert (
            input_params.items() <= task_workflow.input["input"].items()
        ), f"{task_workflow.workflow_name} has some missing parameters - {input_params.items() - task_workflow.input['input'].items()}"
    if results is not None:
        task_res = task_workflow.output["hatchet_results"]
        assert (
            task_res == results
        ), f"{task_workflow.workflow_name} has different results than expected: {task_res}"
    return task_workflow


async def assert_redis_is_clean(redis_client):
    __tracebackhide__ = False  # force pytest to show this frame
    non_persistent_keys = await extract_bad_keys_from_redis(redis_client)
    assert (
        len(non_persistent_keys) == 0
    ), f"Not all redis keys were cleaned: {non_persistent_keys}"


async def assert_task_was_paused(
    runs: HatchetRuns, task: TaskSignature | TaskIdentifierType, with_resume=False
):
    __tracebackhide__ = False  # force pytest to show this frame
    task_id = task.id if isinstance(task, TaskSignature) else task
    wf_by_task_id = map_wf_by_id(runs, also_not_done=True)

    # Check kwargs were stored
    hatchet_call = wf_by_task_id[task_id]
    assert hatchet_call.status == V1TaskStatus.CANCELLED
    expected_dump = task.model_validators.validate(hatchet_call.input["input"])
    updated_callback_signature = await TaskSignature.from_id(task_id)
    for key, value in expected_dump.model_dump().items():
        assert updated_callback_signature.kwargs[key] == value, f"{key} != {value}"

    if with_resume:
        wf_by_task_id = map_wf_by_id(runs)
        _assert_task_done(task_id, wf_by_task_id, None)


def assert_tasks_in_order(wf_by_signature: WF_MAPPING_TYPE, tasks: list[TaskSignature]):
    # Check the task in a chain were called in order
    for i in range(len(tasks) - 1):
        curr_wf = wf_by_signature[tasks[i].id]
        assert (
            curr_wf.status == V1TaskStatus.COMPLETED
        ), f"Task {curr_wf.workflow_name} - {curr_wf.status}"
        next_wf = wf_by_signature[tasks[i + 1].id]
        assert (
            curr_wf.started_at < next_wf.started_at
        ), f"Task {curr_wf.workflow_name} started after {next_wf.workflow_name}"


def assert_signature_not_called(runs: HatchetRuns, task_sign: TaskSignature | str):
    wf_by_signature = map_wf_by_id(runs, also_not_done=True)
    if isinstance(task_sign, TaskSignature):
        task_sign = task_sign.id

    assert task_sign not in wf_by_signature


def assert_swarm_task_done(
    runs: HatchetRuns,
    swarm_task: SwarmTaskSignature,
    batch_items: list[BatchItemTaskSignature],
):
    batch_map = {batch_item.id: batch_item for batch_item in batch_items}

    # Assert for a batch task done as well as extract the wf
    swarm_runs = [
        assert_signature_done(
            runs,
            batch_map[batch_id].original_task_id,
            check_called_once=False,
            check_finished_once=True,
            allow_fails=True,
        )
        for batch_id in swarm_task.tasks
    ]

    expected_output = [
        task_output.get("hatchet_results")
        for wf in swarm_runs
        if wf.status == V1TaskStatus.COMPLETED
        if (task_output := wf.output)
        if "hatchet_results" in task_output
    ]
    for callback_sign in swarm_task.success_callbacks:
        callback_wf = assert_signature_done(runs, callback_sign, check_called_once=True)
        for result in callback_wf.input["input"]["task_result"]:
            assert (
                result in expected_output
            ), f"{result} not found in {expected_output} for callback {callback_wf.workflow_name}"

    for error_callback_sign in swarm_task.error_callbacks:
        assert_signature_not_called(runs, error_callback_sign)


def assert_chain_done(
    runs: HatchetRuns,
    chain_signature: ChainTaskSignature,
    chain_tasks: list[TaskSignature],
):
    wf_by_signature = map_wf_by_id(runs)
    assert_tasks_in_order(wf_by_signature, chain_tasks)
    for chain_task_id in chain_signature.tasks:
        _assert_task_done(chain_task_id, wf_by_signature, None)

    for chain_success in chain_signature.success_callbacks:
        _assert_task_done(chain_success, wf_by_signature, None)


def assert_paused(runs: HatchetRuns, start_time: datetime, end_time: datetime):
    wf_by_task_id = map_wf_by_id(runs, also_not_done=True)
    for wf in wf_by_task_id.values():
        task_start_time = wf.started_at
        start_time = start_time.astimezone(task_start_time.tzinfo)
        start_before_pause = task_start_time < start_time
        end_time = end_time.astimezone(task_start_time.tzinfo)
        started_after_pause = task_start_time > end_time
        assert start_before_pause or started_after_pause

    paused_tasks = [wf for wf in wf_by_task_id.values() if is_task_paused(wf)]
    for paused_wf in paused_tasks:
        task_id = get_task_param(paused_wf, TASK_ID_PARAM_NAME)
        assert_task_was_paused(runs, task_id)


def assert_task_did_not_repeat(runs: HatchetRuns):
    task_done = [
        task_id
        for wf in runs
        if (task_id := get_task_param(wf, TASK_ID_PARAM_NAME))
        if is_wf_done(wf)
    ]

    assert len(task_done) == len(set(task_done)), "Task repeated"
