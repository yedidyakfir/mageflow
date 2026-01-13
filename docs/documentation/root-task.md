# Root Tasks

Root tasks in MageFlow provide a convenient way to manage multiple parallel task executions by automatically creating and managing an internal swarm. When you need a task that spawns other tasks dynamically and wants to wait for all of them to complete before triggering callbacks, root tasks simplify this pattern significantly.

## What is a Root Task?

A root task creates an **internal swarm** that:
- Captures all tasks published within the root task body
- Runs them in parallel with managed concurrency
- Waits for ALL tasks to finish before triggering callbacks
- Uses the same configuration options as a regular swarm (SwarmConfig)

This is particularly useful for "orchestrator" tasks that spawn multiple child tasks and need to track their completion as a group.

## Creating a Root Task

Use the `@hatchet.root_task()` decorator alongside `@hatchet.task()`:

```python
from mageflow import Mageflow

hatchet = Mageflow(hatchet_client, redis)

@hatchet.task()
@hatchet.root_task(max_concurrency=4, stop_after_n_failures=2)
async def process_batch(msg: BatchMessage):
    # All tasks published here are added to the internal swarm
    for item in msg.items:
        task_sig = await mageflow.sign(process_item)
        await task_sig.aio_run_no_wait(ItemMessage(data=item))

    return {"processed": len(msg.items)}
```

!!! info "Alternative Client Usage"
    You can also use the global `mageflow` module for signing tasks inside the root task:

    ```python
    import mageflow

    @hatchet.task()
    @hatchet.root_task(max_concurrency=10)
    async def my_root_task(msg: InputMessage):
        task_sig = await mageflow.sign(child_task)
        await task_sig.aio_run_no_wait(msg)
    ```

### Parameters (SwarmConfig)

The `@hatchet.root_task()` decorator accepts the same configuration options as SwarmConfig:

- `max_concurrency`: Maximum number of child tasks that can run in parallel (default: 30)
- `stop_after_n_failures`: Stop the internal swarm after N task failures (optional, defaults to None - meaning continue despite failures)

These parameters are passed directly to the internal swarm that gets created when the root task starts.

## How Root Tasks Work

Understanding the lifecycle of a root task helps you use it effectively:

1. **Task Start**: When the root task begins execution, an internal swarm is created with the name `root-swarm:{task_name}`
2. **Task Capture**: Any task published via `aio_run()` or `aio_run_no_wait()` within the root task body is automatically added to this internal swarm
3. **Concurrency Management**: The internal swarm manages how many child tasks run in parallel based on `max_concurrency`
4. **Task Completion**: When the root task function returns, the internal swarm is closed (no more tasks can be added)
5. **Callback Execution**: Success/error callbacks attached to the root task only trigger after ALL child tasks in the internal swarm finish

```python
@hatchet.task()
@hatchet.root_task(max_concurrency=5)
async def orchestrator(msg: OrchestratorMessage):
    # Create and run multiple child tasks
    for i in range(10):
        sig = await mageflow.sign(worker_task)
        await sig.aio_run_no_wait(WorkerMessage(index=i))

    # Root task returns, but callbacks wait for all 10 tasks
    return {"spawned": 10}

# Callbacks only execute after all 10 worker tasks complete
root_sig = await mageflow.sign(
    orchestrator,
    success_callbacks=[notify_completion],
    error_callbacks=[handle_failure]
)
await root_sig.aio_run_no_wait(msg)
```

## Root Tasks with Chains and Swarms

Root tasks can contain chains and explicit swarms inside them. These are also captured by the root task's internal swarm:

```python
@hatchet.task()
@hatchet.root_task(max_concurrency=10)
async def complex_orchestrator(msg: InputMessage):
    # Create a chain - will be tracked by root swarm
    chain_sig = await hatchet.chain([task1, task2, task3])
    await chain_sig.aio_run_no_wait(msg)

    # Create an explicit swarm - also tracked by root swarm
    swarm_sig = await hatchet.swarm(
        tasks=[worker1, worker2],
        is_swarm_closed=True
    )
    await swarm_sig.aio_run_no_wait(msg)

    # Individual tasks
    single_sig = await mageflow.sign(single_task)
    await single_sig.aio_run_no_wait(msg)

    return {"completed": True}
```

## Excluding Tasks from the Root Swarm

Sometimes you need to publish a task from within a root task without adding it to the internal swarm. Use the `without_root_swarm` context manager for this:

```python
from mageflow.root.context import without_root_swarm

@hatchet.task()
@hatchet.root_task(max_concurrency=5)
async def orchestrator(msg: OrchestratorMessage):
    # This task IS added to the root swarm
    sig1 = await mageflow.sign(tracked_task)
    await sig1.aio_run_no_wait(msg)

    # This task is NOT added to the root swarm
    with without_root_swarm():
        sig2 = await mageflow.sign(independent_task)
        await sig2.aio_run_no_wait(msg)

    # Back to normal - this IS added to the root swarm
    sig3 = await mageflow.sign(another_tracked_task)
    await sig3.aio_run_no_wait(msg)

    return {"done": True}
```

Tasks published inside the `without_root_swarm()` context:
- Run independently of the root task's internal swarm
- Are not subject to the root task's `max_concurrency` limit
- Do not block the root task's callbacks from executing
- Are useful for "fire and forget" tasks that shouldn't affect the root task completion

## Root Tasks vs Explicit Swarms

| Aspect | Root Task | Explicit Swarm |
|--------|-----------|----------------|
| Creation | `@hatchet.root_task()` decorator | `mageflow.swarm()` |
| Task capture | Automatic (context-based) | Manual (`add_task()`) |
| Closing | Automatic when task returns | Manual (`close_swarm()`) |
| Callbacks | On decorator, wait for all children | On swarm creation |
| Use case | Dynamic task spawning within a task | Batch processing known tasks |

## Example Use Cases

### Dynamic Batch Processing

Process items where you don't know the count upfront:

```python
@hatchet.task()
@hatchet.root_task(max_concurrency=20, stop_after_n_failures=5)
async def process_query_results(msg: QueryMessage):
    # Fetch items dynamically
    items = await fetch_items_from_database(msg.query)

    # Spawn a task for each item
    for item in items:
        sig = await mageflow.sign(process_item)
        await sig.aio_run_no_wait(ItemMessage(id=item.id))

    return {"total_items": len(items)}

# Run with callbacks
notify = await mageflow.sign(send_completion_notification)
root = await mageflow.sign(
    process_query_results,
    success_callbacks=[notify]
)
await root.aio_run_no_wait(QueryMessage(query="SELECT * FROM items"))
```

### Agent Task Spawning

When an AI agent needs to spawn multiple sub-tasks:

```python
@hatchet.task()
@hatchet.root_task(max_concurrency=5)
async def agent_executor(msg: AgentMessage):
    # Agent decides what tasks to run
    plan = await generate_execution_plan(msg.goal)

    for step in plan.steps:
        task_func = get_task_for_step(step)
        sig = await mageflow.sign(task_func)
        await sig.aio_run_no_wait(StepMessage(step=step))

    return {"steps_spawned": len(plan.steps)}
```

### Fan-out/Fan-in Pattern

Distribute work and collect results:

```python
@hatchet.task()
@hatchet.root_task(max_concurrency=10)
async def distributed_analysis(msg: AnalysisMessage):
    # Fan-out: spawn analysis tasks for each data source
    for source in msg.data_sources:
        sig = await mageflow.sign(analyze_source)
        await sig.aio_run_no_wait(SourceMessage(source=source))

    return {"sources": len(msg.data_sources)}

# Fan-in: aggregate results in callback
aggregate = await mageflow.sign(aggregate_results)
root = await mageflow.sign(
    distributed_analysis,
    success_callbacks=[aggregate]
)
```

## Why Use Root Tasks?

Root tasks are ideal when:

- **Dynamic Task Spawning**: You don't know at design time how many tasks will be spawned
- **Simplified Orchestration**: You want automatic tracking without explicit swarm management
- **Callback Coordination**: You need callbacks to wait for all dynamically spawned tasks
- **Clean Code**: You prefer a decorator-based approach over manual swarm lifecycle management

The key benefit is that root tasks give you swarm-like behavior (parallel execution, concurrency control, failure handling) without the boilerplate of creating, managing, and closing a swarm explicitly.
