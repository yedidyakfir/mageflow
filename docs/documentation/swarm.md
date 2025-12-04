# Task Swarms

Task swarms in the Task Orchestrator provide a powerful way to run multiple tasks in parallel with controlled concurrency. Unlike chains where tasks run sequentially, swarms allow you to manage a group of tasks that execute simultaneously while controlling how many can run at once and when to trigger callbacks for the entire group.

## What is a Swarm?

A swarm is a collection of tasks that execute in parallel, where:
- Multiple tasks run concurrently with configurable limits
- Tasks can be added dynamically to the swarm queue
- Callbacks are triggered when all tasks complete or when failure conditions are met
- The swarm manages the lifecycle and concurrency of all its component tasks

## Creating a Swarm

Use `orchestrator.swarm()` to create a task swarm:

```python
import orchestrator

# Create a simple swarm
swarm_signature = await orchestrator.swarm(tasks=[task1, task2, task3])

# Create a swarm with concurrency control and callbacks
swarm_signature = await orchestrator.swarm(
    tasks=[process_file1, process_file2, process_file3],
    success_callbacks=[completion_callback],
    error_callbacks=[error_handler],
    config=SwarmConfig(max_concurrency=2),
)
```

!!! info "Alternative Client Usage"
    You can also create swarms using the orchestrator client instead of the global `orchestrator` module:

    ```python
    from orchestrator import Orchestrator

    # Create orchestrator client
    hatchet = Orchestrator(hatchet, redis)

    # Use client to create swarms
    swarm_signature = await hatchet.swarm(tasks=[task1, task2, task3])
    ```

### Parameters

- `tasks`: List of task signatures, task functions, or task names to run in parallel
- `success_callbacks`: Tasks to execute when all tasks complete successfully
- `error_callbacks`: Tasks to execute when failure conditions are met
- `config`: SwarmConfig object to control swarm behavior
- `is_swarm_closed`: Whether the swarm should be closed immediately (defaults to False)


## Managing Swarm Lifecycle

### Starting a Swarm
You can start a swarm like any other task with the `aio_run_no_wait` method:
```python
# Create swarm (not closed yet)
swarm = await orchestrator.swarm(tasks=[initial_task])
await swarm.aio_run_no_wait(message)

# The swarm will:
# 1. Start tasks up to max_concurrency limit
# 2. Queue remaining tasks
# 3. Start queued tasks as running tasks complete
# 4. Trigger callbacks when all tasks finish or failure conditions are met

```
In this case, all the tasks in the swarm will recieve the message once they got a task slot to run (the number of slots can be configured with the max_concurrency parameter in SwarmConfig).


### Adding Tasks

You can also add tasks after you created the swarm, to run it, in that case you should also run them so they will be added to the swarm ready queue: 

```python
# Create swarm (not closed yet)
swarm = await orchestrator.swarm(tasks=[initial_task])

# Add more tasks while swarm is running
new_task = await swarm.add_task(additional_task)
# Add the task to the queue to be run once the swarm can allocate a task slot 
await new_task.aio_run_no_wait(message)
```
In this way, the task will recieve the message sent to new_task

If we run the swarm as well as running individual added tasks, the message to the task will include both the parameters from the message sent to the swarm and the parameters from the message sent to the new task.
You can configure the message model to ignore extra fields so this merge wont effect the message the task recieve.

```python
class NewTaskMessage(BaseModel):
    data: str
    
    # Ignore extra fields
    model_config = ConfigDict(extra="ignore")
    
class SwarmMessage(BaseModel):
    swarm_data: str

@hatchet.task()
async def new_task(message: NewTaskMessage):
    print(message.data)

swarm = await orchestrator.swarm(tasks=[initial_task])
await swarm.aio_run_no_wait(SwarmMessage(swarm_data="swarm_data"))

# Add more tasks while swarm is running
new_task = await swarm.add_task(additional_task)
# Add the task to the queue to be run once the swarm can allocate a task slot 
await new_task.aio_run_no_wait(message)
```

### Closing a Swarm
When you want finish adding tasks to the swarm, you can close it.

```python
# Close the swarm to prevent new tasks and trigger completion
await swarm.close_swarm()

# Or create a pre-closed swarm
swarm = await orchestrator.swarm(
    tasks=task_list,
    is_swarm_closed=True  # No new tasks can be added
)
```
Once the swarm is closed, it will not accept new tasks and will trigger completion callbacks when all tasks complete.

You can also create swarm that is already closed

```python
# Create swarm (not closed yet)
swarm = await orchestrator.swarm(tasks=[initial_task], is_swarm_closed=True)
```

## Concurrency Control

Swarms automatically manage task concurrency:

```python
# Example: Process 20 files with max 5 concurrent
file_tasks = [
    await orchestrator.sign("process-file", file_path=f"file_{i}.txt") 
    for i in range(20)
]

swarm = await orchestrator.swarm(
    tasks=file_tasks,
    config=SwarmConfig(max_concurrency=5),
    is_swarm_closed=True
)

# Only 5 tasks run simultaneously
# As each completes, the next queued task starts
await swarm.aio_run_no_wait(ProcessMessage())
```

This is espilcally usefull where you want to manage a suddent peak in tasks, without deploying new workers to support the load.

## Failure Handling

Control how swarms handle task failures:

```python
# Stop after 3 failures
swarm = await orchestrator.swarm(
    tasks=risky_tasks,
    error_callbacks=[handle_swarm_failure],
    config=SwarmConfig(stop_after_n_failures=3)
)

# Continue despite individual failures (no stop limit)
swarm = await orchestrator.swarm(
    tasks=optional_tasks,
    success_callbacks=[process_results],  # Called even if some tasks fail
    config=SwarmConfig(stop_after_n_failures=None)
)
```

## Swarm Callback
The swarm will trigger callbacks when all tasks completed. The callback will recieve a list of all the tasks results (see [ReturnValue Annotation](callbacks.md#setting-success-callbacks) docs).

## Example Use Cases

### Parallel File Processing

```python
# Process multiple files concurrently
file_paths = ["file1.csv", "file2.csv", "file3.csv"]
process_tasks = [
    await orchestrator.sign("process-csv-file", file_path=path)
    for path in file_paths
]

consolidate_results = await orchestrator.sign("consolidate-results")
handle_processing_errors = await orchestrator.sign("handle-file-errors")

file_swarm = await orchestrator.swarm(
    tasks=process_tasks,
    success_callbacks=[consolidate_results],
    error_callbacks=[handle_processing_errors],
    config=SwarmConfig(max_concurrency=3),
    is_swarm_closed=True
)

await file_swarm.aio_run_no_wait(ProcessingMessage())
```

### Dynamic Task Queue

```python
# Start with initial tasks, add more dynamically
initial_tasks = [await orchestrator.sign("initial-task")]
notification_task = await orchestrator.sign("notify-completion")

swarm = await orchestrator.swarm(
    tasks=initial_tasks,
    success_callbacks=[notification_task],
    config=SwarmConfig(max_concurrency=10)
)

# Start the swarm
await swarm.aio_run_no_wait(InitialMessage())

# Add tasks as needed
for data_item in dynamic_data_stream:
    new_task = await orchestrator.sign("process-item", data=data_item)
    await swarm.add_task(new_task)
    await new_task.aio_run_no_wait(ProcessMessage())

# Close when done adding tasks
await swarm.close_swarm()
```

### Batch Processing with Error Tolerance

```python
# Process batch with some failure tolerance
batch_tasks = [
    await orchestrator.sign("process-record", record_id=i)
    for i in range(1000)
]

completion_report = await orchestrator.sign("generate-completion-report")
failure_alert = await orchestrator.sign("send-failure-alert")

batch_swarm = await orchestrator.swarm(
    tasks=batch_tasks,
    success_callbacks=[completion_report],
    error_callbacks=[failure_alert],
    config=SwarmConfig(
        max_concurrency=20,
        stop_after_n_failures=50  # Stop if more than 50 records fail
    ),
    is_swarm_closed=True
)

await batch_swarm.aio_run_no_wait(BatchMessage())
```

## Why Use Swarms?

Swarms are ideal when you have multiple independent tasks that can run in parallel:
- **Parallel Processing**: Execute multiple tasks simultaneously for better performance
- **Concurrency Control**: Limit resource usage while maximizing throughput
- **Dynamic Scaling**: Add tasks to the queue as needed
- **Failure Management**: Control how task failures affect the overall operation
- **Resource Management**: Prevent overwhelming downstream systems with configurable concurrency limits