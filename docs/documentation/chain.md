# Task Chains

Task chains in the Task Orchestrator provide a powerful way to create sequential workflows where tasks execute one after another, with each task receiving the output of the previous task as input. This enables complex data processing pipelines and multi-step workflows with automatic error handling and completion callbacks.

## What is a Chain?

A chain is a sequence of tasks that execute in order, where:
- Each task receives the result of the previous task as input
- If any task fails, the entire chain stops and error callbacks are triggered
- When all tasks complete successfully, success callbacks are executed with the final result
- The chain manages the lifecycle of all its component tasks

## Creating a Chain

Use `orchestrator.chain()` to create a task chain:

```python
import orchestrator

# Create a simple chain
chain_signature = await orchestrator.chain([task1, task2, task3])

# Create a chain with name and callbacks
chain_signature = await orchestrator.chain(
    tasks=[process_data, validate_results, send_notification],
    name="data-processing-pipeline",
    success=success_callback_task,
    error=error_callback_task,
)
```

!!! info "Alternative Client Usage"
    You can also create chains using the orchestrator client instead of the global `orchestrator` module:

    ```python
    from orchestrator import Orchestrator

    # Create orchestrator client
    hatchet = Orchestrator(hatchet, redis)

    # Use client to create chains
    chain_signature = await hatchet.chain([task1, task2, task3])
    ```

### Parameters

- `tasks`: List of task signatures, task functions, or task names to chain together
- `name`: Optional name for the chain (defaults to the first task's name)
- `success`: Task to execute when the entire chain completes successfully
- `error`: Task to execute when any task in the chain fails

## Data Flow in Chains

### Sequential Processing

Each task in the chain receives the output of the previous task:

```python
@hatchet.task()
async def extract_data(msg: InputMessage) -> DataOutput:
    # Process initial input
    return DataOutput(processed_data="...")


class SecondMessage(BaseModel):
    results: DataOutput

@hatchet.task()  
async def transform_data(msg: SecondMessage) -> TransformedData:
    # Receives DataOutput from extract_data
    return TransformedData(transformed=msg.processed_data)

class ThirdMessage(BaseModel):
    transformed_data: Annotated[TransformedData, ReturnValue()]
    field_int: int

@hatchet.task()
async def save_data(msg: ThirdMessage) -> SaveResult:
    # Receives TransformedData from transform_data
    return SaveResult(saved_id=123)

# Sing second task
extract_data = await orchestrator.sign(extract_data, field_int=123)

# Create the chain
chain = await orchestrator.chain([
    extract_data,
    extract_data, 
    save_data
])
```

Note here that every message receives the output of the previous task via the [ReturnValue](callbacks.md#setting-success-callbacks) field

### Failure Behavior

When a task fails in a chain:
1. Subsequent tasks in the chain are not executed
2. The error callback of the executed task is triggered immediately
3. The error callback of the chain task is triggered immediately

## Example Use Cases

### Data Processing Pipeline

```python
# ETL Pipeline
extract_task = await orchestrator.sign("extract-from-database")
transform_task = await orchestrator.sign("apply-business-rules") 
load_task = await orchestrator.sign("save-to-warehouse")

audit_task = await orchestrator.sign("log-pipeline-completion")
alert_task = await orchestrator.sign("send-failure-alert")

etl_chain = await orchestrator.chain(
    tasks=[extract_task, transform_task, load_task],
    name="daily-etl",
    success=audit_task,
    error=alert_task
)
```

### Document Processing Workflow

```python
# Document processing chain
parse_doc = await orchestrator.sign("parse-document")
extract_entities = await orchestrator.sign("extract-entities")
classify_content = await orchestrator.sign("classify-content")
index_document = await orchestrator.sign("index-in-search")

notify_completion = await orchestrator.sign("notify-user")
handle_processing_error = await orchestrator.sign("handle-doc-error")

doc_chain = await orchestrator.chain(
    tasks=[parse_doc, extract_entities, classify_content, index_document],
    name="document-processing",
    success=notify_completion,
    error=handle_processing_error
)
```

## Why Use Chains?

Chaining tasks is usefull orchestrating tool when multiple tasks are really one task, and a failure of one should stop the entire pipeline.
Using chain methods you can do exactly that. Binding the entire process as a single tasks, in this way you can also treat it as such when using other orchestration tools like swarm.
