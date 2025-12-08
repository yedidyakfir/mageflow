# Signature API Reference

This page provides detailed API documentation for task signature functionality in MageFlow.

## mageflow.sign()

Create a new task signature.

```python
async def sign(task: str | HatchetTaskType, **options: Any) -> TaskSignature
```

**Parameters:**
- `task`: Task name (string) or HatchetTask instance to create signature for
- `**options`: Additional signature options including:
  - `kwargs`: Dictionary of task parameters
  - `creation_time`: Timestamp when signature was created
  - `model_validators`: Validation models for task input
  - `success_callbacks`: List of task IDs to execute on success
  - `error_callbacks`: List of task IDs to execute on error
  - `task_status`: Initial status for the task
  - `task_identifiers`: Additional identifier mappings

**Returns:** `TaskSignature` - The created task signature

## TaskSignature

The main signature class that manages task execution and lifecycle.

### Properties

- `task_name`: Name of the task
- `kwargs`: Dictionary of task parameters
- `creation_time`: When the signature was created
- `success_callbacks`: Tasks executed when task completes successfully
- `error_callbacks`: Tasks executed when task fails
- `task_status`: Current status information
- `task_identifiers`: Additional identifier mappings
- `id`: Unique identifier for the signature

### Class Methods

#### delete_signature()

Delete a signature by ID.

```python
@classmethod
async def delete_signature(cls, task_id: TaskIdentifierType)
```

### Instance Methods

#### aio_run_no_wait()

Execute the task asynchronously without waiting for completion.

```python
async def aio_run_no_wait(self, msg: BaseModel, **kwargs)
```

**Parameters:**
- `msg`: Message object to pass to the task
- `**kwargs`: Additional execution parameters

#### add_callbacks()

Add success and error callbacks to the signature.

```python
async def add_callbacks(
    self,
    success: list[TaskSignature] = None,
    errors: list[TaskSignature] = None
)
```

#### remove()

Remove the signature and optionally its callbacks.

```python
async def remove(self, with_error: bool = True, with_success: bool = True)
```

### Lifecycle Management

#### suspend()

Suspend task execution before it starts.

```python
async def suspend()
```

Sets task status to `SUSPENDED`. The task will not execute until resumed.

#### resume()

Resume a suspended task.

```python
async def resume()
```

Restores the previous status and re-triggers execution if needed.

#### interrupt()

Aggressively interrupt task execution.

```python
async def interrupt()
```

**Note:** This method is not yet implemented and will raise `NotImplementedError`.

#### pause_task()

Pause task with specified action type.

```python
async def pause_task(self, pause_type: PauseActionTypes = PauseActionTypes.SUSPEND)
```

**Parameters:**
- `pause_type`: Either `SUSPEND` or `INTERRUPT`
- 

## Helper Functions

### mageflow.load_signature()

Load stored signature by ID from redis.

```python
async def load_signature(task_id: TaskIdentifierType) -> Optional[TaskSignature]
```

### mageflow.resume_task() / mageflow.resume()

```python
async def resume_task(task_id: TaskIdentifierType)
async def resume(task_id: TaskIdentifierType)  # Same as resume_task
```

### mageflow.lock_task()

```python
def lock_task(task_id: TaskIdentifierType, **kwargs)
```

Create a lock of the task signature, the signature will not be deleted nor change status while locked.
!!! warning
    This function can be dangerous as signatures wont be able to be deleted or change status while locked. It may prevent task from finishing, causing timeout.
