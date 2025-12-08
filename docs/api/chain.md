# Chain API Reference

This page provides detailed API documentation for chain functionality in MageFlow.

## mageflow.chain()

Create a new task chain for sequential execution.

```python
async def chain(
    tasks: List[TaskSignatureConvertible],
    name: Optional[str] = None,
    error: Optional[TaskInputType] = None,
    success: Optional[TaskInputType] = None,
) -> ChainTaskSignature
```

**Parameters:**
- `tasks`: List of tasks to execute sequentially (minimum 2 tasks required)
- `name`: Optional name for the chain (defaults to first task's name)
- `error`: Task to execute when any task in the chain fails
- `success`: Task to execute when all tasks complete successfully

**Returns:** `ChainTaskSignature` - The chain task signature

**Raises:**
- `ValueError`: If fewer than 2 tasks are provided

## ChainTaskSignature

The main chain class that manages sequential task execution.

### Properties

- `tasks`: List of task IDs in the chain sequence
- `task_name`: Name of the chain (derived from first task if not specified)
- `success_callbacks`: Tasks executed when chain completes successfully
- `error_callbacks`: Tasks executed when any task fails

### Methods

#### suspend()

Suspend the entire chain and all its tasks.

```python
async def suspend()
```

Suspends all tasks in the chain and sets the chain status to `SUSPENDED`.

#### resume()

Resume the chain and all its tasks.

```python
async def resume()
```

Resumes all tasks in the chain and restores the previous status.

#### interrupt()

Interrupt the chain and all its tasks.

```python
async def interrupt()
```

Interrupts all tasks in the chain and sets the status to `INTERRUPTED`.
