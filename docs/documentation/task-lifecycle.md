# Task Lifecycle Management

The Task Orchestrator provides comprehensive lifecycle management capabilities that allow you to control task execution at runtime. You can suspend, interrupt, and resume tasks based on your application's requirements.

## Overview

Task lifecycle management includes three primary operations:

- **Suspend**: Gracefully stops a task only if it hasn't started execution yet
- **Interrupt**: Aggressively stops a task regardless of its current status  
- **Resume**: Restarts a previously suspended or interrupted task

## Suspend

The `suspend` operation provides a graceful way to stop tasks that are still in a pending state.

### Behavior
- ✅ **Will stop**: Tasks that are queued but haven't started execution
- ❌ **Will not stop**: Tasks that are already running (chain and swarm will stop after they have started, however a single task will not)

### Usage
```python
# Suspend a task signature
await task_signature.suspend()

# Suspend a chain
await chain_workflow.suspend()

# Suspend a swarm
await swarm_workflow.suspend()
```

### Use Cases
- Temporarily pausing workflows during maintenance windows
- Stopping tasks when system resources are constrained
- Delaying execution based on external conditions

## Interrupt

The `interrupt` operation aggressively stops tasks regardless of their current execution status.

!!! warning "Aggressive Action Warning"
    Using `interrupt` is an aggressive action that forcefully stops task execution. We cannot guarantee that interrupted tasks can be properly resumed later, as the task state may be left in an inconsistent condition.

### Behavior
- ✅ **Will stop**: Tasks at any point in the task lifecycle, even after it started running
- ⚠️ **Recovery risk**: Resuming interrupted tasks may not work reliably

### Usage
```python
# Interrupt a task signature
await task_signature.interrupt()

# Interrupt a chain
await chain_workflow.interrupt()

# Interrupt a swarm
await swarm_workflow.interrupt()
```

### Use Cases
- Emergency shutdown scenarios
- Canceling long-running tasks that are no longer needed
- System recovery situations

## Resume

The `resume` operation restarts previously suspended or interrupted tasks.

### Behavior
- ✅ **Reliable for suspended tasks**: Tasks that were cleanly suspended
- ⚠️ **Uncertain for interrupted tasks**: Tasks that were forcefully interrupted may have inconsistent state

### Usage
```python
# Resume a task signature
await task_signature.resume()

# Resume a chain
await chain_workflow.resume()

# Resume a swarm
await swarm_workflow.resume()
```

### Use Cases
- Continuing workflows after maintenance is complete
- Restarting tasks when system resources become available
- Recovering from temporary system issues

## Best Practices

### Prefer Suspend Over Interrupt
Always try to use `suspend` first, as it provides a cleaner shutdown:

```python
# Good: Try suspend first
try:
    await workflow.suspend()
except TaskAlreadyRunningError:
    # Only use interrupt if suspend fails
    await workflow.interrupt()
```

## Status Transitions

Understanding how lifecycle operations affect task status:

```
pending → suspend() → suspended → resume() → pending
pending → interrupt() → interrupted → resume() → pending
active → interrupt() → interrupted  → resume() → active (may be inconsistent)
active → suspend() → suspended  → resume() → active (may be inconsistent)
```

## Examples

### Graceful Workflow Pause and Resume

```python
import mageflow
import asyncio


async def pausable_workflow():
    # Create a long-running workflow
    tasks = [long_task_1, long_task_2, long_task_3]
    workflow = await mageflow.chain(tasks, name="pausable-pipeline")

    # Start the workflow
    await workflow.start()

    # Pause after some time
    await asyncio.sleep(10)
    await workflow.suspend()
    print("Workflow paused")

    # Resume later
    await asyncio.sleep(30)
    await workflow.resume()
    print("Workflow resumed")
```
