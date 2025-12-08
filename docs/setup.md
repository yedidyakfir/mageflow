# Setup

This guide walks you through setting up Task Orchestrator with your preferred task manager backend and configuring the necessary dependencies.

## Installation

Install Task Orchestrator with your preferred task manager backend using the appropriate extra:

### Hatchet Backend
```bash
pip install task-mageflow[hatchet]
```

## Configuration

Task Orchestrator requires configuration for both the task manager backend and Redis storage.

### Basic Setup with Hatchet

Here's how to set up Task Orchestrator with Hatchet:

```python
import asyncio
import redis
from dynaconf import Dynaconf
from hatchet_sdk import Hatchet, ClientConfig
from hatchet_sdk.config import HealthcheckConfig

import mageflow

# Configure Hatchet client
config_obj = ClientConfig(token="your-hatchet-token")

# Set up Redis client
redis_client = redis.asyncio.from_url(
    "redis-url",
    max_connections=1028,  # Use connection pool 
    decode_responses=True,  # Mandatory - for redis backend, see https://github.com/yedidyakfir/rapyer docs
)

# Initialize Hatchet
hatchet = Hatchet(debug=True, config=config_obj)

# Create the Orchestrator instance
hatchet = mageflow.Mageflow(hatchet, redis_client=redis_client)
```

For a smooth transition experience, we recommend calling the wrapped object with the original name, it has all the same functions and configurations.

## Creating and Registering Tasks

### Task Definition

Define your tasks using the orchestrator's task decorators:

```python
@hatchet.task(name="process-data", input_validator=YourModelA)
async def process_data(msg: YourModelA):
    # Your task logic here
    result = {"processed": msg.data}
    return result

@hatchet.task(name="send-notification", input_validator=YourModelB)
async def send_notification(msg: YourModelB):
    # Send notification logic
    print(f"Notification sent for: {msg.data}")
    return {"status": "sent"}
```

### Durable Tasks

For long-running or critical tasks, use durable tasks:

```python
@hatchet.durable_task(name="critical-process", input_validator=YourModelB)
async def critical_process(msg):
    await asyncio.sleep(5)  # Simulate long-running process
    return {"completed": True}
```

### Backpropagation
If you want the task to stay with the hatchet definition (with ctx parameter), you can use the param_config parameter

```python
# Create the Orchestrator instance
hatchet = orchestrator.Mageflow(hatchet, redis_client=redis_client, param_config=AcceptParams.ALL)


# Now define the task in the original hatchet definition

@hatchet.task(name="send-notification", input_validator=YourModelB)
async def send_notification(msg: YourModelB, ctx: Context):
    # Send notification logic
    print(f"Notification sent for: {msg.data}")
    return {"status": "sent"}
```

## Worker Setup

Create a worker to run your registered tasks:

```python
def main():
    # Define your workflows (tasks)
    workflows = [
        process_data,
        send_notification,
        critical_process,
    ]
    
    # Create and start the worker
    worker = hatchet.worker("my-worker", workflows=workflows)
    
    worker.start()

if __name__ == "__main__":
    main()
```

## Next Steps

With your setup complete, you're ready to:

1. **Define workflows** using [callbacks](documentation/callbacks.md)
2. **Chain tasks** together with [task chains](documentation/chain.md)
3. **Run parallel tasks** using [swarms](documentation/swarm.md)
4. **Monitor and debug** your workflows using the built-in tools
