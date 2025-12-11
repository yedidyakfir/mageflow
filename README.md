<div align="center">
  <img src="logo.png" alt="MageFlow Logo" width="200"/>

 [![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
  [![codecov](https://codecov.io/gh/yedidyakfir/mageflow/branch/main/graph/badge.svg)](https://codecov.io/gh/yedidyakfir/mageflow)
  [![PyPI version](https://badge.fury.io/py/mageflow.svg)](https://badge.fury.io/py/mageflow)
  [![Downloads](https://static.pepy.tech/badge/mageflow)](https://pepy.tech/project/mageflow)
  [![Documentation](https://img.shields.io/badge/docs-github.io-blue)](https://yedidyakfir.github.io/mageflow/)

  
  📚 **[Full Documentation](https://yedidyakfir.github.io/rapyer/)** | [Installation](https://yedidyakfir.github.io/rapyer/installation/) | [Examples](https://yedidyakfir.github.io/rapyer/examples/) | [API Reference](https://yedidyakfir.github.io/rapyer/api/)

</div>

# MageFlow

**Ma**nage **G**raph **E**xecution Flow - A unified interface for task orchestration across different task managers.

## Why MageFlow?

Instead of spreading workflow logic throughout your codebase, MageFlow centralizes task orchestration with a clean, unified API. Switch between task managers (Hatchet, Taskiq, etc.) without rewriting your orchestration code.

## Key Features

🔗 **Task Chaining** - Sequential workflows where tasks depend on previous completions  
🐝 **Task Swarms** - Parallel execution with intelligent coordination  
📞 **Callback System** - Robust success/error handling  
🎯 **Task Signatures** - Flexible task definition with validation  
⏯️ **Lifecycle Control** - Pause, resume, and monitor task execution  
💾 **Persistent State** - Redis-backed state management with recovery  

## Installation

```bash
pip install mageflow[hatchet]  # For Hatchet backend
```

## Quick Setup

```python
import asyncio
import redis
from hatchet_sdk import Hatchet, ClientConfig
import mageflow

# Configure backend and Redis
config = ClientConfig(token="your-hatchet-token")
redis_client = redis.asyncio.from_url("redis://localhost", decode_responses=True)
hatchet_client = Hatchet(config=config)

# Create MageFlow instance
mf = mageflow.Mageflow(hatchet_client, redis_client=redis_client)
```

## Example Usage

### Define Tasks

```python
from pydantic import BaseModel

class ProcessData(BaseModel):
    data: str

@mf.task(name="process-data", input_validator=ProcessData)
async def process_data(msg: ProcessData):
    return {"processed": msg.data}

@mf.task(name="send-notification") 
async def send_notification(msg):
    print(f"Notification sent: {msg}")
    return {"status": "sent"}
```

### Chain Tasks

```python
# Sequential execution
workflow = await mageflow.chain([
    process_data_task,
    send_notification_task
], name="data-pipeline")
```

### Parallel Swarms

```python
# Parallel execution
swarm = await mageflow.swarm([
    process_user_task,
    update_cache_task,
    send_email_task
], task_name="user-onboarding")
```

### Task Signatures with Callbacks

```python
task_signature = await mageflow.sign(
    task_name="process-order",
    task_identifiers={"order_id": "12345"},
    success_callbacks=[send_confirmation_task],
    error_callbacks=[handle_error_task]
)
```

## Use Cases

- **Data Pipelines** - ETL operations with error handling
- **Microservice Coordination** - Orchestrate distributed service calls  
- **Batch Processing** - Parallel processing of large datasets
- **User Workflows** - Multi-step onboarding and registration
- **Content Processing** - Media processing with multiple stages

## Documentation

- [Setup Guide](docs/setup.md)
- [API Reference](docs/api/)
- [Task Lifecycle](docs/documentation/task-lifecycle.md)
- [Callbacks](docs/documentation/callbacks.md)

## License

MIT