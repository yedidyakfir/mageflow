# MageFlow

**Ma**ngae **G**raph **E**xecution Flow - This package's purpose is to help users of task managers (like hatchet/taskiq etc) to orchestrate their tasks in an easy way from a single point. This way, it is much easier to flow and change, rather than spreading the flow logic all over your projects.
MageFlow provides a unify interface across different task managers that is fully compatible with the task manager api to execute tasks in chain/parallel/conditional tasks that can be calculated in runtime.

## What is Mageflow?

Mageflow abstracts away the complexity of task management systems, providing a unified interface to:

- **Execute tasks with callbacks**: Run tasks with success and error callbacks for robust error handling
- **Chain tasks together**: Create sequential workflows where tasks depend on the completion of previous tasks
- **Manage task swarms**: Run multiple tasks in parallel with sophisticated coordination and monitoring
- **Handle task lifecycle**: Pause, resume, and monitor task execution with built-in state management

## Key Features

### 🔗 Task Chaining
Create sequential workflows where each task depends on the previous one's completion. Perfect for multi-step processes where order matters.

```python
import mageflow

# Create a chain of tasks that run sequentially
task_order = [
    preprocess_data_task,
    analyze_data_task,
    generate_report_task
]
workflow = await mageflow.chain(task_order, name="data-pipeline")
```

### 🐝 Task Swarms
Execute multiple tasks in parallel with intelligent coordination. Ideal for processing large datasets or performing independent operations simultaneously.

```python
import mageflow

# Run multiple tasks in parallel
swarm_tasks = [
    process_user_data_task,
    send_notifications_task,
    update_cache_task
]
parallel_workflow = await mageflow.swarm(swarm_tasks, task_name="user-processing")
```

### 📞 Callback System
Robust error handling and success callbacks ensure your workflows are resilient and responsive.

```python
from mageflow import register_task, handle_task_callback


@register_task("my-task")
@handle_task_callback()
async def my_task(message):
    # Your task logic here
    return {"status": "completed"}
```

### 🎯 Task Signatures
Flexible task definition system with validation, state management, and lifecycle control.

```python
import mageflow

# Create a task signature with validation
task_signature = await mageflow.sign(
    task_name="process-order",
    task_identifiers={"order_id": "12345"},
    success_callbacks=[send_confirmation_task],
    error_callbacks=[handle_error_task]
)
```

## Core Components

### Task Management
- **Task Registration**: Register tasks with mageflow for centralized management
- **Task Lifecycle**: Control task execution with pause, resume, and cancellation capabilities
- **Task Validation**: Built-in validation for task inputs and outputs using Pydantic models

### Workflow Orchestration
- **Sequential Execution**: Chain tasks together for step-by-step processing
- **Parallel Execution**: Run tasks simultaneously with swarm coordination
- **Conditional Logic**: Execute tasks based on the results of previous tasks

### State Management
- **Persistent State**: Tasks maintain state across executions using Redis backend
- **Status Tracking**: Monitor task progress with detailed status information
- **Recovery**: Resume interrupted workflows from their last known state

### Error Handling
- **Callback-based**: Define custom error handling logic for each task
- **Retry Logic**: Automatic retry mechanisms for failed tasks
- **Graceful Degradation**: Continue workflow execution even when individual tasks fail

## Use Cases

MageFlow is perfect for:

- **Data Processing Pipelines**: Sequential data transformation and analysis workflows
- **Microservice Coordination**: Orchestrating calls across multiple services
- **Batch Processing**: Parallel processing of large datasets
- **ETL Operations**: Extract, Transform, Load operations with error handling
- **User Onboarding**: Multi-step user registration and setup processes
- **Content Processing**: Image/video processing workflows with multiple stages

## Architecture

The package is built on top of proven task management systems and provides:

- **Backend Agnostic**: Currently supports Hatchet with plans for Taskiq and other backends
- **Redis Storage**: Persistent state management using Redis
- **Async-First**: Built for modern async Python applications
- **Type Safe**: Full type hints and Pydantic model validation
- **Production Ready**: Designed for high-throughput, reliable production use

## Getting Started

To start using MageFlow, you'll need to:

1. **Install** the package and its dependencies
2. **Set up** your task manager backend (e.g., Hatchet)
3. **Configure** Redis for state storage
4. **Define** your tasks and workflows
5. **Run** your tasks

Ready to get started? Check out our [Installation Guide](installation.md) and [Setup Documentation](setup.md).