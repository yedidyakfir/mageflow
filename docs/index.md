# Task Orchestrator

Task Orchestrator is a Python package that provides a powerful wrapper for task managers like Hatchet and Taskiq. It enables you to orchestrate complex workflows by running tasks with callbacks, chaining tasks together, and managing pools of tasks that can run in parallel.

## What is Task Orchestrator?

Task Orchestrator abstracts away the complexity of task management systems, providing a unified interface to:

- **Execute tasks with callbacks**: Run tasks with success and error callbacks for robust error handling
- **Chain tasks together**: Create sequential workflows where tasks depend on the completion of previous tasks
- **Manage task swarms**: Run multiple tasks in parallel with sophisticated coordination and monitoring
- **Handle task lifecycle**: Pause, resume, and monitor task execution with built-in state management

## Key Features

### 🔗 Task Chaining
Create sequential workflows where each task depends on the previous one's completion. Perfect for multi-step processes where order matters.

```python
import orchestrator

# Create a chain of tasks that run sequentially
task_order = [
    preprocess_data_task,
    analyze_data_task,
    generate_report_task
]
workflow = await orchestrator.chain(task_order, name="data-pipeline")
```

### 🐝 Task Swarms
Execute multiple tasks in parallel with intelligent coordination. Ideal for processing large datasets or performing independent operations simultaneously.

```python
import orchestrator

# Run multiple tasks in parallel
swarm_tasks = [
    process_user_data_task,
    send_notifications_task,
    update_cache_task
]
parallel_workflow = await orchestrator.swarm(swarm_tasks, task_name="user-processing")
```

### 📞 Callback System
Robust error handling and success callbacks ensure your workflows are resilient and responsive.

```python
from orchestrator import register_task, handle_task_callback

@register_task("my-task")
@handle_task_callback()
async def my_task(message):
    # Your task logic here
    return {"status": "completed"}
```

### 🎯 Task Signatures
Flexible task definition system with validation, state management, and lifecycle control.

```python
import orchestrator

# Create a task signature with validation
task_signature = await orchestrator.sign(
    task_name="process-order",
    task_identifiers={"order_id": "12345"},
    success_callbacks=[send_confirmation_task],
    error_callbacks=[handle_error_task]
)
```

## Core Components

### Task Management
- **Task Registration**: Register tasks with the orchestrator for centralized management
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

Task Orchestrator is perfect for:

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

To start using Task Orchestrator, you'll need to:

1. **Install** the package and its dependencies
2. **Set up** your task manager backend (e.g., Hatchet)
3. **Configure** Redis for state storage
4. **Define** your tasks and workflows
5. **Run** your orchestrated tasks

Ready to get started? Check out our [Installation Guide](installation.md) and [Setup Documentation](setup.md).