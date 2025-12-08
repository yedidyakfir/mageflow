from typing import Annotated

# Call With Callback

The callback system in Task Orchestrator allows you to create robust, event-driven workflows with automatic success and error handling. This documentation covers the `orchestrator.sign()` function, which is the foundation for creating task signatures with callbacks.

## Task Signatures

Task signatures define how a task should be executed, including its configuration, validation, and callback behavior. Think of them as blueprints that specify not just what task to run, but how to handle success and failure scenarios.

### Basic Task Signature

Create a basic task signature using `mageflow.sign()`:

```python
import mageflow

# Create a signature for a registered task
signature = await mageflow.sign("process-data")

# Create a signature from a task function
signature = await mageflow.sign(my_task_function)
```

!!! info "Alternative Client Usage"
    You can also create signatures using the orchestrator client instead of the global `orchestrator` module:

    ```python
    from mageflow import Mageflow

    # Create orchestrator client
    hatchet = Mageflow(hatchet, redis)

    # Use client to create signatures
    signature = await hatchet.sign("process-data")
    signature = await hatchet.sign(my_task_function)
    ```

## Attaching Data with kwargs

You can attach additional data to task signatures using the `kwargs` parameter. This data becomes available to the task when it executes.

### Basic kwargs Usage

```python
# Attach configuration data
task_signature = await orchestrator.sign(
    "send-notification", 
    template="welcome_email",
    priority="high",
    retry_count=3
)

# The task will receive these values merged with the input message
```

### Dynamic Data Attachment

You can also update kwargs after creating the signature:

```python
# Create signature
user_task = await orchestrator.sign("process-user-data")

# Update kwargs dynamically
await user_task.kwargs.aupdate(
    user_id="12345",
    preferences={"theme": "dark", "notifications": True},
    processing_mode="batch"
)
```


## Success and Error Callbacks

The power of task signatures lies in their ability to automatically trigger callbacks based on task outcomes.

### Setting Success Callbacks

Success callbacks are executed when a task completes successfully:

```python
# Create callback tasks
success_callback = await orchestrator.sign("send-success-email")
audit_callback = await orchestrator.sign("log-completion")

# Create main task with success callbacks
main_task = await orchestrator.sign(
    "process-order",
    success_callbacks=[success_callback, audit_callback]
)
```

When a success callback is called, we take the return value of the function and inject it into the parameter that is marked with ReturnValue.

```python
from pydantic import BaseModel
from mageflow.models.message import ReturnValue


class SuccessMessage(BaseModel):
    task_result: Annotated[Any, ReturnValue()]
    field_int: int
    ...


@hatchet.task(input_validator=SuccessMessage)
async def success_callback(msg: SuccessMessage):
    # msg.task_result contains the original task's return value
    result = msg.task_result
```

!!! info "ReturnValue Annotation"
    ReturnValue is an annotation that tells the task orchestrator that the return value of the function should be injected into the parameter marked with ReturnValue.
    ```python {title="Creating model with ReturnValue annotation"}
    from pydantic import BaseModel
    from orchestrator.models.message import ReturnValue
    
    class SuccessMessage(BaseModel):
        task_result: Annotated[Any, ReturnValue()]
        field_int: int
    ```

    When no field is marked with ReturnValue, the return value of the function will be sent to the field named results.
    ```python
    class SuccessMessage(BaseModel):
        results: str  # The return value of the function will be sent here
        field_int: int
    ```


### Setting Error Callbacks

Error callbacks are triggered when a task fails:

```python
# Create error handling tasks
error_logger = await orchestrator.sign("log-error")
notify_admin = await orchestrator.sign("alert-administrator")
retry_handler = await orchestrator.sign("schedule-retry")

# Create task with error callbacks
risky_task = await orchestrator.sign(
    "external-api-call",
    error_callbacks=[error_logger, notify_admin, retry_handler]
)
```

For error callbacks, the message will be the same message that was sent to the task itself, obviously you can create a new model with more parameters and bind them to the error callback

```python
from pydantic import BaseModel
from mageflow.models.message import ReturnValue


class ErrorMessage(OriginalMessage):
    additional_field1: int
    additional_field2: str
    ...


@hatchet.task(input_validator=ErrorMessage)
async def error_callback(msg: ErrorMessage):
    # msg.task_result contains the original task's return value
    result = msg.task_result


# Create error handling tasks
error_logger = await orchestrator.sign(error_callback, additional_field1=12345, additional_field2="test")
signature = await orchestrator.sign("task", error_callbacks=[error_logger])
```

## Advanced Signature Configuration

### Model Validation

Specify input validation for your task signatures:

```python
from mageflow.models.message import ContextMessage

# Create signature with specific input validation
validated_task = await orchestrator.sign(
    "validate-data",
    model_validators=ContextMessage
)
```

Usually you dont have to do this, as this is dont automatically. But you can override the usual model_validator with your own.