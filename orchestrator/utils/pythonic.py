import inspect
from typing import Any

from pydantic import BaseModel, create_model

ParamValidationType = dict[str, tuple[type, Any]]


def deep_merge(base: dict, updates: dict) -> dict:
    results = base.copy()
    for key, value in updates.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            results[key] = deep_merge(base[key], value)
        else:
            results[key] = value
    return results


def extract_validators(data: dict) -> ParamValidationType:
    return {key: (type(value), value) for key, value in data.items()}


def create_model_from_validators(validators: ParamValidationType) -> type[BaseModel]:
    return create_model("DynamicModel", **validators)


def create_dynamic_model(data: dict) -> BaseModel:
    validators = extract_validators(data)
    model_type = create_model_from_validators(validators)
    return model_type(**data)


async def flexible_call(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return func(*args, **kwargs)
