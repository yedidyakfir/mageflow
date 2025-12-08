import functools
import inspect
from typing import Any, Optional, Callable, get_origin, Tuple, get_args

from pydantic import TypeAdapter
from pydantic_core import ValidationError
from rapyer.types.base import REDIS_DUMP_FLAG_NAME

from mageflow.utils.pythonic import flexible_call


def try_validate_json(validator: TypeAdapter, data: Any):
    if data is None:
        return None
    try:
        return validator.validate_json(data, context={REDIS_DUMP_FLAG_NAME: True})
    except ValidationError:
        return validator.validate_python(data)


def pydantic_validator(func: Callable):
    signature = inspect.signature(func)

    # Build input validators at once
    input_validators = [
        TypeAdapter(param.annotation)
        for param in signature.parameters.values()
        if param.annotation is not inspect.Signature.empty
    ]

    # Build output validator once (if annotated)
    output_annotation = signature.return_annotation
    output_validator = (
        TypeAdapter(output_annotation)
        if output_annotation is not inspect.Signature.empty
        else None
    )

    # If the output is a tuple type, prebuild per-item adapters once
    tuple_item_adapters: Optional[list[TypeAdapter]] = None
    if output_validator is not None:
        origin = get_origin(output_annotation)
        if origin in (tuple, Tuple):
            type_args = get_args(output_annotation)
            tuple_item_adapters = [TypeAdapter(a) for a in type_args]

    async def validate_data_wrapper(*args):
        # Validate positional args that have annotations
        validated_args = [
            try_validate_json(validator, arg)
            for validator, arg in zip(input_validators, args)
        ]

        # Validate kwargs where annotations exist
        results = await flexible_call(func, *validated_args)

        if output_validator is None:
            return results

        # If tuple return, dump each element individually using prebuilt adapters
        if tuple_item_adapters is not None and isinstance(results, tuple):
            return [
                ad.dump_json(v, context={REDIS_DUMP_FLAG_NAME: True}).decode("utf-8")
                for ad, v in zip(tuple_item_adapters, results)
            ]

        # Non-tuple return: dump whole value
        return output_validator.dump_json(
            results, context={REDIS_DUMP_FLAG_NAME: True}
        ).decode("utf-8")

    return functools.wraps(func)(validate_data_wrapper)
