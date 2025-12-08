import dataclasses
from typing import TypeVar, get_type_hints

from pydantic import BaseModel

PropType = TypeVar("PropType", bound=dataclasses.dataclass)


def get_marked_fields(
    model: type[BaseModel], mark_type: type[PropType]
) -> list[tuple[PropType, str]]:
    hints = get_type_hints(model, include_extras=True)
    marked = []
    for field_name, annotated_type in hints.items():
        if hasattr(annotated_type, "__metadata__"):  # Annotated stores extras here
            for meta in annotated_type.__metadata__:
                if isinstance(meta, mark_type):
                    marked.append((meta, field_name))
    return marked
