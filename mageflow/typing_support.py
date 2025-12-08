# For python 310
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


__all__ = ["Self"]
