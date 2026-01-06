# For python 310
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

try:
    # Python 3.12+
    from typing import Unpack
except ImportError:
    # Older Python versions
    from typing_extensions import Unpack

__all__ = ["Self", "Unpack"]
