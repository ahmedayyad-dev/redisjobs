"""Job priority levels.

Lower numeric values are served first. Three named levels are provided but any
non-negative integer may be used as a custom priority.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["Priority", "normalize_priority"]


class Priority(IntEnum):
    """Well-known priority levels.

    A smaller value means the job is dequeued sooner. The numeric gap between
    levels leaves room for custom integer priorities in between (e.g. ``2``).
    """

    HIGH = 0
    DEFAULT = 5
    LOW = 9


def normalize_priority(priority: Priority | int) -> int:
    """Return the integer value of ``priority``, validating the input.

    Args:
        priority: A :class:`Priority` member or a plain non-negative integer.

    Returns:
        The integer priority value.

    Raises:
        TypeError: If ``priority`` is a bool or not an integer-like value.
        ValueError: If the resulting value is negative.
    """
    if isinstance(priority, bool):
        raise TypeError("priority must not be a bool")
    if isinstance(priority, Priority):
        return int(priority)
    if isinstance(priority, int):
        if priority < 0:
            raise ValueError("priority must be non-negative")
        return priority
    raise TypeError(f"priority must be an int, got {type(priority).__name__}")
