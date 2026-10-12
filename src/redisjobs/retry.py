"""Retry policy and backoff strategies."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .errors import SerializationError

__all__ = [
    "Backoff",
    "ConstantBackoff",
    "ExponentialBackoff",
    "RetryPolicy",
    "backoff_from_spec",
    "backoff_to_spec",
]


@runtime_checkable
class Backoff(Protocol):
    """Strategy computing the delay before the next retry."""

    def delay_for(self, attempt: int) -> float:
        """Return the delay in seconds before retry number ``attempt``.

        Args:
            attempt: 1-based retry counter (the first retry is ``1``).
        """
        ...


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """Exponential backoff, optionally with *full jitter*.

    The delay for retry ``n`` is ``min(max_delay, factor * base ** (n - 1))``.
    When ``jitter`` is enabled the returned delay is a uniform random value in
    ``[0, delay]`` (the "full jitter" strategy recommended by AWS), which avoids
    thundering-herd retries.

    Args:
        base: Growth factor (must be ``>= 1``).
        factor: Delay of the first retry, in seconds.
        max_delay: Upper bound for a single delay, in seconds.
        min_delay: Lower bound for a single delay, in seconds.
        jitter: Apply full jitter when ``True``.
        rng: Random source, injectable for deterministic tests.
    """

    base: float = 2.0
    factor: float = 1.0
    max_delay: float = 3600.0
    min_delay: float = 0.0
    jitter: bool = True
    rng: random.Random = field(default_factory=random.Random, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.base < 1:
            raise ValueError("base must be >= 1")
        if self.factor < 0:
            raise ValueError("factor must be >= 0")
        if self.min_delay < 0:
            raise ValueError("min_delay must be >= 0")
        if self.max_delay < self.min_delay:
            raise ValueError("max_delay must be >= min_delay")

    def delay_for(self, attempt: int) -> float:
        """Return the backoff delay for ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        delay = min(self.factor * (self.base ** (attempt - 1)), self.max_delay)
        delay = max(delay, self.min_delay)
        if self.jitter and delay > self.min_delay:
            delay = self.rng.uniform(self.min_delay, delay)
        return delay


@dataclass(frozen=True, slots=True)
class ConstantBackoff:
    """Fixed delay between retries (mainly useful for tests)."""

    delay: float = 1.0

    def __post_init__(self) -> None:
        if self.delay < 0:
            raise ValueError("delay must be >= 0")

    def delay_for(self, attempt: int) -> float:
        """Return the constant delay for ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        return self.delay


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Combine a maximum retry count with a :class:`Backoff` strategy.

    Args:
        max_retries: Number of *retries* after the initial attempt.
        backoff: Strategy used to compute the delay before each retry.
    """

    max_retries: int = 3
    backoff: Backoff = field(default_factory=ExponentialBackoff)

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")

    def delay_for(self, attempt: int) -> float:
        """Return the delay before retry ``attempt`` (1-based)."""
        return self.backoff.delay_for(attempt)


def backoff_to_spec(backoff: Backoff) -> dict[str, Any]:
    """Serialize a built-in backoff to a JSON-friendly mapping.

    Raises:
        SerializationError: If ``backoff`` is not one of the built-in strategies.
    """
    if isinstance(backoff, ExponentialBackoff):
        return {
            "type": "exponential",
            "base": backoff.base,
            "factor": backoff.factor,
            "max_delay": backoff.max_delay,
            "min_delay": backoff.min_delay,
            "jitter": backoff.jitter,
        }
    if isinstance(backoff, ConstantBackoff):
        return {"type": "constant", "delay": backoff.delay}
    raise SerializationError(
        f"cannot serialize backoff of type {type(backoff).__name__}; "
        "subclass ExponentialBackoff/ConstantBackoff or register a custom queue"
    )


def backoff_from_spec(spec: dict[str, Any]) -> Backoff:
    """Reconstruct a built-in backoff from :func:`backoff_to_spec` output."""
    kind = spec.get("type")
    if kind == "exponential":
        return ExponentialBackoff(
            base=float(spec.get("base", 2.0)),
            factor=float(spec.get("factor", 1.0)),
            max_delay=float(spec.get("max_delay", 3600.0)),
            min_delay=float(spec.get("min_delay", 0.0)),
            jitter=bool(spec.get("jitter", True)),
        )
    if kind == "constant":
        return ConstantBackoff(delay=float(spec.get("delay", 1.0)))
    raise SerializationError(f"unknown backoff spec type: {kind!r}")
