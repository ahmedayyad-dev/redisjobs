"""Tests for backoff strategies and retry policies."""

from __future__ import annotations

import random

import pytest

from redisjobs import ConstantBackoff, ExponentialBackoff, Priority, RetryPolicy
from redisjobs.errors import SerializationError
from redisjobs.priority import normalize_priority
from redisjobs.retry import backoff_from_spec, backoff_to_spec


def test_exponential_without_jitter() -> None:
    backoff = ExponentialBackoff(base=2.0, factor=1.0, max_delay=100.0, jitter=False)
    assert [backoff.delay_for(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_exponential_respects_max_delay() -> None:
    backoff = ExponentialBackoff(base=10.0, factor=1.0, max_delay=5.0, jitter=False)
    assert backoff.delay_for(1) == 1.0
    assert backoff.delay_for(2) == 5.0
    assert backoff.delay_for(3) == 5.0


def test_exponential_jitter_is_bounded_and_deterministic() -> None:
    rng = random.Random(1234)
    backoff = ExponentialBackoff(base=2.0, factor=10.0, jitter=True, rng=rng)
    delays = [backoff.delay_for(n) for n in range(1, 5)]
    assert all(0.0 <= d <= 10.0 * (2 ** (n)) for n, d in enumerate(delays, start=1))


def test_exponential_validation() -> None:
    with pytest.raises(ValueError):
        ExponentialBackoff(base=0.5)
    with pytest.raises(ValueError):
        ExponentialBackoff(max_delay=1.0, min_delay=2.0)
    with pytest.raises(ValueError):
        ExponentialBackoff().delay_for(0)


def test_constant_backoff() -> None:
    backoff = ConstantBackoff(delay=3.0)
    assert backoff.delay_for(1) == 3.0
    assert backoff.delay_for(10) == 3.0
    with pytest.raises(ValueError):
        ConstantBackoff(delay=-1.0)


def test_retry_policy_delegates_to_backoff() -> None:
    policy = RetryPolicy(max_retries=2, backoff=ConstantBackoff(delay=1.5))
    assert policy.max_retries == 2
    assert policy.delay_for(1) == 1.5
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


def test_backoff_spec_roundtrip() -> None:
    spec = backoff_to_spec(ExponentialBackoff(base=2.0, factor=1.0, jitter=False))
    restored = backoff_from_spec(spec)
    assert isinstance(restored, ExponentialBackoff)
    assert restored.base == 2.0


def test_backoff_spec_rejects_unknown() -> None:
    with pytest.raises(SerializationError):
        backoff_to_spec(object())
    with pytest.raises(SerializationError):
        backoff_from_spec({"type": "nope"})


@pytest.mark.parametrize(
    ("value", "expected"),
    [(Priority.HIGH, 0), (Priority.DEFAULT, 5), (Priority.LOW, 9), (3, 3), (42, 42)],
)
def test_normalize_priority(value: object, expected: int) -> None:
    assert normalize_priority(value) == expected


def test_normalize_priority_errors() -> None:
    with pytest.raises(ValueError):
        normalize_priority(-1)
    with pytest.raises(TypeError):
        normalize_priority(True)
    with pytest.raises(TypeError):
        normalize_priority("high")
