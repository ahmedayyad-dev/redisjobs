"""Tests for the Job model and (de)serialization."""

from __future__ import annotations

import time

import pytest

from redisjobs import ConstantBackoff, ExponentialBackoff, Job, JobStatus, Priority
from redisjobs.errors import SerializationError


def test_defaults_and_validation() -> None:
    job = Job(task="m:f", queue="default")
    assert job.status is JobStatus.QUEUED
    assert job.priority == 5
    assert job.attempt == 0
    assert job.retries_left == 3
    assert not job.is_terminal


def test_priority_in_constructor_normalized() -> None:
    job = Job(task="m:f", queue="q", priority=Priority.HIGH)
    assert job.priority == 0


def test_invalid_job_id_rejected() -> None:
    with pytest.raises(ValueError):
        Job(task="m:f", queue="q", id="not-a-uuid")


def test_mapping_roundtrip() -> None:
    job = Job(
        task="m:f",
        queue="emails",
        args=[1, "two", {"three": 3}],
        kwargs={"a": True, "b": None},
        priority=int(Priority.HIGH),
        max_retries=7,
        backoff=ConstantBackoff(delay=2.5),
        status=JobStatus.SCHEDULED,
        attempt=2,
        scheduled_at=time.time() + 10,
        timeout=30.0,
        last_error="boom",
        result={"ok": True},
        worker_id="w1",
    )
    restored = Job.from_mapping(job.to_mapping())
    assert restored.id == job.id
    assert restored.args == job.args
    assert restored.kwargs == job.kwargs
    assert restored.priority == job.priority
    assert restored.max_retries == job.max_retries
    assert isinstance(restored.backoff, ConstantBackoff)
    assert restored.backoff.delay == 2.5
    assert restored.status is JobStatus.SCHEDULED
    assert restored.attempt == 2
    assert restored.timeout == 30.0
    assert restored.last_error == "boom"
    assert restored.result == {"ok": True}
    assert restored.worker_id == "w1"


def test_backoff_roundtrip_exponential() -> None:
    job = Job(task="m:f", queue="q", backoff=ExponentialBackoff(base=3.0, factor=0.5, jitter=False))
    restored = Job.from_mapping(job.to_mapping())
    assert isinstance(restored.backoff, ExponentialBackoff)
    assert restored.backoff.base == 3.0
    assert restored.backoff.factor == 0.5
    assert restored.backoff.jitter is False


def test_unserializable_args_rejected() -> None:
    job = Job(task="m:f", queue="q", args=[object()])
    with pytest.raises(SerializationError):
        job.to_mapping()


def test_terminal_statuses() -> None:
    for status in (JobStatus.COMPLETED, JobStatus.DEAD, JobStatus.CANCELLED):
        assert status.is_terminal
    for status in (JobStatus.QUEUED, JobStatus.ACTIVE, JobStatus.SCHEDULED, JobStatus.RETRYING):
        assert not status.is_terminal
