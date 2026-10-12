"""The :class:`Job` model and its Redis (de)serialization.

A job is stored as a flat Redis HASH: scalar values are stored directly and
structured values (arguments, backoff spec, result) are JSON-encoded strings.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import UUID

from .errors import SerializationError
from .priority import normalize_priority
from .retry import Backoff, ExponentialBackoff, backoff_from_spec, backoff_to_spec

__all__ = ["Job", "JobStatus"]


class JobStatus(StrEnum):
    """Lifecycle state of a job."""

    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RETRYING = "retrying"
    ACTIVE = "active"
    COMPLETED = "completed"
    DEAD = "dead"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Whether the job reached a final state."""
        return self in _TERMINAL


_TERMINAL = frozenset({JobStatus.COMPLETED, JobStatus.DEAD, JobStatus.CANCELLED})


def _encode(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError as exc:  # pragma: no cover - defensive
        raise SerializationError(f"value is not JSON serializable: {exc}") from exc


def _decode(value: str) -> Any:
    if value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise SerializationError(f"value is not valid JSON: {exc}") from exc


def new_job_id() -> str:
    """Return a fresh, unique job identifier."""
    return str(uuid.uuid4())


@dataclass(slots=True)
class Job:
    """A unit of work travelling through the queue.

    Args:
        task: Registered task name to invoke.
        queue: Logical queue the job belongs to.
        id: Unique identifier (generated when omitted).
        args: Positional arguments for the task.
        kwargs: Keyword arguments for the task.
        priority: Lower runs sooner (see :class:`~redisjobs.priority.Priority`).
        max_retries: Retries allowed after the initial attempt.
        backoff: Backoff strategy used between retries.
        status: Current lifecycle state.
        attempt: Number of attempts already performed.
        created_at: Unix timestamp when the job was enqueued.
        scheduled_at: Unix timestamp when the job is due (for delayed jobs).
        started_at: Unix timestamp of the last attempt start.
        finished_at: Unix timestamp of the terminal transition.
        last_error: Human-readable representation of the last failure.
        result: JSON-serializable task return value.
        worker_id: Identifier of the worker that last leased the job.
        timeout: Maximum execution time in seconds, if any.
    """

    task: str
    queue: str
    id: str = field(default_factory=new_job_id)
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 5
    max_retries: int = 3
    backoff: Backoff = field(default_factory=ExponentialBackoff)
    status: JobStatus = JobStatus.QUEUED
    attempt: int = 0
    created_at: float = field(default_factory=time.time)
    scheduled_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    last_error: str | None = None
    result: Any = None
    worker_id: str | None = None
    timeout: float | None = None

    def __post_init__(self) -> None:
        try:
            UUID(self.id)
        except ValueError as exc:
            raise ValueError(f"job id must be a UUID, got {self.id!r}") from exc
        self.priority = normalize_priority(self.priority)
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")

    @property
    def is_terminal(self) -> bool:
        """Whether the job reached a final state."""
        return self.status.is_terminal

    @property
    def retries_left(self) -> int:
        """Number of retries still available."""
        return max(0, self.max_retries - self.attempt)

    def to_mapping(self) -> dict[str, str]:
        """Serialize the job to a flat ``str -> str`` mapping for Redis."""
        return {
            "id": self.id,
            "task": self.task,
            "queue": self.queue,
            "args": _encode(self.args),
            "kwargs": _encode(self.kwargs),
            "priority": str(self.priority),
            "max_retries": str(self.max_retries),
            "backoff": _encode(backoff_to_spec(self.backoff)),
            "status": self.status.value,
            "attempt": str(self.attempt),
            "created_at": repr(self.created_at),
            "scheduled_at": "" if self.scheduled_at is None else repr(self.scheduled_at),
            "started_at": "" if self.started_at is None else repr(self.started_at),
            "finished_at": "" if self.finished_at is None else repr(self.finished_at),
            "last_error": self.last_error or "",
            "result": _encode(self.result),
            "worker_id": self.worker_id or "",
            "timeout": "" if self.timeout is None else repr(self.timeout),
        }

    @classmethod
    def from_mapping(cls, mapping: dict[str, str]) -> Job:
        """Deserialize a job from a Redis HASH mapping."""
        args_value = mapping.get("args", "[]")
        kwargs_value = mapping.get("kwargs", "{}")
        return cls(
            id=mapping["id"],
            task=mapping["task"],
            queue=mapping["queue"],
            args=_decode(args_value) or [],
            kwargs=_decode(kwargs_value) or {},
            priority=int(mapping.get("priority", "5")),
            max_retries=int(mapping.get("max_retries", "3")),
            backoff=backoff_from_spec(_decode(mapping.get("backoff", "")) or {}),
            status=JobStatus(mapping.get("status", JobStatus.QUEUED.value)),
            attempt=int(mapping.get("attempt", "0")),
            created_at=float(mapping.get("created_at", "0") or 0),
            scheduled_at=_opt_float(mapping.get("scheduled_at")),
            started_at=_opt_float(mapping.get("started_at")),
            finished_at=_opt_float(mapping.get("finished_at")),
            last_error=mapping.get("last_error") or None,
            result=_decode(mapping.get("result", "")),
            worker_id=mapping.get("worker_id") or None,
            timeout=_opt_float(mapping.get("timeout")),
        )


def _opt_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
