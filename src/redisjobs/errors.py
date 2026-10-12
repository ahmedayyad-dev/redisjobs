"""Exception hierarchy for :mod:`redisjobs`."""

from __future__ import annotations

__all__ = [
    "ConfigurationError",
    "DuplicateTaskError",
    "JobCancelledError",
    "JobNotFoundError",
    "JobTimeoutError",
    "QueueEmptyError",
    "RedisJobsError",
    "SerializationError",
    "TaskNotRegisteredError",
]


class RedisJobsError(Exception):
    """Base class for every error raised by redisjobs."""


class ConfigurationError(RedisJobsError):
    """Raised when the queue/worker is configured incorrectly."""


class JobNotFoundError(RedisJobsError):
    """Raised when a job id cannot be found in Redis."""


class DuplicateTaskError(RedisJobsError):
    """Raised when two tasks are registered under the same name."""


class TaskNotRegisteredError(RedisJobsError):
    """Raised when a job references a task that the worker does not know about."""


class SerializationError(RedisJobsError):
    """Raised when a job payload cannot be serialized or deserialized."""


class JobCancelledError(RedisJobsError):
    """Raised inside a running task to signal cooperative cancellation."""


class JobTimeoutError(RedisJobsError):
    """Raised when a task exceeds its configured ``timeout``."""


class QueueEmptyError(RedisJobsError):
    """Raised when a blocking fetch times out with no job available."""
