"""redisjobs — an asynchronous, Redis-backed job queue.

Quickstart::

    import asyncio
    from redisjobs import Queue, Worker, task

    @task
    async def add(a: int, b: int) -> int:
        return a + b

    async def main() -> None:
        queue = Queue.from_url("redis://localhost:6379/0")
        await queue.enqueue(add, 2, 3)
        worker = Worker(queue, concurrency=4)
        await worker.run()  # blocks; Ctrl-C to stop

    asyncio.run(main())
"""

from __future__ import annotations

from ._version import __version__
from .config import QueueConfig
from .errors import (
    ConfigurationError,
    DuplicateTaskError,
    JobCancelledError,
    JobNotFoundError,
    JobTimeoutError,
    QueueEmptyError,
    RedisJobsError,
    SerializationError,
    TaskNotRegisteredError,
)
from .job import Job, JobStatus
from .logging import bind_context, configure_logging
from .priority import Priority
from .queue import Queue, QueueStats
from .registry import TaskRegistry, task
from .retry import Backoff, ConstantBackoff, ExponentialBackoff, RetryPolicy
from .worker import Worker, get_current_job

__all__ = [
    "Backoff",
    "ConfigurationError",
    "ConstantBackoff",
    "DuplicateTaskError",
    "ExponentialBackoff",
    "Job",
    "JobCancelledError",
    "JobNotFoundError",
    "JobStatus",
    "JobTimeoutError",
    "Priority",
    "Queue",
    "QueueConfig",
    "QueueEmptyError",
    "QueueStats",
    "RedisJobsError",
    "RetryPolicy",
    "SerializationError",
    "TaskNotRegisteredError",
    "TaskRegistry",
    "Worker",
    "__version__",
    "bind_context",
    "configure_logging",
    "get_current_job",
    "task",
]
