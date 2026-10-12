"""The :class:`Queue` — the producer/administrative entry point.

A :class:`Queue` wraps an async Redis client and exposes the public API used by
*producers* (enqueue, schedule, cancel) and *administrators* (inspect, stats).
Workers use the lower-level helpers on the same object.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from .config import QueueConfig
from .errors import JobNotFoundError, TaskNotRegisteredError
from .job import Job, JobStatus, new_job_id
from .logging import LOGGER_NAME, EventLogger
from .priority import Priority, normalize_priority
from .registry import TaskRegistry, TaskSpec, default_registry
from .retry import Backoff, ExponentialBackoff

__all__ = ["Queue", "QueueStats"]

TaskRef = str | Callable[..., Any]

# Promotion atomically moves a due job from the delayed ZSET into the correct
# priority ready-list, honouring cancellation. Returns 1 promoted, 0 not-due,
# -1 cancelled.
_PROMOTE_LUA = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then return 0 end
if tonumber(score) > tonumber(ARGV[2]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then return -1 end
redis.call('RPUSH', KEYS[2], ARGV[1])
return 1
"""

# Cancel a scheduled/retrying job: flag it and drop it from the delayed set.
_CANCEL_SCHEDULED_LUA = """
redis.call('SADD', KEYS[2], ARGV[1])
return redis.call('ZREM', KEYS[1], ARGV[1])
"""

# Cancel a ready job: flag it and drop it from its ready list.
_CANCEL_QUEUED_LUA = """
redis.call('SADD', KEYS[2], ARGV[1])
return redis.call('LREM', KEYS[1], 1, ARGV[1])
"""

# Release a lock only if we still own it.
_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
"""


@dataclass(frozen=True, slots=True)
class QueueStats:
    """A point-in-time snapshot of a queue's contents.

    Args:
        queue: Queue name.
        ready: Mapping of priority level to number of ready jobs.
        delayed: Number of scheduled/retrying jobs.
        active: Number of in-flight (leased) jobs.
        dead: Number of jobs in the dead-letter queue.
    """

    queue: str
    ready: dict[int, int] = field(default_factory=dict)
    delayed: int = 0
    active: int = 0
    dead: int = 0

    @property
    def total_ready(self) -> int:
        """Total number of ready jobs across all priorities."""
        return sum(self.ready.values())

    def as_dict(self) -> dict[str, Any]:
        """Return a plain, JSON-friendly representation."""
        return {
            "queue": self.queue,
            "ready": dict(self.ready),
            "total_ready": self.total_ready,
            "delayed": self.delayed,
            "active": self.active,
            "dead": self.dead,
        }


class Queue:
    """An async job queue backed by Redis.

    Args:
        redis: A ``redis.asyncio.Redis`` client (ideally with
            ``decode_responses=True``).
        config: Queue configuration; defaults to :class:`QueueConfig`.
        registry: Task registry used to resolve callables to task names.
        logger: Optional logger; defaults to the ``redisjobs`` logger.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        config: QueueConfig | None = None,
        registry: TaskRegistry | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis = redis
        self.config = config or QueueConfig()
        self.registry = registry if registry is not None else default_registry
        self._log = EventLogger(logger or logging.getLogger(LOGGER_NAME))
        self._promote = self.redis.register_script(_PROMOTE_LUA)
        self._cancel_scheduled = self.redis.register_script(_CANCEL_SCHEDULED_LUA)
        self._cancel_queued = self.redis.register_script(_CANCEL_QUEUED_LUA)
        self._release_lock_script = self.redis.register_script(_RELEASE_LOCK_LUA)

    # -- construction -----------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: QueueConfig | None = None,
        registry: TaskRegistry | None = None,
        logger: logging.Logger | None = None,
        **redis_kwargs: Any,
    ) -> Queue:
        """Build a queue from a Redis URL (e.g. ``redis://localhost:6379/0``)."""
        redis_kwargs.setdefault("decode_responses", True)
        client: Redis = Redis.from_url(url, **redis_kwargs)
        return cls(client, config=config, registry=registry, logger=logger)

    async def aclose(self) -> None:
        """Close the underlying Redis connection."""
        close = getattr(self.redis, "aclose", None) or self.redis.close
        await close()

    async def __aenter__(self) -> Queue:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- enqueue ----------------------------------------------------------

    async def enqueue(
        self,
        task: TaskRef,
        *args: Any,
        queue: str | None = None,
        priority: Priority | int = Priority.DEFAULT,
        max_retries: int | None = None,
        backoff: Backoff | None = None,
        timeout: float | None = None,
        job_id: str | None = None,
        delay: float | None = None,
        at: datetime | float | None = None,
        **kwargs: Any,
    ) -> Job:
        """Create and enqueue a job, returning the stored :class:`Job`.

        Args:
            task: A registered callable or a task name.
            *args: Positional arguments passed to the task.
            queue: Target queue; defaults to the task's queue or the config
                default.
            priority: Lower values run sooner (see :class:`Priority`).
            max_retries: Retries after the initial attempt.
            backoff: Backoff strategy; defaults to the task's or exponential.
            timeout: Per-attempt execution timeout in seconds.
            job_id: Explicit job id (must be a UUID); generated when omitted.
            delay: Seconds to wait before the job becomes runnable.
            at: Absolute time (aware :class:`datetime` or POSIX timestamp) at
                which the job becomes runnable. Mutually exclusive with ``delay``.
            **kwargs: Keyword arguments passed to the task.

        Returns:
            The created :class:`Job`.

        Raises:
            ValueError: If both ``delay`` and ``at`` are provided.
        """
        spec = self._resolve_spec(task)
        name = spec.name if spec is not None else str(task)
        queue_name = queue or (spec.queue if spec and spec.queue else self.config.default_queue)
        resolved_retries = (
            max_retries if max_retries is not None else (spec.max_retries if spec else 3)
        )
        resolved_backoff = (
            backoff if backoff is not None else (spec.backoff if spec else ExponentialBackoff())
        )
        resolved_timeout = timeout if timeout is not None else (spec.timeout if spec else None)
        scheduled_at = self._resolve_when(delay, at)

        job = Job(
            task=name,
            queue=queue_name,
            id=job_id or new_job_id(),
            args=list(args),
            kwargs=dict(kwargs),
            priority=normalize_priority(priority),
            max_retries=resolved_retries,
            backoff=resolved_backoff,
            timeout=resolved_timeout,
            scheduled_at=scheduled_at,
            status=JobStatus.SCHEDULED if scheduled_at is not None else JobStatus.QUEUED,
        )
        await self._store(job)
        event = "job_scheduled" if scheduled_at is not None else "job_enqueued"
        self._log.info(
            event,
            task=job.task,
            job_id=job.id,
            queue=job.queue,
            priority=job.priority,
            max_retries=job.max_retries,
            scheduled_at=job.scheduled_at,
        )
        return job

    async def enqueue_at(
        self, when: datetime | float, task: TaskRef, *args: Any, **kwargs: Any
    ) -> Job:
        """Enqueue a job to run at an absolute time (see :meth:`enqueue`)."""
        return await self.enqueue(task, *args, at=when, **kwargs)

    async def enqueue_in(self, delay: float, task: TaskRef, *args: Any, **kwargs: Any) -> Job:
        """Enqueue a job to run after ``delay`` seconds (see :meth:`enqueue`)."""
        return await self.enqueue(task, *args, delay=delay, **kwargs)

    # -- introspection & administration ----------------------------------

    async def get_job(self, job_id: str) -> Job:
        """Return the job stored under ``job_id``.

        Raises:
            JobNotFoundError: If the job is unknown or its record expired.
        """
        mapping = await self.redis.hgetall(self.config.keys.job(job_id))
        if not mapping:
            raise JobNotFoundError(f"job {job_id!r} not found")
        return Job.from_mapping(cast("Any", mapping))

    async def cancel(self, job_id: str) -> bool:
        """Request cancellation of a job.

        Ready and scheduled jobs are removed immediately; active jobs are
        flagged and stop cooperatively (the worker discards their result). The
        job record is marked :attr:`JobStatus.CANCELLED`.

        Returns:
            ``True`` if the job existed and was cancellable, else ``False``.
        """
        keys = self.config.keys
        mapping = cast("dict[str, str]", await self.redis.hgetall(keys.job(job_id)))
        if not mapping:
            return False
        status = JobStatus(mapping["status"])
        if status.is_terminal:
            return False

        queue_name = mapping["queue"]
        cancelled_key = keys.cancelled(queue_name)
        if status in (JobStatus.QUEUED,):
            ready_key = keys.ready(queue_name, int(mapping["priority"]))
            await self._cancel_queued(keys=[ready_key, cancelled_key], args=[job_id])
        elif status in (JobStatus.SCHEDULED, JobStatus.RETRYING):
            await self._cancel_scheduled(
                keys=[keys.delayed(queue_name), cancelled_key], args=[job_id]
            )
        else:  # ACTIVE — cooperative
            await self.redis.sadd(cancelled_key, job_id)

        await self.redis.zrem(keys.leases(queue_name), job_id)
        await self._hset(
            keys.job(job_id),
            {"status": JobStatus.CANCELLED.value, "finished_at": repr(time.time())},
        )
        await self._expire_terminal(job_id)
        self._log.info("job_cancelled", job_id=job_id, queue=queue_name, previous=status.value)
        return True

    async def stats(self, queue: str | None = None) -> QueueStats:
        """Return a snapshot of ``queue`` (defaults to the config default)."""
        queue_name = queue or self.config.default_queue
        keys = self.config.keys
        ready: dict[int, int] = {}
        async for key in self.redis.scan_iter(match=keys.ready_pattern(queue_name)):
            priority = int(key.rsplit(":", 1)[1])
            ready[priority] = int(await self.redis.llen(key))
        return QueueStats(
            queue=queue_name,
            ready=ready,
            delayed=int(await self.redis.zcard(keys.delayed(queue_name))),
            active=int(await self.redis.zcard(keys.leases(queue_name))),
            dead=int(await self.redis.llen(keys.dead(queue_name))),
        )

    # -- internal helpers used by workers ---------------------------------

    async def _store(self, job: Job) -> None:
        keys = self.config.keys
        async with self._pipeline() as pipe:
            pipe.hset(keys.job(job.id), mapping=cast("Any", job.to_mapping()))
            pipe.sadd(keys.queues(), job.queue)
            pipe.sadd(keys.priorities(job.queue), job.priority)
            if job.status is JobStatus.SCHEDULED and job.scheduled_at is not None:
                pipe.zadd(keys.delayed(job.queue), {job.id: job.scheduled_at})
            else:
                pipe.rpush(keys.ready(job.queue, job.priority), job.id)
            await pipe.execute()

    async def _hset(self, key: str, mapping: dict[str, str]) -> None:
        """``HSET`` a mapping, working around redis-py's over-narrow stub types."""
        await self.redis.hset(key, mapping=cast("Any", mapping))

    async def _expire_terminal(self, job_id: str) -> None:
        ttl = self.config.result_ttl
        if ttl is not None:
            await self.redis.expire(self.config.keys.job(job_id), ttl)

    async def _requeue_due(self, queue: str, job_id: str, priority: int, now: float) -> int:
        """Atomically promote ``job_id`` if due. See the module-level Lua."""
        keys = self.config.keys
        result = await self._promote(
            keys=[keys.delayed(queue), keys.ready(queue, priority), keys.cancelled(queue)],
            args=[job_id, repr(now)],
        )
        return int(result)

    def _resolve_spec(self, task: TaskRef) -> TaskSpec | None:
        if callable(task):
            return self.registry.ensure(task)
        try:
            return self.registry.get(task)
        except TaskNotRegisteredError:
            return None

    @staticmethod
    def _resolve_when(delay: float | None, at: datetime | float | None) -> float | None:
        if delay is not None and at is not None:
            raise ValueError("delay and at are mutually exclusive")
        if delay is not None:
            if delay < 0:
                raise ValueError("delay must be non-negative")
            return time.time() + float(delay)
        if at is None:
            return None
        if isinstance(at, datetime):
            return at.timestamp()
        return float(at)

    def _pipeline(self) -> Pipeline:
        return self.redis.pipeline(transaction=True)

    async def _acquire_lock(self, queue: str, token: str, ttl_ms: int) -> bool:
        acquired = await self.redis.set(
            self.config.keys.schedule_locks(queue), token, nx=True, px=ttl_ms
        )
        return bool(acquired)

    async def _release_lock(self, queue: str, token: str) -> None:
        await self._release_lock_script(keys=[self.config.keys.schedule_locks(queue)], args=[token])

    def priorities_for(self, queue: str) -> Sequence[int]:
        """Return the default priority poll order for ``queue``."""
        return (int(Priority.HIGH), int(Priority.DEFAULT), int(Priority.LOW))
