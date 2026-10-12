"""The :class:`Worker` — reliable job execution.

A worker runs a pool of concurrent *executors* fed by one *fetcher* per queue.
Fetchers block on the highest-priority ready list of their queue and hand jobs
to executors through an in-process queue, which keeps blocking Redis calls out
of the execution path and makes graceful shutdown trivial.

Reliability features:

* **At-least-once delivery** — jobs are moved to a per-worker *processing* list
  with the atomic ``LMOVE``/``BLMOVE`` commands and acknowledged afterwards.
* **Lease + reaper** — every in-flight job records a lease that the worker
  refreshes via heartbeats. A background reaper re-schedules jobs whose lease
  expired (i.e. whose worker died).
* **Retries with backoff** and a **dead-letter queue** for exhausted jobs.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import signal
import socket
import time
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from typing import Any, cast
from uuid import uuid4

from .config import QueueConfig
from .errors import JobTimeoutError, SerializationError, TaskNotRegisteredError
from .job import Job, JobStatus
from .logging import EventLogger, bind_context
from .priority import Priority
from .queue import Queue
from .registry import TaskRegistry

__all__ = ["Worker", "get_current_job"]

_CURRENT_JOB: ContextVar[Job | None] = ContextVar("redisjobs_current_job", default=None)
_SENTINEL: object = object()


def get_current_job() -> Job | None:
    """Return the job currently being executed by this coroutine, if any."""
    return _CURRENT_JOB.get()


def _detect_blocking(redis: object) -> bool:
    """Return whether blocking Redis commands are safe to use.

    The in-memory ``fakeredis`` server cannot reliably interrupt blocking
    commands, so workers fall back to short polling when it is detected.
    """
    module = type(redis).__module__
    return "fakeredis" not in module


class Worker:
    """Consume and execute jobs from one or more queues.

    Args:
        queue: The :class:`~redisjobs.queue.Queue` to consume from.
        queues: Queue names to consume; defaults to the config default queue.
        concurrency: Maximum number of jobs executed simultaneously.
        config: Optional config override (defaults to ``queue.config``).
        registry: Optional task registry override.
        logger: Optional logger.
        install_signal_handlers: Trap ``SIGINT``/``SIGTERM`` to stop gracefully.
    """

    def __init__(
        self,
        queue: Queue,
        *,
        queues: Sequence[str] | None = None,
        concurrency: int = 10,
        config: QueueConfig | None = None,
        registry: TaskRegistry | None = None,
        logger: logging.Logger | None = None,
        install_signal_handlers: bool = True,
        blocking: bool | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.queue = queue
        self.config = config or queue.config
        self.registry = registry if registry is not None else queue.registry
        self.queues: tuple[str, ...] = tuple(queues) if queues else (self.config.default_queue,)
        self.concurrency = concurrency
        self._install_signals = install_signal_handlers
        self.blocking = _detect_blocking(queue.redis) if blocking is None else blocking
        self._idle_sleep = min(self.config.poll_timeout, 0.25)
        self._log = EventLogger(logger or logging.getLogger("redisjobs"))

        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        self._internal: asyncio.Queue[Any] = asyncio.Queue(maxsize=concurrency * 2)
        self._stop = asyncio.Event()
        self._fetchers: list[asyncio.Task[None]] = []
        self._executors: list[asyncio.Task[None]] = []
        self._background: list[asyncio.Task[None]] = []
        self._active: set[str] = set()
        self._running = False

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Start fetchers, executors and background maintenance tasks."""
        if self._running:
            return
        self._running = True
        self._stop.clear()
        keys = self.config.keys
        await self.queue.redis.sadd(keys.workers(), self.worker_id)
        await self.queue._hset(
            keys.worker_info(self.worker_id),
            {
                "id": self.worker_id,
                "queues": ",".join(self.queues),
                "concurrency": str(self.concurrency),
                "started_at": repr(time.time()),
                "heartbeat": repr(time.time()),
            },
        )
        self._log.info(
            "worker_started",
            worker_id=self.worker_id,
            queues=list(self.queues),
            concurrency=self.concurrency,
        )
        self._executors = [
            asyncio.create_task(self._executor_loop(i), name=f"redisjobs-executor-{i}")
            for i in range(self.concurrency)
        ]
        self._fetchers = [
            asyncio.create_task(self._fetcher_loop(q), name=f"redisjobs-fetcher-{q}")
            for q in self.queues
        ]
        self._background = [
            asyncio.create_task(self._heartbeat_loop(), name="redisjobs-heartbeat"),
            asyncio.create_task(self._scheduler_loop(), name="redisjobs-scheduler"),
            asyncio.create_task(self._reaper_loop(), name="redisjobs-reaper"),
        ]
        if self._install_signals:
            self._install_signal_handlers()

    async def run(self) -> None:
        """Start the worker and block until :meth:`stop` is called."""
        await self.start()
        try:
            await self._stop.wait()
        finally:
            await self.stop()

    async def stop(self, *, timeout: float = 30.0) -> None:
        """Stop the worker gracefully.

        Fetchers are cancelled (interrupting any blocking Redis call) and
        executors are given up to ``timeout`` seconds to finish in-flight jobs.

        Args:
            timeout: Seconds to wait for in-flight jobs before cancelling them.
        """
        if not self._running:
            return
        self._running = False
        self._stop.set()

        for task in [*self._fetchers, *self._background]:
            task.cancel()
        for _ in self._executors:
            self._internal.put_nowait(_SENTINEL)

        if self._executors:
            done, pending = await asyncio.wait(self._executors, timeout=timeout)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._executors, return_exceptions=True)
            del done
        await asyncio.gather(*self._fetchers, *self._background, return_exceptions=True)

        keys = self.config.keys
        await self.queue.redis.srem(keys.workers(), self.worker_id)
        await self.queue.redis.delete(keys.worker_info(self.worker_id))
        self._log.info("worker_stopped", worker_id=self.worker_id)

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # -- fetchers & executors --------------------------------------------

    async def _fetcher_loop(self, queue: str) -> None:
        keys = self.config.keys
        processing = keys.processing(queue, self.worker_id)
        try:
            while not self._stop.is_set():
                priorities = await self._priority_order(queue)
                ready_keys = [keys.ready(queue, p) for p in priorities]
                job = await self._fetch_one(ready_keys, processing)
                if job is None:
                    if not self.blocking:
                        await asyncio.sleep(self._idle_sleep)
                    continue
                await self._internal.put(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            self._log.error("fetcher_error", str(exc), queue=queue, exc_info=True)

    async def _fetch_one(self, ready_keys: list[str], processing: str) -> Job | None:
        redis = self.queue.redis
        for src in ready_keys:
            job_id = await redis.lmove(src, processing, "LEFT", "RIGHT")
            if job_id:
                job = await self._load(str(job_id))
                if job is not None:
                    return job
                await redis.lrem(processing, 1, str(job_id))
        if not self.blocking or not ready_keys:
            return None
        block_seconds = max(1, int(self.config.poll_timeout))
        job_id = await redis.blmove(
            ready_keys[0], processing, timeout=block_seconds, src="LEFT", dest="RIGHT"
        )
        if not job_id:
            return None
        job = await self._load(str(job_id))
        if job is None:
            await redis.lrem(processing, 1, str(job_id))
        return job

    async def _priority_order(self, queue: str) -> list[int]:
        members = await self.queue.redis.smembers(self.config.keys.priorities(queue))
        priorities = sorted(int(m) for m in members)
        if not priorities:
            priorities = [int(Priority.HIGH), int(Priority.DEFAULT), int(Priority.LOW)]
        return priorities

    async def _executor_loop(self, index: int) -> None:
        while True:
            item = await self._internal.get()
            if item is _SENTINEL:
                self._internal.task_done()
                return
            try:
                await self._execute(item)
            finally:
                self._internal.task_done()

    # -- execution --------------------------------------------------------

    async def _execute(self, job: Job) -> None:
        started = time.perf_counter()
        with bind_context(job_id=job.id, task=job.task, queue=job.queue):
            token = _CURRENT_JOB.set(job)
            keys = self.config.keys
            processing = keys.processing(job.queue, self.worker_id)
            try:
                if await self._is_cancelled(job):
                    await self._finalize_cancelled(job, processing)
                    return
                if not await self._mark_active(job):
                    await self.queue.redis.lrem(processing, 1, job.id)
                    return

                try:
                    spec = self.registry.get(job.task)
                except TaskNotRegisteredError as exc:
                    await self._finalize_dead(job, processing, f"{type(exc).__name__}: {exc}")
                    return

                self._log.info("job_started", attempt=job.attempt, max_retries=job.max_retries)
                try:
                    result = await self._run_task(spec.func, job)
                except asyncio.CancelledError:
                    raise
                except JobTimeoutError as exc:
                    await self._on_failure(job, processing, str(exc), timed_out=True)
                    return
                except Exception as exc:
                    await self._on_failure(job, processing, f"{type(exc).__name__}: {exc}")
                    return

                if await self._is_cancelled(job):
                    await self._finalize_cancelled(job, processing)
                    return
                await self._finalize_success(job, processing, result, started)
            finally:
                _CURRENT_JOB.reset(token)
                self._active.discard(job.id)

    async def _run_task(self, func: Callable[..., Any], job: Job) -> Any:
        async def invoke() -> Any:
            if inspect.iscoroutinefunction(func):
                return await func(*job.args, **job.kwargs)
            return await asyncio.to_thread(func, *job.args, **job.kwargs)

        if job.timeout is not None:
            try:
                return await asyncio.wait_for(invoke(), timeout=job.timeout)
            except TimeoutError as exc:
                raise JobTimeoutError(
                    f"task {job.task!r} exceeded timeout of {job.timeout}s"
                ) from exc
        return await invoke()

    # -- state transitions ------------------------------------------------

    async def _is_cancelled(self, job: Job) -> bool:
        return bool(await self.queue.redis.sismember(self.config.keys.cancelled(job.queue), job.id))

    async def _mark_active(self, job: Job) -> bool:
        keys = self.config.keys
        mapping = await self.queue.redis.hgetall(keys.job(job.id))
        if not mapping:
            return False
        job.attempt += 1
        job.status = JobStatus.ACTIVE
        job.worker_id = self.worker_id
        job.started_at = time.time()
        await self.queue._hset(keys.job(job.id), job.to_mapping())
        await self.queue.redis.zadd(
            keys.leases(job.queue), {job.id: time.time() + self.config.lease}
        )
        self._active.add(job.id)
        return True

    async def _finalize_success(
        self, job: Job, processing: str, result: Any, started: float
    ) -> None:
        keys = self.config.keys
        job.status = JobStatus.COMPLETED
        job.finished_at = time.time()
        job.last_error = None
        job.worker_id = None
        try:
            job.result = result
            mapping = job.to_mapping()
        except SerializationError:
            job.result = None
            mapping = job.to_mapping()
            self._log.warning("result_not_serializable", task=job.task)
        await self.queue._hset(keys.job(job.id), mapping)
        await self._cleanup(job, processing)
        if self.config.result_ttl is not None:
            await self.queue.redis.expire(keys.job(job.id), self.config.result_ttl)
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        self._log.info("job_succeeded", duration_ms=duration_ms, attempt=job.attempt)

    async def _on_failure(
        self, job: Job, processing: str, error: str, *, timed_out: bool = False
    ) -> None:
        job.last_error = error
        if not timed_out and job.attempt <= job.max_retries:
            delay = job.backoff.delay_for(job.attempt)
            job.status = JobStatus.RETRYING
            job.scheduled_at = time.time() + delay
            job.worker_id = None
            keys = self.config.keys
            await self.queue._hset(keys.job(job.id), job.to_mapping())
            await self.queue.redis.zadd(keys.delayed(job.queue), {job.id: job.scheduled_at})
            await self._cleanup(job, processing)
            self._log.warning(
                "job_retry_scheduled",
                attempt=job.attempt,
                max_retries=job.max_retries,
                delay_s=round(delay, 3),
                error=error,
            )
            return
        await self._finalize_dead(job, processing, error)

    async def _finalize_dead(self, job: Job, processing: str, error: str) -> None:
        keys = self.config.keys
        job.status = JobStatus.DEAD
        job.finished_at = time.time()
        job.last_error = error
        job.worker_id = None
        await self.queue._hset(keys.job(job.id), job.to_mapping())
        await self.queue.redis.rpush(keys.dead(job.queue), job.id)
        if self.config.dead_ttl is not None:
            await self.queue.redis.expire(keys.dead(job.queue), self.config.dead_ttl)
        await self._cleanup(job, processing)
        if self.config.result_ttl is not None:
            await self.queue.redis.expire(keys.job(job.id), self.config.result_ttl)
        self._log.error("job_dead_lettered", attempt=job.attempt, error=error)

    async def _finalize_cancelled(self, job: Job, processing: str) -> None:
        keys = self.config.keys
        job.status = JobStatus.CANCELLED
        job.finished_at = time.time()
        job.worker_id = None
        await self.queue._hset(keys.job(job.id), job.to_mapping())
        await self._cleanup(job, processing)
        if self.config.result_ttl is not None:
            await self.queue.redis.expire(keys.job(job.id), self.config.result_ttl)
        self._log.info("job_cancelled", job_id=job.id)

    async def _cleanup(self, job: Job, processing: str) -> None:
        keys = self.config.keys
        await self.queue.redis.lrem(processing, 1, job.id)
        await self.queue.redis.zrem(keys.leases(job.queue), job.id)

    async def _load(self, job_id: str) -> Job | None:
        mapping = await self.queue.redis.hgetall(self.config.keys.job(job_id))
        if not mapping:
            return None
        return Job.from_mapping(cast("Any", mapping))

    # -- background loops -------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.config.heartbeat_interval)
                now = time.time()
                keys = self.config.keys
                for job_id in list(self._active):
                    job = await self._load(job_id)
                    if job is None:
                        continue
                    await self.queue.redis.zadd(
                        keys.leases(job.queue), {job_id: now + self.config.lease}
                    )
                await self.queue._hset(keys.worker_info(self.worker_id), {"heartbeat": repr(now)})
        except asyncio.CancelledError:
            raise

    async def _scheduler_loop(self) -> None:
        try:
            while not self._stop.is_set():
                for queue in self.queues:
                    await self._promote_due(queue)
                await asyncio.sleep(self.config.scheduler_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            self._log.error("scheduler_error", str(exc), exc_info=True)

    async def _promote_due(self, queue: str) -> None:
        token = uuid4().hex
        ttl_ms = int(self.config.scheduler_interval * 2000) + 1000
        if not await self.queue._acquire_lock(queue, token, ttl_ms):
            return
        try:
            now = time.time()
            keys = self.config.keys
            due = await self.queue.redis.zrangebyscore(
                keys.delayed(queue), "-inf", now, start=0, num=100
            )
            for job_id in due:
                job = await self._load(str(job_id))
                if job is None:
                    await self.queue.redis.zrem(keys.delayed(queue), str(job_id))
                    continue
                result = await self.queue._requeue_due(queue, job.id, job.priority, now)
                if result == 1:
                    job.status = JobStatus.QUEUED
                    job.scheduled_at = None
                    job.worker_id = None
                    await self.queue._hset(keys.job(job.id), job.to_mapping())
                    self._log.info("job_promoted", job_id=job.id, queue=queue)
                elif result == -1:
                    self._log.info("job_cancelled", job_id=job.id, queue=queue)
        finally:
            await self.queue._release_lock(queue, token)

    async def _reaper_loop(self) -> None:
        try:
            while not self._stop.is_set():
                for queue in self.queues:
                    await self._reap(queue)
                await asyncio.sleep(self.config.reaper_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            self._log.error("reaper_error", str(exc), exc_info=True)

    async def _reap(self, queue: str) -> None:
        token = uuid4().hex
        ttl_ms = int(self.config.reaper_interval * 2000) + 1000
        if not await self.queue._acquire_lock(queue, token, ttl_ms):
            return
        try:
            keys = self.config.keys
            now = time.time()
            expired = await self.queue.redis.zrangebyscore(
                keys.leases(queue), "-inf", now, start=0, num=100
            )
            for job_id_raw in expired:
                job_id = str(job_id_raw)
                job = await self._load(job_id)
                await self.queue.redis.zrem(keys.leases(queue), job_id)
                if job is None or job.status.is_terminal:
                    continue
                if job.worker_id:
                    await self.queue.redis.lrem(keys.processing(queue, job.worker_id), 1, job_id)
                if await self._is_cancelled(job):
                    job.status = JobStatus.CANCELLED
                    job.finished_at = now
                    await self.queue._hset(keys.job(job.id), job.to_mapping())
                    self._log.info("job_cancelled", job_id=job.id, via="reaper")
                    continue
                self._log.warning("job_lease_expired", job_id=job.id, queue=queue)
                await self._redeliver(job)
        finally:
            await self.queue._release_lock(queue, token)

    async def _redeliver(self, job: Job) -> None:
        job.attempt += 1
        job.last_error = "lease expired (worker presumed dead)"
        keys = self.config.keys
        if job.attempt > job.max_retries:
            job.status = JobStatus.DEAD
            job.finished_at = time.time()
            await self.queue._hset(keys.job(job.id), job.to_mapping())
            await self.queue.redis.rpush(keys.dead(job.queue), job.id)
            self._log.error("job_dead_lettered", job_id=job.id, via="reaper")
            return
        delay = job.backoff.delay_for(job.attempt)
        job.status = JobStatus.RETRYING
        job.scheduled_at = time.time() + delay
        job.worker_id = None
        await self.queue._hset(keys.job(job.id), job.to_mapping())
        await self.queue.redis.zadd(keys.delayed(job.queue), {job.id: job.scheduled_at})

    # -- signals ----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        queues: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> Worker:
        """Build a worker for a Redis URL (convenience for the CLI)."""
        return cls(Queue.from_url(url), queues=queues, **kwargs)
