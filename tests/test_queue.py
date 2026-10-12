"""Tests for enqueue, scheduling, inspection and statistics."""

from __future__ import annotations

import time

import pytest

from redisjobs import JobStatus, Priority, Queue, task
from redisjobs.errors import JobNotFoundError
from redisjobs.registry import TaskRegistry


async def test_enqueue_callable_autoregisters(queue: Queue) -> None:
    async def my_task() -> None: ...

    job = await queue.enqueue(my_task)
    assert job.task in queue.registry
    assert job.queue == "default"
    assert job.status is JobStatus.QUEUED
    assert await queue.redis.llen(queue.config.keys.ready("default", 5)) == 1


async def test_enqueue_string_unknown_task_uses_defaults(queue: Queue) -> None:
    job = await queue.enqueue("external:task", 1, 2, key="value")
    assert job.task == "external:task"
    assert job.args == [1, 2]
    assert job.kwargs == {"key": "value"}
    assert job.queue == "default"


async def test_enqueue_registered_task_uses_task_defaults(redis_client, config) -> None:
    registry = TaskRegistry()

    @task(queue="emails", max_retries=9, registry=registry)
    async def send(): ...

    q = Queue(redis_client, config=config, registry=registry)
    job = await q.enqueue(send)
    assert job.queue == "emails"
    assert job.max_retries == 9


async def test_enqueue_overrides_task_defaults(redis_client, config) -> None:
    registry = TaskRegistry()

    @task(queue="emails", max_retries=9, registry=registry)
    async def send(): ...

    q = Queue(redis_client, config=config, registry=registry)
    job = await q.enqueue(send, queue="other", max_retries=1, priority=Priority.HIGH)
    assert job.queue == "other"
    assert job.max_retries == 1
    assert job.priority == 0


async def test_enqueue_explicit_job_id(queue: Queue) -> None:
    job_id = "12345678-1234-5678-1234-567812345678"
    job = await queue.enqueue("t", job_id=job_id)
    assert job.id == job_id
    assert (await queue.get_job(job_id)).id == job_id


async def test_enqueue_in_is_scheduled(queue: Queue) -> None:
    job = await queue.enqueue_in(60, "t")
    assert job.status is JobStatus.SCHEDULED
    assert job.scheduled_at is not None
    assert job.scheduled_at > time.time()
    assert await queue.redis.zcard(queue.config.keys.delayed("default")) == 1


async def test_enqueue_at_absolute(queue: Queue) -> None:
    when = time.time() + 120
    job = await queue.enqueue_at(when, "t")
    assert job.scheduled_at == pytest.approx(when)


async def test_enqueue_delay_and_at_mutually_exclusive(queue: Queue) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        await queue.enqueue("t", delay=1, at=time.time())


async def test_get_job_missing(queue: Queue) -> None:
    with pytest.raises(JobNotFoundError):
        await queue.get_job("00000000-0000-0000-0000-000000000000")


async def test_priorities_tracked(queue: Queue) -> None:
    await queue.enqueue("t", priority=Priority.HIGH)
    await queue.enqueue("t", priority=Priority.LOW)
    assert await queue.redis.smembers(queue.config.keys.priorities("default")) == {"0", "9"}


async def test_stats(queue: Queue) -> None:
    await queue.enqueue("t", priority=Priority.HIGH)
    await queue.enqueue("t", priority=Priority.HIGH)
    await queue.enqueue("t", priority=Priority.LOW)
    await queue.enqueue_in(60, "t")

    stats = await queue.stats("default")
    assert stats.total_ready == 3
    assert stats.ready[0] == 2
    assert stats.ready[9] == 1
    assert stats.delayed == 1
    assert stats.active == 0
    assert stats.dead == 0
    assert stats.as_dict()["total_ready"] == 3


async def test_queue_context_manager(redis_client, config) -> None:
    async with Queue(redis_client, config=config) as q:
        assert isinstance(q, Queue)
