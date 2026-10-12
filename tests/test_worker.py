"""End-to-end worker behaviour: success, retries, DLQ, timeouts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from redisjobs import ConstantBackoff, JobStatus, Queue, Worker, get_current_job


async def _run_until(
    queue: Queue,
    wait_for: Callable[..., Awaitable[bool]],
    predicate: Callable[[], Awaitable[bool]],
    *,
    queues: list[str] | None = None,
    concurrency: int = 2,
    timeout: float = 5.0,
) -> Worker:
    worker = Worker(
        queue,
        queues=queues or ["default"],
        concurrency=concurrency,
        install_signal_handlers=False,
        blocking=False,
    )
    await worker.start()
    try:
        assert await wait_for(predicate, timeout=timeout), "timed out waiting for condition"
    finally:
        await worker.stop()
    return worker


async def test_successful_async_task(queue: Queue, wait_for) -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    job = await queue.enqueue(add, 2, 3)

    async def done() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

    await _run_until(queue, wait_for, done)
    result = await queue.get_job(job.id)
    assert result.result == 5
    assert result.attempt == 1
    assert result.worker_id is None


async def test_successful_sync_task(queue: Queue, wait_for) -> None:
    def multiply(a: int, b: int) -> int:
        return a * b

    job = await queue.enqueue(multiply, 6, 7)

    async def done() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

    await _run_until(queue, wait_for, done)
    assert (await queue.get_job(job.id)).result == 42


async def test_current_job_is_available(queue: Queue, wait_for) -> None:
    seen: dict[str, str] = {}

    async def introspect() -> None:
        job = get_current_job()
        assert job is not None
        seen["id"] = job.id

    job = await queue.enqueue(introspect)

    async def done() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

    await _run_until(queue, wait_for, done)
    assert seen["id"] == job.id


async def test_retry_then_success(queue: Queue, wait_for) -> None:
    attempts: list[int] = []

    async def flaky() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("transient")
        return "ok"

    job = await queue.enqueue(flaky, max_retries=3, backoff=ConstantBackoff(delay=0.01))

    async def done() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

    await _run_until(queue, wait_for, done)
    result = await queue.get_job(job.id)
    assert result.result == "ok"
    assert result.attempt == 3


async def test_dead_letter_after_exhausting_retries(queue: Queue, wait_for) -> None:
    attempts: list[int] = []

    async def always_fails() -> None:
        attempts.append(1)
        raise ValueError("nope")

    job = await queue.enqueue(always_fails, max_retries=1, backoff=ConstantBackoff(delay=0.01))

    async def dead() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.DEAD

    await _run_until(queue, wait_for, dead)
    result = await queue.get_job(job.id)
    assert result.attempt == 2
    assert result.last_error is not None and "nope" in result.last_error
    assert len(attempts) == 2
    assert await queue.redis.llen(queue.config.keys.dead("default")) == 1


async def test_unregistered_task_goes_straight_to_dlq(queue: Queue, wait_for) -> None:
    job = await queue.enqueue("module.does_not_exist")

    async def dead() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.DEAD

    await _run_until(queue, wait_for, dead)
    result = await queue.get_job(job.id)
    assert result.attempt == 1
    assert result.last_error is not None
    assert "TaskNotRegisteredError" in result.last_error


async def test_timeout_is_treated_as_failure(queue: Queue, wait_for) -> None:
    async def slow() -> None:
        await asyncio.sleep(5)

    job = await queue.enqueue(slow, timeout=0.05, max_retries=0)

    async def dead() -> bool:
        return (await queue.get_job(job.id)).status is JobStatus.DEAD

    await _run_until(queue, wait_for, dead, timeout=3.0)
    result = await queue.get_job(job.id)
    assert result.last_error is not None
    assert "timeout" in result.last_error.lower()


async def test_concurrency_processes_multiple_jobs(queue: Queue, wait_for) -> None:
    done_ids: list[str] = []

    async def record(job_id: str) -> None:
        await asyncio.sleep(0.05)
        done_ids.append(job_id)

    jobs = [await queue.enqueue(record, f"j{i}") for i in range(5)]

    async def all_done() -> bool:
        statuses = [await queue.get_job(j.id) for j in jobs]
        return all(s.status is JobStatus.COMPLETED for s in statuses)

    await _run_until(queue, wait_for, all_done, concurrency=5)
    assert len(set(done_ids)) == 5
