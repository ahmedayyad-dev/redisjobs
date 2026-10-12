"""Cancellation of queued, scheduled and running jobs."""

from __future__ import annotations

import asyncio

from redisjobs import JobStatus, Queue, Worker


async def test_cancel_unknown_job_returns_false(queue: Queue) -> None:
    assert await queue.cancel("00000000-0000-0000-0000-000000000000") is False


async def test_cancel_queued_job(queue: Queue) -> None:
    job = await queue.enqueue("t")
    assert await queue.cancel(job.id) is True
    assert (await queue.get_job(job.id)).status is JobStatus.CANCELLED
    assert await queue.redis.llen(queue.config.keys.ready("default", 5)) == 0


async def test_cancelled_queued_job_is_never_executed(queue: Queue) -> None:
    ran: list[int] = []

    async def never() -> None:
        ran.append(1)

    job = await queue.enqueue(never)
    assert await queue.cancel(job.id) is True

    worker = Worker(queue, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:
        await asyncio.sleep(0.3)
    finally:
        await worker.stop()
    assert ran == []
    assert (await queue.get_job(job.id)).status is JobStatus.CANCELLED


async def test_cancel_scheduled_job(queue: Queue) -> None:
    job = await queue.enqueue_in(60, "t")
    assert await queue.cancel(job.id) is True
    assert await queue.redis.zcard(queue.config.keys.delayed("default")) == 0
    assert (await queue.get_job(job.id)).status is JobStatus.CANCELLED


async def test_cancel_terminal_job_returns_false(queue: Queue) -> None:
    job = await queue.enqueue("t")
    await queue.cancel(job.id)
    assert await queue.cancel(job.id) is False


async def test_cancel_running_job_discards_result(queue: Queue, wait_for) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def long_running() -> str:
        started.set()
        await release.wait()
        return "discarded"

    job = await queue.enqueue(long_running)
    worker = Worker(queue, concurrency=1, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:
        assert await wait_for(started.is_set)

        async def active() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.ACTIVE

        assert await wait_for(active)
        assert await queue.cancel(job.id) is True
        release.set()

        async def cancelled() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.CANCELLED

        assert await wait_for(cancelled)
    finally:
        await worker.stop()

    result = await queue.get_job(job.id)
    assert result.status is JobStatus.CANCELLED
    assert result.result is None
