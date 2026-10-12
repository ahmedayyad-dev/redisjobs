"""Tests for the delayed-job scheduler."""

from __future__ import annotations

import asyncio

from redisjobs import JobStatus, Queue, Worker


async def test_delayed_job_runs_after_delay(queue: Queue, wait_for) -> None:
    ran: list[float] = []

    async def delayed() -> None:
        ran.append(asyncio.get_running_loop().time())

    job = await queue.enqueue_in(0.15, delayed)
    assert (await queue.get_job(job.id)).status is JobStatus.SCHEDULED

    worker = Worker(queue, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:

        async def done() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

        assert await wait_for(done, timeout=5.0)
    finally:
        await worker.stop()
    assert ran


async def test_promoted_job_leaves_delayed_set(queue: Queue, wait_for) -> None:
    async def noop() -> None: ...

    job = await queue.enqueue_in(0.05, noop)
    worker = Worker(queue, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:

        async def done() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

        assert await wait_for(done, timeout=5.0)
    finally:
        await worker.stop()
    assert await queue.redis.zcard(queue.config.keys.delayed("default")) == 0
