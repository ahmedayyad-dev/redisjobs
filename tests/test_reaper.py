"""Reaper: recovery of jobs leased by a crashed worker."""

from __future__ import annotations

import time

from redisjobs import JobStatus, Queue, Worker


async def test_reaper_redelivers_expired_lease(queue: Queue, wait_for) -> None:
    async def work() -> str:
        return "recovered"

    job = await queue.enqueue(work, max_retries=1)
    keys = queue.config.keys
    # The job is no longer ready: it was picked up by the (now dead) worker.
    await queue.redis.lrem(keys.ready("default", 5), 1, job.id)

    # Simulate a job picked up by a worker that then died: mark it active,
    # point it at a "ghost" worker and give it an expired lease.
    mapping = await queue.redis.hgetall(keys.job(job.id))
    mapping["status"] = JobStatus.ACTIVE.value
    mapping["worker_id"] = "ghost"
    await queue.redis.hset(keys.job(job.id), mapping=mapping)
    await queue.redis.zadd(keys.leases("default"), {job.id: time.time() - 1})
    await queue.redis.rpush(keys.processing("default", "ghost"), job.id)

    worker = Worker(queue, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:

        async def completed() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.COMPLETED

        assert await wait_for(completed, timeout=5.0)
    finally:
        await worker.stop()

    result = await queue.get_job(job.id)
    assert result.status is JobStatus.COMPLETED
    assert result.result == "recovered"
    assert result.attempt == 2
    assert await queue.redis.llen(keys.processing("default", "ghost")) == 0


async def test_reaper_dead_letters_when_retries_exhausted(queue: Queue, wait_for) -> None:
    async def work() -> None: ...

    job = await queue.enqueue(work, max_retries=0)
    keys = queue.config.keys
    await queue.redis.lrem(keys.ready("default", 5), 1, job.id)

    mapping = await queue.redis.hgetall(keys.job(job.id))
    mapping["status"] = JobStatus.ACTIVE.value
    mapping["worker_id"] = "ghost"
    mapping["attempt"] = "1"
    await queue.redis.hset(keys.job(job.id), mapping=mapping)
    await queue.redis.zadd(keys.leases("default"), {job.id: time.time() - 1})

    worker = Worker(queue, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:

        async def dead() -> bool:
            return (await queue.get_job(job.id)).status is JobStatus.DEAD

        assert await wait_for(dead, timeout=5.0)
    finally:
        await worker.stop()

    assert await queue.redis.llen(keys.dead("default")) == 1
