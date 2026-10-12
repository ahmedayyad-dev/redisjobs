"""Tests that higher-priority jobs are dequeued first."""

from __future__ import annotations

import asyncio

from redisjobs import JobStatus, Priority, Queue, Worker


async def test_ready_jobs_are_processed_in_priority_order(queue: Queue, wait_for) -> None:
    order: list[str] = []

    async def record(label: str) -> None:
        order.append(label)

    # Enqueue low first so ordering cannot be an accident of insertion.
    low = await queue.enqueue(record, "low", priority=Priority.LOW)
    mid = await queue.enqueue(record, "mid", priority=Priority.DEFAULT)
    high = await queue.enqueue(record, "high", priority=Priority.HIGH)

    worker = Worker(queue, concurrency=1, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:
        ids = [low.id, mid.id, high.id]

        async def all_done() -> bool:
            statuses = [await queue.get_job(i) for i in ids]
            return all(s.status is JobStatus.COMPLETED for s in statuses)

        assert await wait_for(all_done, timeout=5.0)
    finally:
        await worker.stop()

    assert order == ["high", "mid", "low"]


async def test_custom_integer_priority_ordering(queue: Queue, wait_for) -> None:
    order: list[str] = []

    async def record(label: str) -> None:
        await asyncio.sleep(0)
        order.append(label)

    await queue.enqueue(record, "five", priority=5)
    await queue.enqueue(record, "one", priority=1)

    worker = Worker(queue, concurrency=1, install_signal_handlers=False, blocking=False)
    await worker.start()
    try:

        async def all_done() -> bool:
            return len(order) == 2

        assert await wait_for(all_done, timeout=5.0)
    finally:
        await worker.stop()

    assert order == ["one", "five"]
