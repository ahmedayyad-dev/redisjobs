"""Producer: enqueue a few jobs.

Run a worker in another terminal:

    python examples/worker.py

then run this script.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tasks import add, fetch_url, registry, send_email

from redisjobs import Priority, Queue


async def main() -> None:
    queue = Queue.from_url("redis://localhost:6379/0", registry=registry)

    # Normal, high-priority job.
    await queue.enqueue(add, 2, 3, priority=Priority.HIGH)
    # Delayed job, runs in 10 seconds.
    await queue.enqueue_in(10, send_email, to="a@example.com", subject="hi")
    # Job with retries and a per-attempt timeout.
    await queue.enqueue(fetch_url, "https://example.com", max_retries=5, timeout=30.0)

    print("enqueued 3 jobs on 'default'")
    await queue.aclose()


if __name__ == "__main__":
    asyncio.run(main())
