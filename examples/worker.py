"""Worker: consume jobs from the ``default`` queue.

Run this in its own terminal, then run ``examples/producer.py``.
Stop with Ctrl-C.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tasks import registry

from redisjobs import Queue, Worker, configure_logging


async def main() -> None:
    configure_logging("INFO", json_output=True)

    queue = Queue.from_url("redis://localhost:6379/0", registry=registry)
    worker = Worker(queue, queues=["default"], concurrency=4)
    print(f"worker {worker.worker_id} starting; press Ctrl-C to stop")
    try:
        await worker.run()
    finally:
        await queue.aclose()


if __name__ == "__main__":
    asyncio.run(main())
