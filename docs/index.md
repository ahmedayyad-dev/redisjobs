# redisjobs

**An asynchronous, Redis-backed job queue for Python** — small, fully typed and
dependency-light (`redis` is the only runtime dependency).

## Why redisjobs?

- **Async first.** Built directly on `redis.asyncio`; no threads and no blocking
  calls in the hot path.
- **Reliable.** Jobs are leased with atomic `LMOVE`/`BLMOVE` commands and a
  background *reaper* recovers jobs whose worker died (at-least-once delivery).
- **Feature-complete enough to be useful.** Priorities, delayed/scheduled jobs,
  retries with exponential backoff, a dead-letter queue, cancellation and
  structured logging — all included.
- **Testable.** The whole test-suite runs against
  [`fakeredis`](https://github.com/cunla/fakeredis-py); no Redis required in CI.

## Installation

```bash
pip install redisjobs
```

Requirements:

- Python **3.12+**
- Redis **6.2+** (for `BLMOVE`); 7.0+ recommended.

## 30-second tour

```python
import asyncio
from redisjobs import Queue, Worker, task


@task(queue="math", max_retries=3)
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    queue = Queue.from_url("redis://localhost:6379/0")
    await queue.enqueue(add, 2, 3)

    worker = Worker(queue, queues=["math"], concurrency=4)
    await worker.run()


asyncio.run(main())
```

In production you would run the worker in a separate process (or use the CLI:
`redisjobs worker -q math --app myapp.tasks`).

Navigate to:

- [Quickstart](quickstart.md) — from zero to a working producer/consumer.
- [Concepts](concepts.md) — jobs, queues, priorities and the task registry.
- [Reliability](reliability.md) — leases, retries, DLQ and cancellation.
- [API reference](api.md) — every public class and function.
- [CLI](cli.md) — the `redisjobs` command.
