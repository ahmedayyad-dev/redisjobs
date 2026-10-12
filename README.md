# redisjobs

An asynchronous, **Redis-backed job queue** for Python — small, typed and
dependency-light.

`redisjobs` gives you `enqueue`, concurrent `workers`, retries with exponential
backoff, a dead-letter queue, delayed/scheduled jobs, priorities, cancellation
and structured logging — with full type hints and a test suite powered by
`fakeredis`.

> Full documentation lives in [`docs/`](docs/).

## Features

- ⚡ **Async first** — built on `redis.asyncio`, no threads required.
- 🔁 **Reliable delivery** — atomic `LMOVE`/`BLMOVE` leasing + a reaper that
  re-schedules jobs from crashed workers (at-least-once).
- 🎯 **Priorities** — three named levels plus arbitrary integer priorities.
- ⏱️ **Delayed & scheduled jobs** — `enqueue_in` / `enqueue_at` with a built-in
  scheduler.
- ♻️ **Retries** — pluggable exponential backoff with jitter.
- ☠️ **Dead-letter queue** — exhausted jobs are parked, never lost.
- 🚫 **Cancellation** — immediate for queued/scheduled, cooperative for running.
- 🧾 **Structured logging** — JSON events with job/task/queue context.
- 🧪 **Tested** — `pytest` + `fakeredis`, no real Redis needed for CI.

## Installation

```bash
pip install redisjobs
```

Requires Python 3.12+ and a Redis server (6.2+ recommended for `BLMOVE`).

## Quickstart

```python
import asyncio
from redisjobs import Queue, Worker, task


@task(queue="math", max_retries=3)
async def add(a: int, b: int) -> int:
    return a + b


async def main() -> None:
    queue = Queue.from_url("redis://localhost:6379/0")

    # Producer: schedule a job to run in 10 seconds at high priority.
    job = await queue.enqueue(add, 2, 3, priority="high", delay=10)
    print("enqueued", job.id)

    # Consumer: run a worker (blocks until Ctrl-C).
    worker = Worker(queue, queues=["math"], concurrency=4)
    await worker.run()


asyncio.run(main())
```

Run the producer and consumer in separate processes. The worker must import the
module that defines `@task` so the task is registered.

## License

[MIT](LICENSE)
