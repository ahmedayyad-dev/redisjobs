# Quickstart

## 1. Define tasks

Tasks are ordinary (async or sync) functions decorated with `@task`. The
decorator registers them under a name so workers in other processes can find
them.

```python
# myapp/tasks.py
from redisjobs import ExponentialBackoff, task


@task(queue="default", max_retries=3, backoff=ExponentialBackoff(base=2.0))
async def send_welcome(user_id: int) -> None: ...


@task(queue="emails", timeout=30.0)
def render_pdf(document: dict) -> bytes:  # sync tasks run in a thread pool
    ...
```

## 2. Enqueue work

```python
import asyncio
from myapp.tasks import send_welcome
from redisjobs import Priority, Queue


async def main() -> None:
    queue = Queue.from_url("redis://localhost:6379/0")

    await queue.enqueue(send_welcome, 42)
    await queue.enqueue(send_welcome, 43, priority=Priority.HIGH)
    await queue.enqueue_in(60, send_welcome, 44)  # in 60 seconds
    await queue.enqueue_at(datetime_utc, send_welcome, 45)  # absolute time

    await queue.aclose()


asyncio.run(main())
```

`enqueue` returns the stored [`Job`](api.md#job), whose `id` you can keep to
inspect progress or cancel later.

## 3. Run a worker

The worker must import the module that registers the tasks.

```python
# myapp/worker.py
import asyncio
from myapp import tasks  # noqa: F401  (import for side effects / registration)
from redisjobs import Queue, Worker, configure_logging


async def main() -> None:
    configure_logging("INFO", json_output=True)
    queue = Queue.from_url("redis://localhost:6379/0")
    worker = Worker(queue, queues=["default", "emails"], concurrency=8)
    await worker.run()  # blocks; handles SIGINT/SIGTERM


asyncio.run(main())
```

Or with the CLI:

```bash
redisjobs worker -q default -q emails -c 8 --app myapp.tasks
```

## 4. Inspect and cancel

```python
job = await queue.get_job(job_id)
print(job.status, job.result, job.attempt)

await queue.cancel(job_id)  # immediate when queued/scheduled
print(await queue.stats("default"))  # ready / delayed / active / dead counts
```

## Next steps

- Understand the [job lifecycle and priorities](concepts.md).
- Learn how delivery guarantees and retries work in [Reliability](reliability.md).
