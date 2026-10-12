# Reliability

redisjobs targets **at-least-once** delivery: every accepted job is executed at
least once. Tasks should therefore be *idempotent* (safe to run more than once).

## Reliable fetch

A worker never adds a job to an in-memory queue without first recording that it
owns it. When a fetcher needs a job it calls `LMOVE`/`BLMOVE`, which atomically
moves the job id from a `ready:<priority>` list into the worker's
`processing:<worker>` list. Executors are fed through an in-process queue; only
after they finish is the job removed from the processing list.

If a worker process dies mid-execution the job id stays in its processing list
and its lease expires — see below.

## Leases & the reaper

Every in-flight job records a **lease**: a deadline (`now + config.lease`) in a
`leases` sorted set. The worker refreshes the lease of all of its in-flight jobs
on every heartbeat, so a *live* worker's jobs never expire.

A background **reaper** (running inside every worker, guarded by a Redis lock so
only one acts at a time) scans for expired leases. An expired lease means the
owning worker is gone, so the job is:

1. removed from the dead worker's processing list, and
2. re-delivered — counted as a retry, scheduled with the job's backoff.

When the retries are exhausted by repeated worker deaths, the job goes to the
dead-letter queue.

> Set `lease` comfortably above your slowest task duration. Because heartbeats
> refresh leases, a *live but slow* worker is never reclaimed — only truly dead
> ones.

## Retries & backoff

On a task failure the worker increments `attempt` and, if `attempt <=
max_retries`, schedules a retry using the job's backoff strategy:

```
delay(attempt) = min(max_delay, factor * base ** (attempt - 1))   [with jitter]
```

The default is `ExponentialBackoff(base=2, factor=1, max_delay=3600, jitter=True)`.
*Full jitter* (the default) picks a uniform random delay in `[0, delay]`,
avoiding thundering-herd retries. Use `ConstantBackoff` for fixed delays or
implement the [`Backoff`](api.md#backoff) protocol for custom behaviour.

```python
from redisjobs import ExponentialBackoff, RetryPolicy

backoff = ExponentialBackoff(base=2.0, factor=1.0, max_delay=300.0, jitter=True)
await queue.enqueue(task, max_retries=5, backoff=backoff)
```

`TaskNotRegisteredError` is special-cased and dead-lettered immediately — retrying
a missing task can never succeed.

## Dead-letter queue

Jobs that exhaust their retries are pushed onto the per-queue `dead` list and
their record is marked `DEAD` with the final `last_error`. Inspect them with:

```python
stats = await queue.stats("default")
print(stats.dead)

# Raw ids are in the "dead" list under the queue's namespace:
ids = await queue.redis.lrange(queue.config.keys.dead("default"), 0, -1)
```

## Cancellation

- **Queued** jobs are removed from their ready list immediately.
- **Scheduled / retrying** jobs are removed from the delayed set immediately.
- **Running** jobs are cancelled *cooperatively*: a flag is set and, when the
  task returns, the worker discards its result and marks the job `CANCELLED`.
  The scheduler also skips cancelled jobs when promoting them.

```python
if await queue.cancel(job_id):
    print("cancellation requested")
```

## Idempotency & results

Because delivery is at-least-once:

- Make tasks idempotent, or guard side-effects with your own de-duplication key
  (the stable `job.id` is handy for this).
- Results are stored on completion and the job record expires after
  `config.result_ttl` seconds (set `None` to keep forever).
