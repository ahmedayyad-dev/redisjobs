# Concepts

## Jobs

A [`Job`](api.md#job) is a JSON-serialized record describing *what* to run and
*how*:

| Field | Meaning |
| --- | --- |
| `id` | UUID, unique per job. |
| `task` | Registered task name. |
| `queue` | Logical queue. |
| `args` / `kwargs` | Positional and keyword arguments. |
| `priority` | Lower runs sooner. |
| `max_retries` | Retries after the initial attempt. |
| `backoff` | Strategy computing the delay between retries. |
| `timeout` | Per-attempt execution timeout (seconds). |
| `status` | Current lifecycle state. |
| `attempt` | Number of attempts performed so far. |
| `result` / `last_error` | Outcome of the last attempt. |

Only JSON-serializable arguments are supported. This is a deliberate choice: it
keeps jobs portable across languages and processes and avoids the security
pitfalls of pickling.

## Job lifecycle

```
              enqueue_at / delay
                     │
                     ▼
 enqueue ──────► QUEUED ──► ACTIVE ──► COMPLETED
                 ▲   ▲        │
   (retry)       │   │        ├── success ───────────► COMPLETED
  SCHEDULED ◄────┘   │        │
   RETRYING ◄────────┘        ├── failure & retries left ─► RETRYING ─┐
        ▲                     │                                     │
        └─────────────────────┘ (promoted by scheduler)             │
                              │                                     │
                              └── failure, no retries left ─► DEAD ──┘ (DLQ)

 any non-terminal state ── cancel() ──► CANCELLED
```

Terminal states are `COMPLETED`, `DEAD` and `CANCELLED`.

## Queues

Any job belongs to a *queue* (a string). A worker subscribes to a list of
queues. Queues are independent namespaces inside the same Redis database; the
key layout embeds a hash tag `{queue}` so that (on Redis Cluster) every key of a
queue lands in the same slot, which keeps the Lua-based operations atomic.

## Priorities

Priorities are integers; **lower values are dequeued first**. Three named levels
are provided:

| Name | Value |
| --- | --- |
| `Priority.HIGH` | `0` |
| `Priority.DEFAULT` | `5` |
| `Priority.LOW` | `9` |

Any non-negative integer is accepted, so you can slot custom levels in between
(e.g. `2`). Each priority has its own Redis list; a worker polls them from the
highest priority downwards.

> ⚠️ Priority is *strict*: a continuously-fed high-priority queue can starve
> lower priorities. Keep priorities for coarse scheduling, not for fairness.

## Task registry

Workers resolve a job's `task` string to a callable through a
[`TaskRegistry`](api.md#taskregistry). `@task` adds an entry to the global
registry (or a registry you pass with `registry=`). For cross-process setups the
worker must import the module that defines the tasks.

Enqueuing a raw callable auto-registers it, which is convenient for producers
and single-process applications:

```python
queue = Queue.from_url(url, registry=my_registry)
await queue.enqueue(my_function)  # auto-registered as "module:qualname"
await queue.enqueue("module:qualname")  # by name
```

## Configuration

[`QueueConfig`](api.md#queueconfig) controls the namespace prefix, the default
queue, polling, the scheduler/reaper intervals and lease/TTL durations. It is
immutable and safe to share.

```python
from redisjobs import QueueConfig

config = QueueConfig(
    prefix="myapp",
    default_queue="default",
    lease=600.0,  # a job may run up to 10 min before being recovered
    scheduler_interval=1.0,
    reaper_interval=30.0,
    result_ttl=3600,  # keep finished job records for an hour
)
```
