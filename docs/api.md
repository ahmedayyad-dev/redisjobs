# API reference

Everything below is importable directly from `redisjobs`.

```python
from redisjobs import (
    Queue,
    QueueStats,
    Worker,
    QueueConfig,
    Job,
    JobStatus,
    Priority,
    task,
    TaskRegistry,
    get_current_job,
    Backoff,
    ExponentialBackoff,
    ConstantBackoff,
    RetryPolicy,
    configure_logging,
    bind_context,
    # errors
    RedisJobsError,
    ConfigurationError,
    JobNotFoundError,
    DuplicateTaskError,
    TaskNotRegisteredError,
    SerializationError,
    JobCancelledError,
    JobTimeoutError,
    QueueEmptyError,
)
```

## Queue

```python
class Queue:
    def __init__(self, redis, *, config=None, registry=None, logger=None): ...
    @classmethod
    def from_url(cls, url, *, config=None, registry=None, logger=None, **redis_kwargs) -> Queue: ...
    async def aclose(self) -> None: ...
```

| Method | Description |
| --- | --- |
| `await enqueue(task, *args, queue=None, priority=Priority.DEFAULT, max_retries=None, backoff=None, timeout=None, job_id=None, delay=None, at=None, **kwargs) -> Job` | Create and store a job. `task` is a callable or a registered name. `delay` (seconds) and `at` (datetime/timestamp) are mutually exclusive. |
| `await enqueue_at(when, task, *args, **kwargs) -> Job` | Convenience for `enqueue(..., at=when)`. |
| `await enqueue_in(delay, task, *args, **kwargs) -> Job` | Convenience for `enqueue(..., delay=delay)`. |
| `await get_job(job_id) -> Job` | Fetch a job; raises `JobNotFoundError`. |
| `await cancel(job_id) -> bool` | Request cancellation; `False` if unknown/terminal. |
| `await stats(queue=None) -> QueueStats` | Snapshot of ready/delayed/active/dead counts. |

`Queue` is an async context manager.

## Worker

```python
class Worker:
    def __init__(
        self,
        queue,
        *,
        queues=None,
        concurrency=10,
        config=None,
        registry=None,
        logger=None,
        install_signal_handlers=True,
        blocking=None,
    ): ...
    @classmethod
    def from_url(cls, url, *, queues=None, **kwargs) -> Worker: ...
    async def start(self) -> None: ...
    async def run(self) -> None: ...  # start() then block until stop()
    async def stop(self, *, timeout=30.0) -> None: ...
```

- `queues` defaults to `[config.default_queue]`.
- `blocking=None` auto-detects: single-node Redis uses blocking `BLMOVE`;
  `fakeredis` falls back to short polling so tests never hang.
- `install_signal_handlers=True` traps `SIGINT`/`SIGTERM` for graceful shutdown.
- `Worker` is an async context manager.

Inside a task, `get_current_job()` returns the `Job` being executed.

## QueueConfig

```python
@dataclass(frozen=True)
class QueueConfig:
    prefix: str = "redisjobs"
    default_queue: str = "default"
    poll_timeout: float = 5.0
    lease: float = 300.0
    heartbeat_interval: float = 15.0
    scheduler_interval: float = 1.0
    reaper_interval: float = 30.0
    result_ttl: int | None = 3600
    dead_ttl: int | None = None
```

## Job & JobStatus

`Job` is a dataclass with the fields documented in
[Concepts](concepts.md#jobs), plus:

- `is_terminal: bool`
- `retries_left: int`
- `to_mapping() -> dict[str, str]` / `Job.from_mapping(mapping) -> Job`

`JobStatus` is a `StrEnum`:
`QUEUED`, `SCHEDULED`, `RETRYING`, `ACTIVE`, `COMPLETED`, `DEAD`, `CANCELLED`,
with an `is_terminal` property.

## Priority

`Priority` is an `IntEnum` (`HIGH=0`, `DEFAULT=5`, `LOW=9`). Any non-negative
integer is also accepted wherever a priority is expected.

## Tasks

```python
@task
def fn(...): ...

@task(name=None, queue=None, max_retries=3, backoff=None, timeout=None, registry=None)
def fn(...): ...
```

| Symbol | Description |
| --- | --- |
| `TaskRegistry` | Mutable `name -> TaskSpec` mapping with `register`, `get`, `ensure`, `unregister`, `names`. |
| `default_registry` | The process-wide registry used by `@task` unless `registry=` is given. |
| `get_current_job()` | Returns the running `Job` (or `None`). |

## Retries

| Symbol | Description |
| --- | --- |
| `Backoff` | Runtime-checkable protocol with `delay_for(attempt: int) -> float` (1-based). |
| `ExponentialBackoff(base=2.0, factor=1.0, max_delay=3600.0, min_delay=0.0, jitter=True)` | Exponential delay with optional full jitter. |
| `ConstantBackoff(delay=1.0)` | Fixed delay. |
| `RetryPolicy(max_retries=3, backoff=ExponentialBackoff())` | Bundles a count and a strategy. |

## Logging

```python
configure_logging(level="INFO", *, json_output=True, logger_name="redisjobs", stream=None)
bind_context(**fields)          # context manager; fields attach to every record
```

redisjobs emits structured events such as `job_enqueued`, `job_scheduled`,
`job_started`, `job_succeeded`, `job_retry_scheduled`, `job_dead_lettered`,
`job_cancelled`, `job_promoted`, `job_lease_expired` and `worker_started/stopped`.

## Exceptions

| Exception | Raised when |
| --- | --- |
| `RedisJobsError` | Base class. |
| `ConfigurationError` | Invalid configuration. |
| `JobNotFoundError` | Unknown/expired job id. |
| `DuplicateTaskError` | Two tasks share a name. |
| `TaskNotRegisteredError` | A worker cannot resolve a task name. |
| `SerializationError` | A value is not JSON-serializable. |
| `JobCancelledError` | Available to tasks signalling their own cancellation. |
| `JobTimeoutError` | A task exceeded its `timeout`. |
| `QueueEmptyError` | A blocking fetch returned nothing. |
