"""Queue and worker configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from .keys import DEFAULT_PREFIX, KeyBuilder

__all__ = ["QueueConfig"]


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """Immutable configuration shared by a :class:`~redisjobs.queue.Queue`.

    Args:
        prefix: Redis key namespace.
        default_queue: Queue name used when a task does not declare one.
        poll_timeout: Seconds a worker blocks waiting for a job before looping.
        lease: Seconds a job may stay in-flight before the reaper considers the
            owning worker dead and re-schedules the job.
        heartbeat_interval: Seconds between worker heartbeat refreshes.
        scheduler_interval: Seconds between scheduler ticks (delayed promotion).
        reaper_interval: Seconds between reaper ticks (stale lease recovery).
        result_ttl: Seconds to retain a finished job hash, or ``None`` to keep.
        dead_ttl: Seconds to retain entries in the dead-letter queue, or ``None``.
    """

    prefix: str = DEFAULT_PREFIX
    default_queue: str = "default"
    poll_timeout: float = 5.0
    lease: float = 300.0
    heartbeat_interval: float = 15.0
    scheduler_interval: float = 1.0
    reaper_interval: float = 30.0
    result_ttl: int | None = 3600
    dead_ttl: int | None = None

    keys: KeyBuilder = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.poll_timeout <= 0:
            raise ValueError("poll_timeout must be positive")
        if self.lease <= 0:
            raise ValueError("lease must be positive")
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.lease:
            raise ValueError("heartbeat_interval must be smaller than lease")
        object.__setattr__(self, "keys", KeyBuilder(self.prefix))
