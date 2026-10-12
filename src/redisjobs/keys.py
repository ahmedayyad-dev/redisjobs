"""Centralised Redis key naming.

All keys share a configurable ``prefix`` (namespace). Keys that belong to a
single logical queue embed a Redis *hash tag* (``{queue}``) so that they land in
the same cluster slot, which lets the bundled Lua scripts stay atomic on Redis
Cluster.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["DEFAULT_PREFIX", "KeyBuilder"]

DEFAULT_PREFIX = "redisjobs"


@dataclass(frozen=True, slots=True)
class KeyBuilder:
    """Build every Redis key used by redisjobs.

    Args:
        prefix: Namespace prepended to every key.
    """

    prefix: str = DEFAULT_PREFIX

    def ready(self, queue: str, priority: int) -> str:
        """Return the LIST key holding ready jobs for ``queue``/``priority``."""
        return f"{self.prefix}:{{{queue}}}:ready:{priority}"

    def ready_pattern(self, queue: str) -> str:
        """Return a SCAN pattern matching every ready-priority list of ``queue``."""
        return f"{self.prefix}:{{{queue}}}:ready:*"

    def delayed(self, queue: str) -> str:
        """Return the ZSET key holding scheduled/retrying jobs for ``queue``."""
        return f"{self.prefix}:{{{queue}}}:delayed"

    def processing(self, queue: str, worker_id: str) -> str:
        """Return the LIST key holding in-flight jobs leased by ``worker_id``."""
        return f"{self.prefix}:{{{queue}}}:processing:{worker_id}"

    def dead(self, queue: str) -> str:
        """Return the LIST key acting as the dead-letter queue for ``queue``."""
        return f"{self.prefix}:{{{queue}}}:dead"

    def job(self, job_id: str) -> str:
        """Return the HASH key storing a job's payload and metadata."""
        return f"{self.prefix}:job:{job_id}"

    def queues(self) -> str:
        """Return the SET key of known queue names."""
        return f"{self.prefix}:queues"

    def priorities(self, queue: str) -> str:
        """Return the SET key of priority levels ever used by ``queue``."""
        return f"{self.prefix}:{{{queue}}}:priorities"

    def leases(self, queue: str) -> str:
        """Return the ZSET key mapping in-flight job ids to lease expiry times."""
        return f"{self.prefix}:{{{queue}}}:leases"

    def cancelled(self, queue: str) -> str:
        """Return the SET key of cancelled job ids for ``queue``."""
        return f"{self.prefix}:{{{queue}}}:cancelled"

    def workers(self) -> str:
        """Return the SET key of registered worker ids."""
        return f"{self.prefix}:workers"

    def worker_info(self, worker_id: str) -> str:
        """Return the HASH key holding a worker's heartbeat metadata."""
        return f"{self.prefix}:worker:{worker_id}"

    def schedule_locks(self, queue: str) -> str:
        """Return the lock key guarding scheduler/reaper leader election."""
        return f"{self.prefix}:{{{queue}}}:schedule:lock"
