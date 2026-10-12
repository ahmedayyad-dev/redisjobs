"""Shared pytest fixtures for the redisjobs test-suite."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import fakeredis.aioredis
import pytest
from redis.asyncio import Redis

from redisjobs import Queue, QueueConfig, TaskRegistry


@pytest.fixture
def redis_client() -> Redis:
    """A fresh in-memory Redis server (decoded responses)."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def config() -> QueueConfig:
    """A fast-polling configuration suitable for tests."""
    return QueueConfig(
        poll_timeout=0.05,
        scheduler_interval=0.02,
        reaper_interval=0.05,
        heartbeat_interval=0.05,
        lease=5.0,
        result_ttl=None,
    )


@pytest.fixture
def registry() -> TaskRegistry:
    """A per-test registry so tasks never leak between tests."""
    return TaskRegistry()


@pytest.fixture
def queue(redis_client: Redis, config: QueueConfig, registry: TaskRegistry) -> Queue:
    """A :class:`Queue` bound to the in-memory Redis and a private registry."""
    return Queue(redis_client, config=config, registry=registry)


@pytest.fixture
def wait_for() -> Callable[..., Awaitable[bool]]:
    """Return an async helper that waits until ``predicate`` is truthy."""

    async def _wait_for(
        predicate: Callable[[], Awaitable[bool]] | Callable[[], bool],
        *,
        timeout: float = 5.0,
        interval: float = 0.01,
    ) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            result = predicate()
            if asyncio.iscoroutine(result):
                result = await result
            if result:
                return True
            await asyncio.sleep(interval)
        return False

    return _wait_for
