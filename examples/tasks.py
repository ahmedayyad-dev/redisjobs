"""Shared task definitions for the examples.

Both the producer and the worker import this module so that the tasks are
registered under identical names in both processes.
"""

from __future__ import annotations

import asyncio

from redisjobs import ExponentialBackoff, TaskRegistry, task

# A dedicated registry keeps the examples self-contained.
registry = TaskRegistry()


@task(registry=registry, queue="default", max_retries=3)
async def add(a: int, b: int) -> int:
    await asyncio.sleep(0.1)
    print(f"add({a}, {b}) = {a + b}")
    return a + b


@task(registry=registry, queue="default", max_retries=5, timeout=30.0)
async def fetch_url(url: str) -> str:
    await asyncio.sleep(0.1)
    print(f"fetching {url}")
    return f"contents of {url}"


@task(
    registry=registry,
    queue="default",
    max_retries=5,
    backoff=ExponentialBackoff(base=2.0, factor=1.0, max_delay=60.0),
)
async def send_email(to: str, subject: str) -> None:
    await asyncio.sleep(0.1)
    print(f"email to {to}: {subject}")


@task(registry=registry, queue="default", max_retries=2)
async def flaky() -> str:
    """Always fails — demonstrates the dead-letter queue."""
    raise RuntimeError("boom")
