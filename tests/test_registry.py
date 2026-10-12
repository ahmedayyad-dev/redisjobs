"""Tests for the task registry and decorator."""

from __future__ import annotations

import pytest

from redisjobs import task
from redisjobs.errors import DuplicateTaskError, TaskNotRegisteredError
from redisjobs.registry import TaskRegistry, default_task_name


def test_task_decorator_bare_registers_in_default_registry() -> None:
    registry = TaskRegistry()

    @task(registry=registry, name="bare")
    async def fn() -> None: ...

    assert "bare" in registry
    assert registry.get("bare").func is fn
    assert fn.__redisjobs_task__.name == "bare"  # type: ignore[attr-defined]


def test_task_decorator_with_options() -> None:
    registry = TaskRegistry()

    @task(name="x", queue="q", max_retries=2, timeout=5.0, registry=registry)
    def fn() -> None: ...

    spec = registry.get("x")
    assert spec.queue == "q"
    assert spec.max_retries == 2
    assert spec.timeout == 5.0


def test_default_task_name_uses_module_qualname() -> None:
    def sample() -> None: ...

    name = default_task_name(sample)
    assert ":" in name
    assert name.endswith("sample")


def test_duplicate_registration_rejected() -> None:
    from redisjobs.registry import TaskSpec

    registry = TaskRegistry()

    async def a() -> None: ...

    async def b() -> None: ...

    registry.register(TaskSpec(name="dup", func=a))
    with pytest.raises(DuplicateTaskError):
        registry.register(TaskSpec(name="dup", func=b))


def test_ensure_is_idempotent_and_rejects_conflicts() -> None:
    registry = TaskRegistry()

    async def a() -> None: ...

    async def b() -> None: ...

    spec = registry.ensure(a, name="n")
    assert registry.ensure(a, name="n") is spec
    with pytest.raises(DuplicateTaskError):
        registry.ensure(b, name="n")


def test_get_missing_raises() -> None:
    registry = TaskRegistry()
    with pytest.raises(TaskNotRegisteredError):
        registry.get("nope")


def test_unregister_and_names() -> None:
    registry = TaskRegistry()

    async def a() -> None: ...

    registry.ensure(a, name="a")
    assert registry.names() == ("a",)
    assert len(registry) == 1
    registry.unregister("a")
    assert "a" not in registry
    assert registry.names() == ()
