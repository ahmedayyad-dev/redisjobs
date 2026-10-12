"""Task registration and the :func:`task` decorator.

Workers resolve jobs by name, so tasks must be *registered* in the process that
executes them. Registering is done at import time with :func:`task`::

    from redisjobs import task

    @task(queue="emails", max_retries=5)
    async def send_email(to: str, subject: str) -> None:
        ...
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ParamSpec, TypeVar, cast, overload

from .errors import DuplicateTaskError, TaskNotRegisteredError
from .retry import Backoff, ExponentialBackoff

__all__ = ["TaskRegistry", "TaskSpec", "default_registry", "task"]

P = ParamSpec("P")
R = TypeVar("R")

TaskFunc = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """Metadata describing a registered task.

    Args:
        name: Unique registry name.
        func: The callable to execute (sync or async).
        queue: Default queue for this task, or ``None`` for the queue default.
        max_retries: Default retries after the initial attempt.
        backoff: Default backoff strategy.
        timeout: Default per-attempt timeout in seconds, or ``None``.
    """

    name: str
    func: TaskFunc
    queue: str | None = None
    max_retries: int = 3
    backoff: Backoff = field(default_factory=ExponentialBackoff)
    timeout: float | None = None


class TaskRegistry:
    """A mutable mapping of task name to :class:`TaskSpec`."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskSpec] = {}

    def register(self, spec: TaskSpec) -> TaskSpec:
        """Register ``spec``.

        Raises:
            DuplicateTaskError: If the name is already bound to a different
                function.
        """
        existing = self._tasks.get(spec.name)
        if existing is not None and existing.func is not spec.func:
            raise DuplicateTaskError(f"a different task is already registered as {spec.name!r}")
        self._tasks[spec.name] = spec
        return spec

    def get(self, name: str) -> TaskSpec:
        """Return the spec registered under ``name``.

        Raises:
            TaskNotRegisteredError: If ``name`` is unknown.
        """
        try:
            return self._tasks[name]
        except KeyError as exc:
            raise TaskNotRegisteredError(
                f"no task registered as {name!r}; import the module that defines it"
            ) from exc

    def ensure(self, func: TaskFunc, *, name: str | None = None) -> TaskSpec:
        """Return the spec for ``func``, registering it if necessary."""
        resolved = name or default_task_name(func)
        existing = self._tasks.get(resolved)
        if existing is not None:
            if existing.func is not func:
                raise DuplicateTaskError(f"a different task is already registered as {resolved!r}")
            return existing
        return self.register(TaskSpec(name=resolved, func=func))

    def unregister(self, name: str) -> None:
        """Remove ``name`` from the registry if present."""
        self._tasks.pop(name, None)

    def names(self) -> tuple[str, ...]:
        """Return all registered task names."""
        return tuple(sorted(self._tasks))

    def __contains__(self, name: object) -> bool:
        return name in self._tasks

    def __len__(self) -> int:
        return len(self._tasks)


def default_task_name(func: TaskFunc) -> str:
    """Return the default registry name for ``func`` (``module:qualname``)."""
    module = getattr(func, "__module__", None) or "unknown"
    qualname = getattr(func, "__qualname__", None) or getattr(func, "__name__", "task")
    return f"{module}:{qualname}"


default_registry = TaskRegistry()


@overload
def task(func: Callable[P, R], /) -> Callable[P, R]: ...


@overload
def task(
    *,
    name: str | None = ...,
    queue: str | None = ...,
    max_retries: int = ...,
    backoff: Backoff | None = ...,
    timeout: float | None = ...,
    registry: TaskRegistry | None = ...,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def task(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    queue: str | None = None,
    max_retries: int = 3,
    backoff: Backoff | None = None,
    timeout: float | None = None,
    registry: TaskRegistry | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Register a callable as a task.

    Usable both bare (``@task``) and with arguments (``@task(queue="x")``). The
    original function is returned unchanged and gains a ``__redisjobs_task__``
    attribute referencing its :class:`TaskSpec`.
    """
    target_registry = registry if registry is not None else default_registry

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        spec = TaskSpec(
            name=name or default_task_name(fn),
            func=fn,
            queue=queue,
            max_retries=max_retries,
            backoff=backoff if backoff is not None else ExponentialBackoff(),
            timeout=timeout,
        )
        target_registry.register(spec)
        cast("Any", fn).__redisjobs_task__ = spec
        return fn

    if func is not None:
        return decorator(func)
    return decorator
