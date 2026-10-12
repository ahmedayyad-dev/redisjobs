"""Structured logging helpers.

redisjobs emits *event-oriented* logs. Each log record carries an ``event``
name plus arbitrary structured ``fields`` (job id, task, queue, attempt, ...).
Contextual fields such as ``job_id`` can be bound with :func:`bind_context` and
are automatically attached to every record emitted inside the block.

Two formatters are provided: :class:`JsonFormatter` (single-line JSON, ideal for
log aggregation) and :class:`TextFormatter` (human readable).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Final

__all__ = [
    "LOGGER_NAME",
    "ContextFilter",
    "EventLogger",
    "JsonFormatter",
    "TextFormatter",
    "bind_context",
    "configure_logging",
    "emit",
    "get_context",
]

LOGGER_NAME: Final[str] = "redisjobs"

_EMPTY_CONTEXT: Final[Mapping[str, Any]] = MappingProxyType({})
_context: ContextVar[Mapping[str, Any]] = ContextVar("redisjobs_context", default=_EMPTY_CONTEXT)

_RESERVED: Final[frozenset[str]] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
        "event",
        "fields",
        "context",
    }
)


def get_context() -> dict[str, Any]:
    """Return a copy of the currently bound logging context."""
    return dict(_context.get())


@contextmanager
def bind_context(**fields: Any) -> Iterator[None]:
    """Temporarily merge ``fields`` into the logging context.

    Args:
        **fields: Key/value pairs to bind for the duration of the block.
    """
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


class ContextFilter(logging.Filter):
    """Attach the contextvar-backed context and normalise event fields."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.context = {**get_context(), **getattr(record, "fields", {})}
        if not getattr(record, "event", None):
            record.event = record.funcName
        return True


def _extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    merged: dict[str, Any] = dict(getattr(record, "context", {}) or {})
    for key, value in record.__dict__.items():
        if key not in _RESERVED and not key.startswith("_"):
            merged.setdefault(key, value)
    if record.exc_info and record.exc_info[0] is not None:
        merged["exc_type"] = record.exc_info[0].__name__
    return merged


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", record.funcName),
            "message": record.message,
        }
        payload.update(_extra_fields(record))
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Render log records as a compact, human-readable line."""

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        ts = datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        event = getattr(record, "event", record.funcName)
        extras = _extra_fields(record)
        tail = " ".join(f"{key}={value}" for key, value in extras.items())
        line = f"{ts} {record.levelname:<8} [{event}] {record.message}"
        if tail:
            line = f"{line} | {tail}"
        if record.exc_info and record.exc_info[0] is not None:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


def configure_logging(
    level: int | str = logging.INFO,
    *,
    json_output: bool = True,
    logger_name: str = LOGGER_NAME,
    stream: Any | None = None,
) -> logging.Logger:
    """Configure and return the ``redisjobs`` logger.

    The logger is marked to *not* propagate to the root logger so that it can be
    styled independently; a single :class:`~logging.StreamHandler` is attached.

    Args:
        level: Numeric or textual logging level.
        json_output: Emit JSON when ``True``, otherwise human-readable text.
        logger_name: Name of the logger to configure.
        stream: Optional stream to write to (defaults to ``sys.stderr``).

    Returns:
        The configured :class:`logging.Logger`.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter() if json_output else TextFormatter())
    handler.addFilter(ContextFilter())
    logger.addHandler(handler)
    return logger


def emit(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str = "",
    *,
    exc_info: bool = False,
    **fields: Any,
) -> None:
    """Emit a structured log record.

    Args:
        logger: Target logger.
        level: Logging level (e.g. :data:`logging.INFO`).
        event: Machine-readable event name (e.g. ``"job_succeeded"``).
        message: Optional human-readable message.
        exc_info: Whether to attach the current exception traceback.
        **fields: Additional structured fields.
    """
    logger.log(
        level,
        message or event,
        exc_info=exc_info,
        extra={"event": event, "fields": fields},
    )


class EventLogger:
    """Thin convenience wrapper around :func:`emit`."""

    __slots__ = ("_defaults", "_logger")

    def __init__(self, logger: logging.Logger, **defaults: Any) -> None:
        self._logger = logger
        self._defaults = defaults

    def bind(self, **fields: Any) -> EventLogger:
        """Return a new :class:`EventLogger` with extra default fields."""
        return EventLogger(self._logger, **{**self._defaults, **fields})

    def _emit(
        self, level: int, event: str, message: str, exc_info: bool, fields: dict[str, Any]
    ) -> None:
        emit(self._logger, level, event, message, exc_info=exc_info, **{**self._defaults, **fields})

    def debug(
        self, event: str, message: str = "", *, exc_info: bool = False, **fields: Any
    ) -> None:
        self._emit(logging.DEBUG, event, message, exc_info, fields)

    def info(self, event: str, message: str = "", *, exc_info: bool = False, **fields: Any) -> None:
        self._emit(logging.INFO, event, message, exc_info, fields)

    def warning(
        self, event: str, message: str = "", *, exc_info: bool = False, **fields: Any
    ) -> None:
        self._emit(logging.WARNING, event, message, exc_info, fields)

    def error(
        self, event: str, message: str = "", *, exc_info: bool = False, **fields: Any
    ) -> None:
        self._emit(logging.ERROR, event, message, exc_info, fields)
