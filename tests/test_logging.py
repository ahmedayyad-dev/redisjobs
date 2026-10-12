"""Structured logging tests."""

from __future__ import annotations

import json
import logging

from redisjobs.logging import (
    ContextFilter,
    JsonFormatter,
    TextFormatter,
    bind_context,
    configure_logging,
    emit,
    get_context,
)


def _make_record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="redisjobs",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    ContextFilter().filter(record)
    return record


def test_json_formatter_structure() -> None:
    record = _make_record(event="job_succeeded", fields={"job_id": "abc", "attempt": 2})
    payload = json.loads(JsonFormatter().format(record))
    assert payload["event"] == "job_succeeded"
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["job_id"] == "abc"
    assert payload["attempt"] == 2
    assert "ts" in payload


def test_text_formatter_contains_event_and_fields() -> None:
    record = _make_record(event="job_started", fields={"queue": "default"})
    line = TextFormatter().format(record)
    assert "job_started" in line
    assert "queue=default" in line


def test_json_formatter_includes_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = _make_record(event="job_failed", exc_info=sys.exc_info())
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in payload["exception"]


def test_bind_context_restores_previous_context() -> None:
    assert get_context() == {}
    with bind_context(job_id="one"):
        assert get_context() == {"job_id": "one"}
        with bind_context(queue="default"):
            assert get_context() == {"job_id": "one", "queue": "default"}
        assert get_context() == {"job_id": "one"}
    assert get_context() == {}


def test_configure_logging_install_json() -> None:
    logger = configure_logging(logging.DEBUG, json_output=True, logger_name="redisjobs.test")
    assert logger.propagate is False
    assert logger.level == logging.DEBUG
    handler = logger.handlers[0]
    assert isinstance(handler.formatter, JsonFormatter)
    # Emitting must not raise and must include the event.
    emit(logger, logging.INFO, "job_enqueued", "enqueued", job_id="x")


def test_configure_logging_text(capsys) -> None:
    logger = configure_logging(logging.INFO, json_output=False, logger_name="redisjobs.text")
    emit(logger, logging.WARNING, "job_retry_scheduled", "retrying", attempt=1)
    captured = capsys.readouterr()
    assert "job_retry_scheduled" in captured.err
