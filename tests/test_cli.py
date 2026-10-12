"""Lightweight CLI tests (no real Redis needed)."""

from __future__ import annotations

import json

import pytest

from redisjobs import Queue
from redisjobs.cli import _build_parser, _parse_arg, main


def test_parse_arg_json_and_fallback() -> None:
    assert _parse_arg("42") == 42
    assert _parse_arg('{"a": 1}') == {"a": 1}
    assert _parse_arg("hello") == "hello"


def test_parser_requires_subcommand() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_worker_defaults() -> None:
    parser = _build_parser()
    args = parser.parse_args(["worker"])
    assert args.command == "worker"
    assert args.concurrency == 10
    assert args.queues is None


def test_cli_enqueue(redis_client, config, monkeypatch, capsys) -> None:
    def fake_from_url(url: str, **kwargs: object) -> Queue:
        return Queue(redis_client, config=config)

    monkeypatch.setattr(Queue, "from_url", staticmethod(fake_from_url))
    code = main(["enqueue", "my.task", "1", "two", "-q", "default"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["task"] == "my.task"
    assert payload["queue"] == "default"
