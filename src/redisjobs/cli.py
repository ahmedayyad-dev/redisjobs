"""Command line interface: ``redisjobs worker|enqueue|stats``."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
from collections.abc import Sequence
from typing import Any

from ._version import __version__
from .job import Job
from .logging import configure_logging
from .queue import Queue
from .worker import Worker

__all__ = ["main"]

DEFAULT_URL = "redis://localhost:6379/0"


def _load_app(targets: Sequence[str]) -> None:
    for target in targets:
        importlib.import_module(target)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="redisjobs", description="Redis-backed async job queue.")
    parser.add_argument("--version", action="version", version=f"redisjobs {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker", help="run a worker consuming one or more queues")
    worker.add_argument("-q", "--queue", action="append", dest="queues", default=None)
    worker.add_argument("-c", "--concurrency", type=int, default=10)
    worker.add_argument("--url", default=DEFAULT_URL)
    worker.add_argument(
        "-a", "--app", action="append", default=[], help="module(s) to import for task registration"
    )
    worker.add_argument("--log-level", default="INFO")
    worker.add_argument("--no-json-logs", action="store_true")

    enqueue = sub.add_parser("enqueue", help="enqueue a task by name")
    enqueue.add_argument("task", help="registered task name")
    enqueue.add_argument(
        "args", nargs="*", help="positional arguments (parsed as JSON when possible)"
    )
    enqueue.add_argument("-q", "--queue", default=None)
    enqueue.add_argument("-p", "--priority", type=int, default=5)
    enqueue.add_argument("-d", "--delay", type=float, default=None)
    enqueue.add_argument("--url", default=DEFAULT_URL)

    stats = sub.add_parser("stats", help="print queue statistics as JSON")
    stats.add_argument("-q", "--queue", default=None)
    stats.add_argument("--url", default=DEFAULT_URL)
    stats.add_argument("--log-level", default="WARNING")
    return parser


def _parse_arg(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


async def _cmd_worker(args: argparse.Namespace) -> int:
    configure_logging(args.log_level, json_output=not args.no_json_logs)
    _load_app(args.app)
    queue = Queue.from_url(args.url)
    worker = Worker(queue, queues=args.queues, concurrency=args.concurrency)
    try:
        await worker.run()
    finally:
        await queue.aclose()
    return 0


async def _cmd_enqueue(args: argparse.Namespace) -> int:
    queue = Queue.from_url(args.url)
    try:
        parsed = [_parse_arg(a) for a in args.args]
        job: Job = await queue.enqueue(
            args.task, *parsed, queue=args.queue, priority=args.priority, delay=args.delay
        )
        print(
            json.dumps(
                {"id": job.id, "task": job.task, "queue": job.queue, "status": job.status.value}
            )
        )
    finally:
        await queue.aclose()
    return 0


async def _cmd_stats(args: argparse.Namespace) -> int:
    configure_logging(args.log_level, json_output=True)
    queue = Queue.from_url(args.url)
    try:
        stats = await queue.stats(args.queue)
        print(json.dumps(stats.as_dict(), indent=2))
    finally:
        await queue.aclose()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``redisjobs`` console script."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    handlers = {"worker": _cmd_worker, "enqueue": _cmd_enqueue, "stats": _cmd_stats}
    try:
        return asyncio.run(handlers[args.command](args))
    except KeyboardInterrupt:  # pragma: no cover - interactive
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
