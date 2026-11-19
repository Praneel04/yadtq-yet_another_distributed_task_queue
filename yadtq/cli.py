"""Command line entry point: ``yadtq {worker,tracker,dashboard,submit,status}``."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sys

from .config import Settings


def _load_tasks(spec: str) -> dict:
    """Load ``module`` or ``module:attr``; the attr must be a {name: function} dict."""
    sys.path.insert(0, os.getcwd())
    module_name, _, attr = spec.partition(":")
    tasks = getattr(importlib.import_module(module_name), attr or "TASKS")
    if not isinstance(tasks, dict):
        raise SystemExit(f"{spec} is not a dict of task functions")
    return tasks


def _parse_arg(raw: str):
    """Arguments are JSON literals (``1``, ``2.5``, ``"x"``, ``[1,2]``); anything else is a string."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _stop_on_sigterm(component) -> None:
    signal.signal(signal.SIGTERM, lambda *_: component.stop())


def cmd_worker(args, settings):
    from .worker import YADTQWorker

    worker = YADTQWorker(_load_tasks(args.tasks), settings)
    _stop_on_sigterm(worker)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()


def cmd_tracker(args, settings):
    from .tracker import HeartbeatTracker

    tracker = HeartbeatTracker(settings)
    _stop_on_sigterm(tracker)
    try:
        tracker.run()
    except KeyboardInterrupt:
        tracker.stop()


def cmd_dashboard(args, settings):
    from . import dashboard

    try:
        dashboard.run(settings, refresh=args.refresh, limit=args.limit)
    except KeyboardInterrupt:
        pass


def cmd_submit(args, settings):
    from .client import YADTQClient

    client = YADTQClient(settings)
    task_id = client.submit(args.name, *map(_parse_arg, args.args))
    if not args.wait:
        print(task_id)
        return
    info = client.wait(task_id, timeout=args.timeout)
    print(json.dumps(info, indent=2, default=str))
    sys.exit(0 if info["status"] == "success" else 1)


def cmd_status(args, settings):
    from .store import TaskStore

    store = TaskStore.from_url(settings.redis_url, settings.key_prefix)
    info = store.get(args.task_id) or {"task_id": args.task_id, "status": "unknown"}
    print(json.dumps(info, indent=2, default=str))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="yadtq", description="Yet Another Distributed Task Queue")
    parser.add_argument("--kafka", help="Kafka bootstrap servers (env YADTQ_KAFKA_BOOTSTRAP_SERVERS)")
    parser.add_argument("--redis", help="Redis URL (env YADTQ_REDIS_URL)")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("worker", help="run a worker")
    p.add_argument("--tasks", default="examples.tasks", help="module[:attr] with a TASKS dict")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("tracker", help="track heartbeats, requeue tasks of dead workers")
    p.set_defaults(func=cmd_tracker)

    p = sub.add_parser("dashboard", help="live view of workers and tasks")
    p.add_argument("--refresh", type=float, default=2.0)
    p.add_argument("--limit", type=int, default=15)
    p.set_defaults(func=cmd_dashboard)

    p = sub.add_parser("submit", help="submit a task, e.g. `yadtq submit add 1 2 --wait`")
    p.add_argument("name")
    p.add_argument("args", nargs="*")
    p.add_argument("--wait", action="store_true", help="block until the task finishes")
    p.add_argument("--timeout", type=float, default=None)
    p.set_defaults(func=cmd_submit)

    p = sub.add_parser("status", help="print a task's status")
    p.add_argument("task_id")
    p.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # kafka-python logs every connection at INFO; keep the output readable.
    logging.getLogger("kafka").setLevel(logging.DEBUG if args.verbose else logging.WARNING)

    overrides = {}
    if args.kafka:
        overrides["kafka_bootstrap_servers"] = args.kafka
    if args.redis:
        overrides["redis_url"] = args.redis
    args.func(args, Settings.from_env(**overrides))


if __name__ == "__main__":
    main()
