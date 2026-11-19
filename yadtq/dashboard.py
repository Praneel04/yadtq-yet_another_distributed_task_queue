"""Live terminal dashboard of workers and recent tasks (reads Redis only)."""

from __future__ import annotations

import time

from rich.console import Group
from rich.live import Live
from rich.table import Table

from .config import Settings
from .store import FAILED, PROCESSING, QUEUED, SUCCESS, WORKER_ACTIVE, TaskStore

HIDE_GONE_AFTER = 600  # seconds; dead/stopped workers drop off the table after this
STATUS_STYLE = {QUEUED: "yellow", PROCESSING: "cyan", SUCCESS: "green", FAILED: "red"}


def _ago(ts: float | None) -> str:
    return "-" if not ts else f"{time.time() - ts:.0f}s ago"


def render(store: TaskStore, limit: int = 15) -> Group:
    workers = Table(title="Workers", expand=True)
    for col in ("worker", "status", "last heartbeat", "current task", "done", "failed"):
        workers.add_column(col)
    for wid, info in store.workers().items():
        status = info.get("status", "?")
        if status != WORKER_ACTIVE and time.time() - (info.get("last_heartbeat") or 0) > HIDE_GONE_AFTER:
            continue
        workers.add_row(
            wid[:8],
            f"[{'green' if status == WORKER_ACTIVE else 'red'}]{status}[/]",
            _ago(info.get("last_heartbeat")),
            (info.get("current_task") or "-")[:8],
            str(info.get("completed", 0)),
            str(info.get("failed", 0)),
        )

    tasks = Table(title=f"Last {limit} tasks", expand=True)
    for col in ("task", "call", "status", "attempts", "worker", "result / error"):
        tasks.add_column(col, overflow="fold")
    for t in store.recent(limit):
        status = t["status"]
        call = f"{t.get('name')}({', '.join(map(repr, t.get('args', [])))})"
        outcome = repr(t["result"]) if status == SUCCESS else t.get("error", "")
        tasks.add_row(
            t["task_id"][:8], call, f"[{STATUS_STYLE.get(status, 'white')}]{status}[/]",
            str(t.get("attempts", 0)), (t.get("worker") or "-")[:8], outcome,
        )
    return Group(workers, tasks)


def run(settings: Settings | None = None, refresh: float = 2.0, limit: int = 15) -> None:
    settings = settings or Settings.from_env()
    store = TaskStore.from_url(settings.redis_url, settings.key_prefix)
    with Live(render(store, limit), refresh_per_second=4, screen=False) as live:
        while True:
            time.sleep(refresh)
            live.update(render(store, limit))
