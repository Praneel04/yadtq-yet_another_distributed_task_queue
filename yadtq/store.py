"""Redis-backed result backend: task state and the worker registry.

Key layout (``<p>`` is ``Settings.key_prefix``):

    <p>:task:<id>            hash   status, name, args, kwargs, attempts, worker, result, error, timestamps
    <p>:tasks                zset   task ids scored by creation time (for listing)
    <p>:worker:<id>          hash   status, last_heartbeat, current_task, completed, failed
    <p>:worker:<id>:tasks    set    ids of tasks the worker is currently processing
    <p>:workers              set    every worker id ever seen
"""

from __future__ import annotations

import json
import time
from typing import Any

import redis

QUEUED = "queued"
PROCESSING = "processing"
SUCCESS = "success"
FAILED = "failed"
TERMINAL = frozenset({SUCCESS, FAILED})

WORKER_ACTIVE = "active"
WORKER_DEAD = "dead"  # stopped heartbeating; declared by the tracker
WORKER_STOPPED = "stopped"  # shut down cleanly

_JSON_FIELDS = ("args", "kwargs", "result")
_FLOAT_FIELDS = ("created_at", "started_at", "finished_at", "last_heartbeat")
_INT_FIELDS = ("attempts", "completed", "failed")


def _decode(raw: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = dict(raw)
    for k in _JSON_FIELDS:
        if k in out:
            out[k] = json.loads(out[k])
    for k in _FLOAT_FIELDS:
        if out.get(k):
            out[k] = float(out[k])
    for k in _INT_FIELDS:
        if k in out:
            out[k] = int(out[k])
    return out


class TaskStore:
    def __init__(self, client: redis.Redis, prefix: str = "yadtq"):
        self.r = client
        self.p = prefix

    @classmethod
    def from_url(cls, url: str, prefix: str = "yadtq") -> "TaskStore":
        return cls(redis.Redis.from_url(url, decode_responses=True), prefix)

    # -- keys -------------------------------------------------------------
    def _task(self, task_id: str) -> str:
        return f"{self.p}:task:{task_id}"

    def _worker(self, worker_id: str) -> str:
        return f"{self.p}:worker:{worker_id}"

    def _worker_tasks(self, worker_id: str) -> str:
        return f"{self.p}:worker:{worker_id}:tasks"

    # -- tasks ------------------------------------------------------------
    def create(self, task_id: str, name: str, args: list, kwargs: dict) -> None:
        now = time.time()
        pipe = self.r.pipeline()
        pipe.hset(self._task(task_id), mapping={
            "status": QUEUED,
            "name": name,
            "args": json.dumps(args),
            "kwargs": json.dumps(kwargs),
            "attempts": 0,
            "created_at": now,
        })
        pipe.zadd(f"{self.p}:tasks", {task_id: now})
        pipe.execute()

    def get(self, task_id: str) -> dict[str, Any] | None:
        raw = self.r.hgetall(self._task(task_id))
        if not raw:
            return None
        return {"task_id": task_id, **_decode(raw)}

    def recent(self, limit: int = 20) -> list[dict[str, Any]]:
        ids = self.r.zrevrange(f"{self.p}:tasks", 0, limit - 1)
        return [t for t in (self.get(i) for i in ids) if t]

    def mark_processing(self, task_id: str, worker_id: str) -> None:
        pipe = self.r.pipeline()
        pipe.hset(self._task(task_id), mapping={
            "status": PROCESSING, "worker": worker_id, "started_at": time.time(),
        })
        pipe.sadd(self._worker_tasks(worker_id), task_id)
        pipe.sadd(f"{self.p}:workers", worker_id)
        pipe.hset(self._worker(worker_id), "current_task", task_id)
        pipe.execute()

    def mark_success(self, task_id: str, worker_id: str, result: Any) -> None:
        pipe = self.r.pipeline()
        pipe.hset(self._task(task_id), mapping={
            "status": SUCCESS,
            "result": json.dumps(result, default=str),
            "finished_at": time.time(),
        })
        pipe.hdel(self._task(task_id), "error")
        self._release(pipe, task_id, worker_id, "completed")
        pipe.execute()

    def mark_attempt_failed(self, task_id: str, worker_id: str, error: str) -> int:
        """Record a failed attempt, put the task back to ``queued``; return the attempt count."""
        pipe = self.r.pipeline()
        pipe.hincrby(self._task(task_id), "attempts", 1)
        pipe.hset(self._task(task_id), mapping={"status": QUEUED, "error": error, "worker": ""})
        self._release(pipe, task_id, worker_id, None)
        return int(pipe.execute()[0])

    def mark_failed(self, task_id: str, worker_id: str | None, error: str) -> None:
        pipe = self.r.pipeline()
        pipe.hset(self._task(task_id), mapping={
            "status": FAILED, "error": error, "finished_at": time.time(),
        })
        if worker_id:
            self._release(pipe, task_id, worker_id, "failed")
        pipe.execute()

    def _release(self, pipe, task_id: str, worker_id: str, counter: str | None) -> None:
        pipe.srem(self._worker_tasks(worker_id), task_id)
        pipe.hset(self._worker(worker_id), "current_task", "")
        if counter:
            pipe.hincrby(self._worker(worker_id), counter, 1)

    def reclaim(self, worker_id: str) -> list[dict[str, Any]]:
        """Reset every task a dead worker was processing to ``queued`` and return them."""
        reclaimed = []
        for task_id in self.r.smembers(self._worker_tasks(worker_id)):
            task = self.get(task_id)
            if task and task["status"] == PROCESSING and task.get("worker") == worker_id:
                self.r.hset(self._task(task_id), mapping={"status": QUEUED, "worker": ""})
                reclaimed.append(task)
        self.r.delete(self._worker_tasks(worker_id))
        self.r.hset(self._worker(worker_id), "current_task", "")
        return reclaimed

    # -- workers ----------------------------------------------------------
    def record_heartbeat(self, worker_id: str, timestamp: float) -> None:
        pipe = self.r.pipeline()
        pipe.sadd(f"{self.p}:workers", worker_id)
        pipe.hset(self._worker(worker_id), mapping={
            "status": WORKER_ACTIVE, "last_heartbeat": timestamp,
        })
        pipe.execute()

    def set_worker_status(self, worker_id: str, status: str) -> None:
        self.r.hset(self._worker(worker_id), "status", status)

    def worker_status(self, worker_id: str) -> str | None:
        return self.r.hget(self._worker(worker_id), "status")

    def workers(self) -> dict[str, dict[str, Any]]:
        ids = sorted(self.r.smembers(f"{self.p}:workers"))
        return {w: _decode(self.r.hgetall(self._worker(w))) for w in ids}

    def set_active_worker_count(self, n: int) -> None:
        self.r.set(f"{self.p}:active_workers", n)
