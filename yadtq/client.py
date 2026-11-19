"""Client API: submit tasks and query their status."""

from __future__ import annotations

import time
import uuid
from typing import Any

from . import broker
from .config import Settings
from .store import TERMINAL, TaskStore


class TaskTimeout(TimeoutError):
    pass


class YADTQClient:
    def __init__(self, settings: Settings | None = None, *, publisher=None, store: TaskStore | None = None):
        self.settings = settings or Settings.from_env()
        if publisher is None:
            broker.ensure_topics(self.settings)
            publisher = broker.RoundRobinPublisher(broker.make_producer(self.settings))
        self.publisher = publisher
        self.store = store or TaskStore.from_url(self.settings.redis_url, self.settings.key_prefix)

    def submit(self, name: str, *args: Any, **kwargs: Any) -> str:
        """Queue ``name(*args, **kwargs)`` for execution and return its task id."""
        task_id = str(uuid.uuid4())
        # Record the task before publishing so a fast worker never sees an unknown id.
        self.store.create(task_id, name, list(args), kwargs)
        self.publisher.send(self.settings.task_topic, {
            "task_id": task_id, "name": name, "args": list(args), "kwargs": kwargs,
        })
        return task_id

    def status(self, task_id: str) -> dict[str, Any]:
        return self.store.get(task_id) or {"task_id": task_id, "status": "unknown"}

    def wait(self, task_id: str, timeout: float | None = None, poll: float = 0.5) -> dict[str, Any]:
        """Block until the task succeeds or fails, and return its final status."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            info = self.status(task_id)
            if info["status"] in TERMINAL:
                return info
            if deadline is not None and time.monotonic() >= deadline:
                raise TaskTimeout(f"task {task_id} still {info['status']} after {timeout}s")
            time.sleep(poll)

    # Names used by the original course API.
    def submit_task(self, name: str, args: list) -> str:
        return self.submit(name, *args)

    def get_task_status(self, task_id: str) -> dict[str, Any]:
        return self.status(task_id)
