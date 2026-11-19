"""Heartbeat tracker: detects dead workers, requeues their tasks, and scales partitions."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from kafka import KafkaConsumer

from . import broker
from .config import Settings
from .store import WORKER_ACTIVE, WORKER_DEAD, WORKER_STOPPED, TaskStore

log = logging.getLogger(__name__)


class HeartbeatTracker:
    """Run exactly one tracker per deployment."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        consumer=None,
        publisher=None,
        admin=None,
        store: TaskStore | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.settings = settings or Settings.from_env()
        self.store = store or TaskStore.from_url(self.settings.redis_url, self.settings.key_prefix)
        self.clock = clock
        if consumer is None:
            broker.ensure_topics(self.settings)
            consumer = KafkaConsumer(
                self.settings.heartbeat_topic,
                auto_offset_reset="latest",
                **broker.consumer_config(self.settings),
            )
        self.consumer = consumer
        self.publisher = publisher or broker.RoundRobinPublisher(broker.make_producer(self.settings))
        self.admin = admin if admin is not None else broker.make_admin(self.settings)
        self._stop = threading.Event()
        self._partitions = self.settings.initial_partitions
        self._last_active_count: int | None = None

        # Resume tracking workers that were alive before a tracker restart, so
        # one that died in the meantime is still detected and its tasks requeued.
        self.last_seen: dict[str, float] = {
            wid: info.get("last_heartbeat") or self.clock()
            for wid, info in self.store.workers().items()
            if info.get("status") == WORKER_ACTIVE
        }

    def observe(self, heartbeat: dict) -> None:
        worker_id, ts = heartbeat["worker_id"], float(heartbeat["timestamp"])
        if worker_id not in self.last_seen:
            log.info("worker %s joined", worker_id)
        self.last_seen[worker_id] = ts
        self.store.record_heartbeat(worker_id, ts)

    def reap(self) -> list[str]:
        """Declare silent workers dead and requeue their in-flight tasks."""
        now = self.clock()
        dead = [w for w, ts in self.last_seen.items() if now - ts > self.settings.heartbeat_timeout]
        for worker_id in dead:
            del self.last_seen[worker_id]
            # A worker interrupted mid-task (Ctrl+C) is "stopped" but may still
            # own a task, so reclaim in both cases.
            stopped = self.store.worker_status(worker_id) == WORKER_STOPPED
            if not stopped:
                self.store.set_worker_status(worker_id, WORKER_DEAD)
            tasks = self.store.reclaim(worker_id)
            if stopped:
                log.info("worker %s shut down; requeuing %d task(s)", worker_id, len(tasks))
            else:
                log.warning("worker %s is dead; requeuing %d task(s)", worker_id, len(tasks))
            for task in tasks:
                self.publisher.send(self.settings.task_topic, {
                    "task_id": task["task_id"],
                    "name": task["name"],
                    "args": task.get("args", []),
                    "kwargs": task.get("kwargs", {}),
                })
        return dead

    def rescale(self) -> None:
        active = len(self.last_seen)
        if active == self._last_active_count:
            return
        self._last_active_count = active
        self.store.set_active_worker_count(active)
        target = max(self.settings.initial_partitions, active * self.settings.partitions_per_worker)
        if target > self._partitions:
            broker.grow_partitions(self.admin, self.settings.task_topic, target)
            self._partitions = target

    def tick(self) -> None:
        for records in self.consumer.poll(timeout_ms=1000).values():
            for record in records:
                try:
                    self.observe(record.value)
                except (KeyError, TypeError, ValueError) as exc:
                    log.error("ignoring malformed heartbeat %r: %s", record.value, exc)
        self.reap()
        self.rescale()

    def run(self) -> None:
        log.info("tracking heartbeats (timeout %.0fs)", self.settings.heartbeat_timeout)
        try:
            while not self._stop.is_set():
                self.tick()
        finally:
            self.consumer.close()

    def stop(self) -> None:
        self._stop.set()
