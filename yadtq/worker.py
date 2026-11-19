"""Worker: consumes tasks from Kafka, runs them, and records results in Redis."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable

from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from . import broker
from .config import Settings
from .store import PROCESSING, TERMINAL, WORKER_STOPPED, TaskStore

log = logging.getLogger(__name__)

TaskFn = Callable[..., Any]


class YADTQWorker:
    """Runs registered task functions.

    Delivery is at-least-once: the Kafka offset is committed only after a task
    finishes, and the heartbeat tracker re-publishes tasks of dead workers. A
    task that is already ``success``/``failed`` is skipped when it shows up again.
    """

    def __init__(
        self,
        tasks: dict[str, TaskFn] | None = None,
        settings: Settings | None = None,
        *,
        worker_id: str | None = None,
        consumer=None,
        publisher=None,
        store: TaskStore | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.settings = settings or Settings.from_env()
        self.tasks: dict[str, TaskFn] = dict(tasks or {})
        self.worker_id = worker_id or str(uuid.uuid4())
        self.store = store or TaskStore.from_url(self.settings.redis_url, self.settings.key_prefix)
        self._consumer = consumer
        self._publisher = publisher
        self._sleep = sleep
        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    def task(self, fn: TaskFn | None = None, *, name: str | None = None):
        """Decorator that registers a function as a task: ``@worker.task``."""
        def register(f: TaskFn) -> TaskFn:
            self.tasks[name or f.__name__] = f
            return f
        return register(fn) if fn else register

    # -- Kafka connections (created lazily so handle() is testable without Kafka) --
    @property
    def publisher(self):
        if self._publisher is None:
            self._publisher = broker.RoundRobinPublisher(broker.make_producer(self.settings))
        return self._publisher

    @property
    def consumer(self):
        if self._consumer is None:
            s = self.settings
            broker.ensure_topics(s)
            self._consumer = KafkaConsumer(
                s.task_topic,
                group_id=s.consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=1,  # commit after every task, never lose a fetched batch
                max_poll_interval_ms=s.max_task_seconds * 1000,
                partition_assignment_strategy=[RoundRobinPartitionAssignor],
                **broker.consumer_config(s),
            )
        return self._consumer

    # -- task execution -----------------------------------------------------
    def handle(self, message: dict[str, Any]) -> None:
        """Run one task message to completion, retrying or failing it as needed."""
        task_id = message["task_id"]
        name = message.get("name") or message.get("task")  # "task" = original message format
        args = message.get("args") or []
        kwargs = message.get("kwargs") or {}

        current = self.store.get(task_id)
        if current and current["status"] in TERMINAL:
            log.info("skipping %s: already %s", task_id, current["status"])
            return
        if current and current["status"] == PROCESSING and current.get("worker") != self.worker_id:
            # A duplicate delivery (Kafka redelivery plus tracker requeue). The
            # owner either finishes it or dies, and then the tracker requeues it.
            log.info("skipping %s: already running on %s", task_id, current["worker"])
            return
        if current is None:  # published by someone who bypassed the client
            self.store.create(task_id, name, args, kwargs)

        fn = self.tasks.get(name)
        if fn is None:
            log.error("task %s: unknown task %r", task_id, name)
            self.store.mark_failed(task_id, None, f"unknown task {name!r}")
            return

        log.info("task %s: running %s(*%r, **%r)", task_id, name, args, kwargs)
        self.store.mark_processing(task_id, self.worker_id)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - any task error is recorded
            self._on_error(message, exc)
        else:
            self.store.mark_success(task_id, self.worker_id, result)
            log.info("task %s: success -> %r", task_id, result)

    def _on_error(self, message: dict[str, Any], exc: Exception) -> None:
        task_id = message["task_id"]
        error = f"{type(exc).__name__}: {exc}"
        max_retries = self.settings.max_retries
        attempts = self.store.mark_attempt_failed(task_id, self.worker_id, error)
        if attempts <= max_retries:
            delay = self.settings.retry_delay * 2 ** (attempts - 1)
            log.warning("task %s: attempt %d failed (%s); retry %d/%d in %.1fs",
                        task_id, attempts, error, attempts, max_retries, delay)
            self._sleep(delay)
            self.publisher.send(self.settings.task_topic, message)
        else:
            self.store.mark_failed(task_id, self.worker_id, error)
            log.error("task %s: failed permanently after %d attempts: %s", task_id, attempts, error)

    # -- lifecycle ----------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.publisher.send(self.settings.heartbeat_topic, {
                    "worker_id": self.worker_id, "timestamp": time.time(),
                })
            except Exception:  # noqa: BLE001 - keep beating through transient errors
                log.exception("failed to send heartbeat")
            self._stop.wait(self.settings.heartbeat_interval)

    def run(self) -> None:
        """Consume and execute tasks until :meth:`stop` is called."""
        log.info("worker %s started with tasks: %s", self.worker_id, ", ".join(sorted(self.tasks)))
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat")
        self._heartbeat_thread.start()
        try:
            while not self._stop.is_set():
                for records in self.consumer.poll(timeout_ms=1000).values():
                    for record in records:
                        try:
                            self.handle(record.value)
                        except (KeyError, TypeError) as exc:
                            log.error("dropping malformed message %r: %s", record.value, exc)
                        self.consumer.commit()
        finally:
            self._stop.set()
            self._heartbeat_thread.join(timeout=2)
            self.store.set_worker_status(self.worker_id, WORKER_STOPPED)
            self.consumer.close()
            log.info("worker %s stopped", self.worker_id)

    def stop(self) -> None:
        self._stop.set()
