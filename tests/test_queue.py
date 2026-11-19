"""Unit tests that run without Kafka or Redis (fakeredis + in-memory publisher)."""

import fakeredis
import pytest

from yadtq import FAILED, PROCESSING, QUEUED, SUCCESS, Settings, TaskStore
from yadtq.client import TaskTimeout, YADTQClient
from yadtq.tracker import HeartbeatTracker
from yadtq.worker import YADTQWorker

SETTINGS = Settings(max_retries=2, retry_delay=0.5, heartbeat_timeout=10, initial_partitions=2)


class FakePublisher:
    def __init__(self):
        self.sent = []

    def send(self, topic, value, flush=True):
        self.sent.append((topic, value))

    def tasks(self):
        return [v for t, v in self.sent if t == SETTINGS.task_topic]


class FakeAdmin:
    def __init__(self):
        self.grown = []

    def create_partitions(self, spec):
        self.grown.append({t: p.total_count for t, p in spec.items()})


@pytest.fixture
def store():
    return TaskStore(fakeredis.FakeRedis(decode_responses=True))


@pytest.fixture
def pub():
    return FakePublisher()


@pytest.fixture
def client(store, pub):
    return YADTQClient(SETTINGS, publisher=pub, store=store)


def make_worker(store, pub, tasks, worker_id="w1"):
    sleeps = []
    w = YADTQWorker(tasks, SETTINGS, worker_id=worker_id, publisher=pub, store=store, sleep=sleeps.append)
    w.sleeps = sleeps
    return w


def test_submit_records_and_publishes(client, store, pub):
    tid = client.submit("add", 1, 2)
    assert store.get(tid)["status"] == QUEUED
    assert pub.tasks() == [{"task_id": tid, "name": "add", "args": [1, 2], "kwargs": {}}]
    assert client.status("nope")["status"] == "unknown"


def test_success_keeps_result_type(client, store, pub):
    tid = client.submit("add", 1, 2)
    w = make_worker(store, pub, {"add": lambda a, b: a + b})
    w.handle(pub.tasks()[0])
    info = store.get(tid)
    assert info["status"] == SUCCESS and info["result"] == 3 and info["worker"] == "w1"
    assert store.workers()["w1"]["completed"] == 1


def test_retries_with_backoff_then_fails(client, store, pub):
    tid = client.submit("divide", 1, 0)
    w = make_worker(store, pub, {"divide": lambda a, b: a / b})
    for _ in range(SETTINGS.max_retries + 1):
        w.handle(pub.tasks()[-1])
    info = store.get(tid)
    assert info["status"] == FAILED
    assert info["attempts"] == SETTINGS.max_retries + 1
    assert "ZeroDivisionError" in info["error"]
    assert w.sleeps == [0.5, 1.0]  # exponential backoff
    assert len(pub.tasks()) == 1 + SETTINGS.max_retries  # original + republished retries


def test_retry_can_succeed(client, store, pub):
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return "ok"

    tid = client.submit("flaky")
    w = make_worker(store, pub, {"flaky": flaky})
    w.handle(pub.tasks()[0])
    assert store.get(tid)["status"] == QUEUED
    w.handle(pub.tasks()[1])
    info = store.get(tid)
    assert info["status"] == SUCCESS and info["result"] == "ok" and "error" not in info


def test_unknown_task_fails_without_retry(client, store, pub):
    tid = client.submit("missing")
    make_worker(store, pub, {}).handle(pub.tasks()[0])
    assert store.get(tid)["status"] == FAILED
    assert len(pub.tasks()) == 1


def test_duplicate_deliveries_are_skipped(client, store, pub):
    calls = []
    tid = client.submit("add", 1, 2)
    msg = pub.tasks()[0]
    w = make_worker(store, pub, {"add": lambda a, b: calls.append(1) or a + b})
    w.handle(msg)
    w.handle(msg)  # already succeeded
    store.mark_processing(tid, "other")
    make_worker(store, pub, {"add": lambda a, b: calls.append(1)}, "w2").handle(msg)  # running elsewhere
    assert calls == [1]


def test_tracker_requeues_tasks_of_dead_worker(client, store, pub):
    now = [1000.0]
    admin = FakeAdmin()
    tracker = HeartbeatTracker(SETTINGS, consumer=object(), publisher=pub, admin=admin,
                               store=store, clock=lambda: now[0])
    tid = client.submit("add", 1, 2)
    tracker.observe({"worker_id": "w1", "timestamp": now[0]})
    tracker.observe({"worker_id": "w2", "timestamp": now[0]})
    store.mark_processing(tid, "w1")

    tracker.rescale()
    assert admin.grown == [{SETTINGS.task_topic: 4}]  # 2 workers * 2 partitions each

    now[0] += 5
    tracker.observe({"worker_id": "w2", "timestamp": now[0]})
    now[0] += 6  # w1 silent for 11s > timeout, w2 for 6s
    assert tracker.reap() == ["w1"]
    assert store.get(tid)["status"] == QUEUED
    assert pub.tasks()[-1]["task_id"] == tid
    assert store.workers()["w1"]["status"] == "dead"

    # A tracker restarted later still knows w2 and reaps it.
    now[0] += 20
    restarted = HeartbeatTracker(SETTINGS, consumer=object(), publisher=pub, admin=admin,
                                 store=store, clock=lambda: now[0])
    assert restarted.reap() == ["w2"]


def test_tracker_reclaims_from_cleanly_stopped_worker(client, store, pub):
    now = [1000.0]
    tracker = HeartbeatTracker(SETTINGS, consumer=object(), publisher=pub, admin=FakeAdmin(),
                               store=store, clock=lambda: now[0])
    tid = client.submit("add", 1, 2)
    tracker.observe({"worker_id": "w1", "timestamp": now[0]})
    store.mark_processing(tid, "w1")
    store.set_worker_status("w1", "stopped")  # Ctrl+C in the middle of the task
    now[0] += 11
    tracker.reap()
    assert store.workers()["w1"]["status"] == "stopped"
    assert store.get(tid)["status"] == QUEUED
    assert pub.tasks()[-1]["task_id"] == tid


def test_wait_times_out(client):
    tid = client.submit("add", 1, 2)
    with pytest.raises(TaskTimeout):
        client.wait(tid, timeout=0.05, poll=0.01)


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("YADTQ_MAX_RETRIES", "7")
    monkeypatch.setenv("YADTQ_HEARTBEAT_TIMEOUT", "2.5")
    s = Settings.from_env(task_topic="t")
    assert (s.max_retries, s.heartbeat_timeout, s.task_topic) == (7, 2.5, "t")


def test_worker_task_decorator(store, pub):
    w = make_worker(store, pub, {})

    @w.task
    def ping():
        return "pong"

    @w.task(name="renamed")
    def other():
        return 1

    assert set(w.tasks) == {"ping", "renamed"}
    assert PROCESSING  # status constants are exported
