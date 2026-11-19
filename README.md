# YADTQ: Yet Another Distributed Task Queue

A small distributed task queue in Python. **Kafka** is the message broker, **Redis** is the
result backend. Clients submit tasks, a pool of workers runs them, and a heartbeat tracker
detects crashed workers and requeues their tasks.

Built as a Big Data course project (RR Team 52), restructured into an installable package.

## Features

- **Load balancing:** tasks are published round-robin across partitions and spread over
  workers with Kafka's round-robin partition assignor.
- **Retries with backoff:** a failing task is retried up to `max_retries` times
  (delays of 1s, 2s, 4s, and so on), then marked `failed` with the error.
- **Fault tolerance:** workers send heartbeats. If a worker goes silent, the tracker marks it
  `dead` and republishes its in-flight tasks. Offsets are committed only after a task
  finishes, so a crash never loses a task.
- **Duplicate protection:** delivery is at-least-once. A worker skips a task that is already
  finished or running on another live worker.
- **Elastic partitions:** the tracker grows the task topic to
  `max(initial_partitions, active_workers × partitions_per_worker)` as workers join.
- **Monitoring:** a live terminal dashboard of workers and recent tasks, plus a CLI.

## Architecture

```
            submit()                         poll / commit
 client ───────────────►  Kafka  ───────────────────────────►  worker 1..N
   │      yadtq.tasks   (N partitions)   ◄── retries ──────────    │  │
   │                                                               │  │ heartbeat
   │                       yadtq.heartbeats  ◄─────────────────────┘  │ every 5s
   │                             │                                    │
   │                             ▼                                    │
   │                          tracker ──── requeue dead workers' ──►  │
   │                             │          tasks, grow partitions    │
   ▼                             ▼                                    ▼
 status() ◄──────────────────  Redis: task state, results, worker registry
```

Task lifecycle: `queued → processing → success | failed`. A failed attempt that will be
retried goes back to `queued`.

## Quick start

Requires Python 3.9+ and Docker.

```bash
docker compose up -d              # Kafka (KRaft) on :9092, Redis on :6379
pip install -e ".[dev]"

yadtq tracker                     # terminal 1: run exactly one
yadtq worker                      # terminals 2..N: as many as you like
yadtq dashboard                   # optional live view

python examples/demo_client.py    # submit a batch of demo tasks
yadtq submit add 1 2 --wait       # or one task from the CLI
```

`python -m yadtq ...` works the same as `yadtq ...`.

## Using it from Python

```python
from yadtq import YADTQClient

client = YADTQClient()
task_id = client.submit("add", 1, 2)
print(client.wait(task_id, timeout=30))
# {'task_id': '...', 'status': 'success', 'result': 3, 'attempts': 0, 'worker': '...', ...}
```

Define tasks as a dict of plain functions and point a worker at the module:

```python
# mytasks.py
def resize(path, width):
    ...
    return new_path

TASKS = {"resize": resize}
```

```bash
yadtq worker --tasks mytasks          # looks up mytasks.TASKS
yadtq worker --tasks mytasks:REGISTRY # or a different attribute name
```

Arguments and results must be JSON-serializable. You can also embed a worker and register
tasks with `@worker.task`:

```python
from yadtq import YADTQWorker

worker = YADTQWorker()

@worker.task
def add(a, b):
    return a + b

worker.run()
```

## Configuration

Every setting in [`yadtq/config.py`](yadtq/config.py) can be overridden with a
`YADTQ_<NAME>` environment variable:

| Variable | Default | Meaning |
|---|---|---|
| `YADTQ_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers (or `--kafka`) |
| `YADTQ_REDIS_URL` | `redis://localhost:6379/0` | Redis backend (or `--redis`) |
| `YADTQ_TASK_TOPIC` | `yadtq.tasks` | topic tasks are published to |
| `YADTQ_INITIAL_PARTITIONS` | `6` | partitions the task topic starts with |
| `YADTQ_PARTITIONS_PER_WORKER` | `2` | partitions added per active worker |
| `YADTQ_HEARTBEAT_INTERVAL` | `5` | seconds between worker heartbeats |
| `YADTQ_HEARTBEAT_TIMEOUT` | `15` | silence before a worker is declared dead |
| `YADTQ_MAX_RETRIES` | `3` | retries after the first attempt |
| `YADTQ_RETRY_DELAY` | `1` | base retry delay in seconds; doubles each retry |
| `YADTQ_MAX_TASK_SECONDS` | `300` | longest a single task may run |
| `YADTQ_DEMO_DELAY` | `3` | simulated work time in `examples/tasks.py` |

## Project layout

```
yadtq/
  config.py     Settings (dataclass + env vars)
  broker.py     Kafka topics, producer, round-robin publisher
  store.py      Redis task state and worker registry
  client.py     YADTQClient: submit / status / wait
  worker.py     YADTQWorker: consume, execute, retry, heartbeat
  tracker.py    HeartbeatTracker: dead-worker detection, requeue, partition scaling
  dashboard.py  rich live dashboard
  cli.py        `yadtq` command
examples/       demo tasks and client
tests/          unit tests (no Kafka/Redis needed: fakeredis + fakes)
```

## Tests

```bash
pytest -q
```

The unit tests cover retries and backoff, unknown tasks, duplicate deliveries, dead-worker
requeueing, tracker restarts, and partition scaling. They also run in GitHub Actions.

## Changes from the original version

- Split the single `yadtq.py` into a package with a CLI, and removed dead and commented-out
  code: the broken `monitor.py`/`worker_monitor.py` (they used `_init_` and imported a
  nonexistent class) and the duplicate monitoring in `report.py`.
- Replaced the removed `kafka.partitioner.roundrobin` import, which no longer exists in
  kafka-python, with explicit round-robin publishing.
- Requeue of dead workers' tasks used to fail: workers never recorded themselves on a task,
  and the client didn't store the task name and args. Both are stored now.
- Each worker now tracks its own in-flight tasks, so the tracker no longer scans every Redis key.
- Keys are namespaced (`yadtq:*`), and results keep their JSON type (`3`, not `"3"`).
- Unknown task names fail immediately instead of being retried.
- Removed the Kafka admin connection that opened at import time and the `describe_topics`
  call made every second. Partitions are now grown only when the worker count changes.
- Heartbeat threads are daemonized, and SIGTERM/Ctrl+C shut workers down cleanly.
- Added Docker Compose, `pyproject.toml`, tests, CI, and `.gitignore`. Removed the
  committed `worker.log`.
