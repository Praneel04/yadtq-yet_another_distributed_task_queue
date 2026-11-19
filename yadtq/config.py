"""Runtime configuration, overridable through ``YADTQ_*`` environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "yadtq"

    task_topic: str = "yadtq.tasks"
    heartbeat_topic: str = "yadtq.heartbeats"
    consumer_group: str = "yadtq-workers"

    # The tracker grows the task topic to max(initial, active_workers * per_worker)
    # partitions. Kafka can only add partitions, never remove them.
    initial_partitions: int = 6
    partitions_per_worker: int = 2

    heartbeat_interval: float = 5.0  # seconds between worker heartbeats
    heartbeat_timeout: float = 15.0  # silence after which a worker is declared dead

    max_retries: int = 3  # retries after the first attempt
    retry_delay: float = 1.0  # base delay; doubles with every retry

    # How long a single task may run before Kafka evicts the worker from the group.
    max_task_seconds: int = 300

    @classmethod
    def from_env(cls, **overrides) -> "Settings":
        """Build settings from defaults, then ``YADTQ_<FIELD>`` env vars, then ``overrides``."""
        values = {}
        for f in fields(cls):
            raw = os.environ.get(f"YADTQ_{f.name.upper()}")
            if raw is not None:
                values[f.name] = type(f.default)(raw)
        values.update(overrides)
        return cls(**values)
