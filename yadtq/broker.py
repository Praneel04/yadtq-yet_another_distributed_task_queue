"""Kafka plumbing: topic administration, producers and round-robin publishing."""

from __future__ import annotations

import itertools
import json
import logging
import threading

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewPartitions, NewTopic
from kafka.errors import InvalidPartitionsError, TopicAlreadyExistsError

from .config import Settings

log = logging.getLogger(__name__)

# Refresh metadata often so producers and consumers notice new partitions quickly.
METADATA_MAX_AGE_MS = 10_000


def _dumps(value) -> bytes:
    return json.dumps(value).encode("utf-8")


def _loads(raw: bytes):
    return json.loads(raw.decode("utf-8"))


def make_admin(settings: Settings) -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)


def ensure_topics(settings: Settings, admin: KafkaAdminClient | None = None) -> None:
    """Create the task and heartbeat topics if they do not exist yet."""
    own = admin is None
    admin = admin or make_admin(settings)
    try:
        for name, partitions in (
            (settings.task_topic, settings.initial_partitions),
            (settings.heartbeat_topic, 1),
        ):
            try:
                admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
                log.info("created topic %s with %d partitions", name, partitions)
            except TopicAlreadyExistsError:
                pass
    finally:
        if own:
            admin.close()


def grow_partitions(admin: KafkaAdminClient, topic: str, total: int) -> bool:
    """Grow ``topic`` to ``total`` partitions. Returns False if it already has that many."""
    try:
        admin.create_partitions({topic: NewPartitions(total_count=total)})
    except InvalidPartitionsError:
        return False
    log.info("grew topic %s to %d partitions", topic, total)
    return True


def make_producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=_dumps,
        metadata_max_age_ms=METADATA_MAX_AGE_MS,
        acks="all",
    )


def consumer_config(settings: Settings) -> dict:
    return dict(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=_loads,
        metadata_max_age_ms=METADATA_MAX_AGE_MS,
    )


class RoundRobinPublisher:
    """Sends each message to the next partition of a topic in turn.

    Picking the partition ourselves (instead of a ``partitioner=`` plugin) works
    the same on every kafka-python version, whose partitioner interface has changed.
    """

    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self._counter = itertools.count()
        self._lock = threading.Lock()

    def send(self, topic: str, value: dict, flush: bool = True) -> None:
        partitions = sorted(self.producer.partitions_for(topic) or [])
        with self._lock:
            n = next(self._counter)
        partition = partitions[n % len(partitions)] if partitions else None
        self.producer.send(topic, value, partition=partition)
        if flush:
            self.producer.flush()
