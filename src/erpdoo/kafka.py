"""Kafka sink. send() returns only once the broker has acknowledged every event."""

from __future__ import annotations

import json
import logging

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from erpdoo.events import Event

log = logging.getLogger(__name__)


class DeliveryError(Exception):
    pass


class KafkaSink:
    def __init__(self, bootstrap_servers: str, topic: str, flush_timeout: float = 30.0) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.flush_timeout = flush_timeout
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        })

    def ensure_topic(self, partitions: int = 3) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        # replication_factor=-1 uses the broker's default.
        futures = admin.create_topics([NewTopic(self.topic, num_partitions=partitions, replication_factor=-1)])
        try:
            futures[self.topic].result(timeout=30)
            log.info("created topic %s", self.topic)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

    def send(self, events: list[Event]) -> None:
        errors: list[KafkaError] = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(err)

        for event in events:
            self.producer.produce(
                self.topic,
                key=event.key.encode(),
                value=json.dumps(event.payload).encode(),
                headers=[("event_type", event.event_type.encode()), ("event_id", event.event_id.encode())],
                on_delivery=on_delivery,
            )
        remaining = self.producer.flush(self.flush_timeout)
        if errors or remaining:
            message = f"{len(errors)} failed, {remaining} undelivered: {errors[:3]}"
            # The slot was not acknowledged, so these events are read again
            # after a restart. Drop the queued copies so that close() cannot
            # deliver them as well once the broker is back.
            self.producer.purge()
            self.producer.poll(0)
            raise DeliveryError(message)

    def close(self) -> None:
        self.producer.flush(self.flush_timeout)
