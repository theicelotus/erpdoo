"""Kafka sink. send() returns only once the broker has acknowledged every event."""

from __future__ import annotations

import json
import logging

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from erpdoo.events import Event
from erpdoo.sinks import DeliveryError

log = logging.getLogger(__name__)

# key, value, headers
Record = tuple[bytes, bytes, list[tuple[str, bytes]]]


class KafkaSink:
    def __init__(
        self, bootstrap_servers: str, topic: str, partitions: int = 3, flush_timeout: float = 30.0
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.flush_timeout = flush_timeout
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        })
        self.ensure_topic(partitions)

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

    def deliver(self, records: list[Record]) -> list[KafkaError | None]:
        """Write records and wait for the broker. Returns each record's final
        error (None if written). Raises DeliveryError if the broker does not
        answer in time, which is not the fault of any one record."""
        results: list[KafkaError | None] = [None] * len(records)
        for i, (key, value, headers) in enumerate(records):
            def on_delivery(err, _msg, i=i):
                results[i] = err

            try:
                self.producer.produce(self.topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
            except KafkaException as e:  # rejected locally, e.g. message too large
                results[i] = e.args[0]
        remaining = self.producer.flush(self.flush_timeout)
        if remaining:
            # The slot was not acknowledged, so these events are read again
            # after a restart. Drop the queued copies so that close() cannot
            # deliver them as well once the broker is back.
            self.producer.purge()
            self.producer.poll(0)
            raise DeliveryError(f"kafka: {remaining} of {len(records)} records not acknowledged"
                                f" within {self.flush_timeout:g}s")
        return results

    def send(self, events: list[Event]) -> None:
        errors = self.deliver([
            (event.key.encode(), json.dumps(event.payload).encode(),
             [("event_type", event.event_type.encode()), ("event_id", event.event_id.encode())])
            for event in events
        ])
        # librdkafka reports an error only after its own retries, so these are final.
        failed = [(event, f"kafka: {err.str()}") for event, err in zip(events, errors) if err is not None]
        if failed:
            raise DeliveryError(f"kafka: {len(failed)} of {len(events)} events rejected, first: {failed[0][1]}",
                                failed=failed)

    def close(self) -> None:
        self.producer.flush(self.flush_timeout)
