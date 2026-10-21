"""Kafka sink. send() returns only once the broker has acknowledged every event.

Without a schema registry, each message is the event as JSON. With one
(``schema_registry: {url: ...}``), each event type's schema is registered
and messages use the registry's wire format, as JSON Schema
(``format: json``, the default) or Avro (``format: avro``); see
erpdoo.schema_registry.
"""

from __future__ import annotations

import json
import logging

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from erpdoo.backoff import Unreachable
from erpdoo.config import ConfigError
from erpdoo.events import Event
from erpdoo.schemas import FORMATS, SchemaError
from erpdoo.sinks import DeliveryError

log = logging.getLogger(__name__)

# Receives librdkafka's own log lines during check_reachable(), and drops them.
_probe_log = logging.getLogger(f"{__name__}.probe")
_probe_log.addHandler(logging.NullHandler())
_probe_log.propagate = False

# key, value, headers
Record = tuple[bytes, bytes, list[tuple[str, bytes]]]


REGISTRY_OPTIONS = {"url", "namespace", "username", "password"}


class KafkaSink:
    def __init__(
        self, bootstrap_servers: str, topic: str, partitions: int = 3, flush_timeout: float = 30.0,
        format: str = "json", schema_registry: dict | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.flush_timeout = flush_timeout
        self.schemas = schemas(format, schema_registry, topic)
        check_reachable(bootstrap_servers)
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        })
        self.ensure_topic(partitions)

    def ensure_topic(self, partitions: int = 3, topic: str | None = None) -> None:
        """Create the sink's topic (or ``topic``) unless it exists."""
        topic = topic or self.topic
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        # replication_factor=-1 uses the broker's default.
        futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=-1)])
        try:
            futures[topic].result(timeout=30)
            log.info("created topic %s", topic)
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

    def schema_plan(self, rules, registry):
        """Check the schemas of ``rules``; the plan's apply() registers them
        (see erpdoo.sinks.prepare). None without a schema registry."""
        return self.schemas.plan(rules, registry) if self.schemas else None

    def send(self, events: list[Event]) -> None:
        records, sent, failed = [], [], []
        for event in events:
            try:
                value = (self.schemas.serialize(event.event_type, event.payload) if self.schemas
                         else json.dumps(event.payload).encode())
            except SchemaError as e:
                failed.append((event, f"kafka: {e}"))
                continue
            records.append((event.key.encode(), value,
                            [("event_type", event.event_type.encode()), ("event_id", event.event_id.encode())]))
            sent.append(event)
        errors = self.deliver(records)
        # librdkafka reports an error only after its own retries, so these are final.
        failed += [(event, f"kafka: {err.str()}") for event, err in zip(sent, errors) if err is not None]
        if failed:
            raise DeliveryError(f"kafka: {len(failed)} of {len(events)} events rejected, first: {failed[0][1]}",
                                failed=failed)

    def close(self) -> None:
        self.producer.flush(self.flush_timeout)


def schemas(format: str, spec: dict | None, topic: str):
    """The sink's schema registration, or None for plain JSON."""
    if format not in FORMATS:
        raise ConfigError(f"kafka sink {topic}: format must be one of {', '.join(FORMATS)}, not {format!r}")
    if spec is None:
        if format != "json":
            raise ConfigError(f"kafka sink {topic}: format {format} needs schema_registry: {{url: ...}}")
        return None
    if not isinstance(spec, dict) or "url" not in spec or spec.keys() - REGISTRY_OPTIONS:
        raise ConfigError(f"kafka sink {topic}: schema_registry takes {', '.join(sorted(REGISTRY_OPTIONS))}"
                          " (url is required)")
    from erpdoo.schema_registry import Client, Schemas

    try:
        return Schemas(Client(spec["url"], spec.get("username"), spec.get("password")), format,
                       spec.get("namespace", "erpdoo"))
    except SchemaError as e:
        raise ConfigError(f"kafka sink {topic}: {e}") from None


def check_reachable(bootstrap_servers: str, timeout: float = 5.0) -> None:
    """Raise Unreachable unless a broker answers within ``timeout`` seconds.
    librdkafka would print several lines per failed connection; instead its
    first error (e.g. a host name that does not resolve) goes in the message."""
    errors: list[str] = []
    admin = AdminClient({
        "bootstrap.servers": bootstrap_servers,
        "logger": _probe_log,
        "error_cb": lambda err: errors.append(err.str()),
    })
    try:
        admin.list_topics(timeout=timeout)
    except KafkaException:
        admin.poll(0)  # runs error_cb for the errors so far
        detail = next((m for m in errors if "brokers are down" not in m),
                      f"{bootstrap_servers}: no answer within {timeout:g} s")
        raise Unreachable(detail) from None
