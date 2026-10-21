"""Event sinks. A sink's send() returns only once every event is delivered,
and raises DeliveryError otherwise; erpdoo confirms the replication slot
only after every sink returned, so delivery is at-least-once.

A sink can have a dead-letter topic: events it gives up on are written
there with the error, and the stream continues. Without one, a failed
event stops erpdoo until it can be delivered.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Protocol

from erpdoo.config import ConfigError
from erpdoo.events import Event

log = logging.getLogger(__name__)


class DeliveryError(Exception):
    """A sink could not deliver.

    ``failed`` lists the events the sink gave up on (rejected, or retries
    exhausted) with the reason; events of the batch not in ``failed`` or
    ``remaining`` were delivered. ``remaining`` are events not attempted yet.
    With nothing in ``failed``, the failure is not tied to an event (for
    example the broker is down) and nothing can be dead-lettered.
    """

    def __init__(self, message: str, failed=(), remaining=()) -> None:
        super().__init__(message)
        self.failed: list[tuple[Event, str]] = list(failed)
        self.remaining: list[Event] = list(remaining)


class Sink(Protocol):
    def send(self, events: list[Event]) -> None: ...

    def close(self) -> None: ...


class DeadLetterSink:
    """Wraps a sink: events it gives up on go to a Kafka topic together with
    the error, and the rest of the batch carries on. If the dead-letter topic
    cannot be written either, send() raises, so no event is lost."""

    def __init__(self, sink: Sink, name: str, topic: str, bootstrap_servers: str, queue=None) -> None:
        self.sink = sink
        self.name = name
        self.topic = topic
        if queue is None:
            from erpdoo.sinks.kafka import KafkaSink

            queue = KafkaSink(bootstrap_servers, topic)
        self.queue = queue

    def send(self, events: list[Event]) -> None:
        pending = list(events)
        while pending:
            try:
                self.sink.send(pending)
                return
            except DeliveryError as e:
                if not e.failed:
                    raise
                self._dead_letter(e.failed)
                pending = e.remaining

    def _dead_letter(self, failed: list[tuple[Event, str]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        records = [
            (event.key.encode(),
             json.dumps({"event": event.payload, "error": error, "sink": self.name, "failed_at": now}).encode(),
             [("event_type", event.event_type.encode()), ("event_id", event.event_id.encode()),
              ("error", error.encode())])
            for event, error in failed
        ]
        errors = self.queue.deliver(records)
        if any(err is not None for err in errors):
            raise DeliveryError(f"{self.name}: could not write to dead-letter topic {self.topic}: "
                                f"{next(err for err in errors if err is not None)}")
        for event, error in failed:
            log.warning("%s: sent event %s (%s) to dead-letter topic %s: %s",
                        self.name, event.event_id, event.event_type, self.topic, error)

    def schema_plan(self, rules, registry):
        plan = getattr(self.sink, "schema_plan", None)
        return plan(rules, registry) if plan else None

    def close(self) -> None:
        self.sink.close()
        self.queue.close()


def prepare(sinks: list, rules, registry) -> None:
    """Register the event schemas of ``rules`` for every sink that has a
    schema registry. Every sink's schemas are checked first; if any would
    break compatibility, none is registered and SchemaError says why."""
    from erpdoo.schema_registry import refused
    from erpdoo.schemas import SchemaError

    plans = [plan for sink in sinks if (plan := getattr(sink, "schema_plan", lambda *_: None)(rules, registry))]
    if any(plan.problems for plan in plans):
        raise SchemaError(refused(plans))
    for plan in plans:
        plan.apply()


def make_sink(spec: dict) -> Sink:
    """Build a sink from one entry of the config's ``sinks`` list."""
    options = dict(spec)
    kind = options.pop("type", None)
    dead_letter = options.pop("dead_letter", None)
    try:
        if kind == "kafka":
            from erpdoo.sinks.kafka import KafkaSink

            sink, name = KafkaSink(**options), f"kafka topic {options.get('topic')}"
        elif kind == "webhook":
            from erpdoo.sinks.webhook import WebhookSink

            sink, name = WebhookSink(**options), f"webhook {options.get('url')}"
        elif kind == "stdout":
            from erpdoo.sinks.stdout import StdoutSink

            sink, name = StdoutSink(**options), "stdout"
        else:
            raise ConfigError(f"unknown sink type {kind!r}; expected kafka, webhook or stdout")
    except TypeError as e:  # unknown or missing options
        raise ConfigError(f"sink {kind}: {e}") from None
    if dead_letter is None:
        return sink
    if not isinstance(dead_letter, dict) or not {"topic", "bootstrap_servers"} <= dead_letter.keys():
        raise ConfigError(f"sink {kind}: dead_letter needs topic and bootstrap_servers")
    unknown = dead_letter.keys() - {"topic", "bootstrap_servers"}
    if unknown:
        raise ConfigError(f"sink {kind}: dead_letter has unknown keys {sorted(unknown)}")
    try:
        return DeadLetterSink(sink, name, dead_letter["topic"], dead_letter["bootstrap_servers"])
    except Exception:
        sink.close()  # e.g. Kafka is unreachable and the caller tries again
        raise
