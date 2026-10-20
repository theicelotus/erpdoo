"""Event sinks. A sink's send() returns only once every event is delivered,
and raises DeliveryError otherwise; erpdoo confirms the replication slot
only after every sink returned, so delivery is at-least-once."""

from __future__ import annotations

from typing import Protocol

from erpdoo.events import Event


class DeliveryError(Exception):
    pass


class Sink(Protocol):
    def send(self, events: list[Event]) -> None: ...

    def close(self) -> None: ...


def make_sink(spec: dict) -> Sink:
    """Build a sink from one entry of the config's ``sinks`` list."""
    options = dict(spec)
    kind = options.pop("type", None)
    if kind == "kafka":
        from erpdoo.sinks.kafka import KafkaSink

        return KafkaSink(**options)
    if kind == "webhook":
        from erpdoo.sinks.webhook import WebhookSink

        return WebhookSink(**options)
    if kind == "stdout":
        from erpdoo.sinks.stdout import StdoutSink

        return StdoutSink(**options)
    raise ValueError(f"unknown sink type {kind!r}; expected kafka, webhook or stdout")
