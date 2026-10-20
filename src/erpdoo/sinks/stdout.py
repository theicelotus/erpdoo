"""Stdout sink: one JSON event per line, for debugging and log pipelines."""

from __future__ import annotations

import json
import sys

from erpdoo.events import Event
from erpdoo.sinks import DeliveryError


class StdoutSink:
    def __init__(self, stream=None) -> None:
        self.stream = stream

    def send(self, events: list[Event]) -> None:
        stream = self.stream or sys.stdout
        try:
            for event in events:
                stream.write(json.dumps(event.payload) + "\n")
            stream.flush()
        except OSError as e:  # e.g. a closed pipe
            raise DeliveryError(f"stdout: {e}") from e

    def close(self) -> None:
        pass
