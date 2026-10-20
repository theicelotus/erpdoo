import io
import json

import pytest

from erpdoo.events import Event
from erpdoo.sinks import make_sink
from erpdoo.sinks.stdout import StdoutSink


def event(n: int) -> Event:
    return Event(f"id-{n}", "SaleOrderConfirmed", "sale.order", n, {"event_id": f"id-{n}", "record_id": n})


def test_stdout_writes_json_lines():
    out = io.StringIO()
    StdoutSink(out).send([event(1), event(2)])
    assert [json.loads(line)["record_id"] for line in out.getvalue().splitlines()] == [1, 2]


def test_make_sink_rejects_unknown_type():
    with pytest.raises(ValueError, match="unknown sink type 'ftp'"):
        make_sink({"type": "ftp"})
