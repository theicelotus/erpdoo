import hashlib
import hmac
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from erpdoo.events import Event
from erpdoo.config import ConfigError
from erpdoo.sinks import DeadLetterSink, DeliveryError, make_sink
from erpdoo.sinks.stdout import StdoutSink
from erpdoo.sinks.webhook import WebhookSink


def event(n: int) -> Event:
    return Event(f"id-{n}", "SaleOrderConfirmed", "sale.order", n, {"event_id": f"id-{n}", "record_id": n})


@pytest.fixture
def receiver():
    """A local HTTP server answering with scripted status codes (then 200)."""
    state = {"statuses": [], "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            state["requests"].append((dict(self.headers), body))
            status = state["statuses"].pop(0) if state["statuses"] else 200
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{server.server_port}/events"
    yield state
    server.shutdown()


def sink(url, **kwargs):
    return WebhookSink(url, sleep=lambda s: None, **kwargs)


def test_posts_each_event_in_order_with_headers(receiver):
    sink(receiver["url"], headers={"Authorization": "Bearer t"}).send([event(1), event(2)])
    assert [json.loads(body)["record_id"] for _, body in receiver["requests"]] == [1, 2]
    headers = receiver["requests"][0][0]
    assert headers["Content-Type"] == "application/json"
    assert headers["X-Erpdoo-Event-Id"] == "id-1"
    assert headers["Authorization"] == "Bearer t"


def test_signature(receiver):
    sink(receiver["url"], secret="s3cret").send([event(1)])
    headers, body = receiver["requests"][0]
    assert headers["X-Erpdoo-Signature"] == "sha256=" + hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()


def test_retries_server_errors_then_succeeds(receiver):
    receiver["statuses"] = [500, 503, 429]
    sink(receiver["url"], retries=3).send([event(1)])
    assert len(receiver["requests"]) == 4


def test_gives_up_after_retries(receiver):
    receiver["statuses"] = [500] * 10
    with pytest.raises(DeliveryError, match="failed after 3 attempts: HTTP 500"):
        sink(receiver["url"], retries=2).send([event(1)])


def test_client_error_is_not_retried(receiver):
    receiver["statuses"] = [400]
    with pytest.raises(DeliveryError, match="rejected event id-1: HTTP 400"):
        sink(receiver["url"]).send([event(1), event(2)])
    assert len(receiver["requests"]) == 1  # the second event is not sent past a failed one


def test_unreachable_receiver_raises():
    with pytest.raises(DeliveryError, match="failed after 2 attempts"):
        sink("http://127.0.0.1:9/events", retries=1, timeout=1).send([event(1)])


def test_stdout_writes_json_lines():
    out = io.StringIO()
    StdoutSink(out).send([event(1), event(2)])
    assert [json.loads(line)["record_id"] for line in out.getvalue().splitlines()] == [1, 2]


def test_make_sink_rejects_bad_config():
    with pytest.raises(ConfigError, match="unknown sink type 'ftp'"):
        make_sink({"type": "ftp"})
    with pytest.raises(ConfigError, match="sink webhook: .*urll"):
        make_sink({"type": "webhook", "urll": "x"})
    with pytest.raises(ConfigError, match="dead_letter needs topic and bootstrap_servers"):
        make_sink({"type": "stdout", "dead_letter": {"topic": "dlq"}})


def test_webhook_failure_names_the_event_and_what_is_left(receiver):
    receiver["statuses"] = [200, 400]
    with pytest.raises(DeliveryError) as e:
        sink(receiver["url"]).send([event(1), event(2), event(3)])
    assert [ev.record_id for ev, _ in e.value.failed] == [2]
    assert "HTTP 400" in e.value.failed[0][1]
    assert [ev.record_id for ev in e.value.remaining] == [3]


class FakeSink:
    """Fails the events whose record id is in ``reject`` (one at a time, like
    the webhook sink), or everything if ``down``."""

    def __init__(self, reject=(), down=False):
        self.reject, self.down, self.delivered = set(reject), down, []

    def send(self, events):
        if self.down:
            raise DeliveryError("unreachable")
        for i, ev in enumerate(events):
            if ev.record_id in self.reject:
                raise DeliveryError("rejected", failed=[(ev, f"HTTP 400 for {ev.record_id}")], remaining=events[i + 1:])
            self.delivered.append(ev.record_id)

    def close(self):
        pass


class FakeQueue:
    def __init__(self, fail=False):
        self.records, self.fail = [], fail

    def deliver(self, records):
        self.records.extend(records)
        return ["broker error" if self.fail else None for _ in records]

    def close(self):
        pass


def test_dead_letter_takes_failed_events_and_continues():
    inner, queue = FakeSink(reject={2, 4}), FakeQueue()
    DeadLetterSink(inner, "webhook x", "dlq", "unused", queue=queue).send([event(n) for n in range(1, 6)])
    assert inner.delivered == [1, 3, 5]
    letters = [json.loads(value) for _, value, _ in queue.records]
    assert [(d["event"]["record_id"], d["error"], d["sink"]) for d in letters] == [
        (2, "HTTP 400 for 2", "webhook x"), (4, "HTTP 400 for 4", "webhook x")]


def test_dead_letter_still_blocks_when_no_event_is_at_fault():
    with pytest.raises(DeliveryError, match="unreachable"):
        DeadLetterSink(FakeSink(down=True), "kafka", "dlq", "unused", queue=FakeQueue()).send([event(1)])


def test_dead_letter_blocks_if_the_dead_letter_topic_fails():
    with pytest.raises(DeliveryError, match="could not write to dead-letter topic dlq"):
        DeadLetterSink(FakeSink(reject={1}), "webhook x", "dlq", "unused", queue=FakeQueue(fail=True)).send([event(1)])


def test_unreachable_kafka_names_the_cause():
    from erpdoo.backoff import Unreachable
    from erpdoo.sinks.kafka import check_reachable

    with pytest.raises(Unreachable, match="127.0.0.1:1.*Connection refused"):
        check_reachable("127.0.0.1:1", timeout=1)
