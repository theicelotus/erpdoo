import hashlib
import hmac
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from erpdoo.events import Event
from erpdoo.sinks import DeliveryError, make_sink
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


def test_make_sink_rejects_unknown_type():
    with pytest.raises(ValueError, match="unknown sink type 'ftp'"):
        make_sink({"type": "ftp"})
