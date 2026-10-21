import io
import json
import logging
import re
import struct
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote, urlparse

import fastavro
import pytest

import fake_odoo
from erpdoo.backoff import Unreachable
from erpdoo.config import ConfigError
from erpdoo.events import Event
from erpdoo.schema_registry import Client, Schemas, describe
from erpdoo.schemas import SchemaError
from erpdoo.sinks import DeadLetterSink, DeliveryError, make_sink, prepare
from erpdoo.sinks.kafka import KafkaSink
from test_events import CONFIRMED, builder, order, txn, update

# Messages as Redpanda's registry gives them (verbose=true).
INCOMPATIBLE = [
    "{errorType:\"PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL\", description:\"The new has a closed content model and"
    " is missing a property or item present at path '#/properties/record/properties/amount_total' in the old"
    " schema\"}",
    "{oldSchemaVersion: 1}",
    "{oldSchema: '{\"type\":\"object\"}'}",
    "{compatibility: 'BACKWARD'}",
]


@pytest.fixture
def registry_server():
    """A schema registry with the calls erpdoo makes. Subjects in
    state["refuse"] fail the compatibility check."""
    state = {"subjects": {}, "config": {}, "schemas": [], "refuse": set(), "calls": []}

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def handle_call(self, method):
            url = urlparse(self.path)
            parts = [unquote(p) for p in url.path.strip("/").split("/")]
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or "null")
            state["calls"].append((method, "/".join(parts)))
            versions = state["subjects"].get(parts[-1] if parts[0] != "compatibility" else parts[2])
            if method == "GET" and parts[0] == "config":
                level = state["config"].get(parts[1])
                return self.reply(200, {"compatibilityLevel": level}) if level else self.reply(
                    404, {"error_code": 40408, "message": "no subject-level compatibility"})
            if method == "PUT" and parts[0] == "config":
                state["config"][parts[1]] = body["compatibility"]
                return self.reply(200, body)
            if parts[0] == "compatibility":
                if not versions:
                    return self.reply(404, {"error_code": 40401, "message": "Subject not found."})
                refused = parts[2] in state["refuse"]
                return self.reply(200, {"is_compatible": not refused, "messages": INCOMPATIBLE if refused else []})
            if parts == ["subjects", parts[1], "versions"]:
                state["schemas"].append(body)
                state["subjects"].setdefault(parts[1], []).append(len(state["schemas"]))
                return self.reply(200, {"id": len(state["schemas"])})
            if parts == ["subjects", parts[1]]:
                for schema_id in versions or []:
                    if state["schemas"][schema_id - 1] == body:
                        return self.reply(200, {"id": schema_id, "version": 1})
                return self.reply(404, {"error_code": 40403 if versions else 40401, "message": "not found"})
            self.reply(500, {"message": "unexpected"})

        def do_GET(self):
            self.handle_call("GET")

        def do_POST(self):
            self.handle_call("POST")

        def do_PUT(self):
            self.handle_call("PUT")

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{server.server_port}"
    yield state
    server.shutdown()


class FakeProducerSink(KafkaSink):
    """A KafkaSink without a broker: deliver() keeps the records."""

    def __init__(self, schemas):
        self.schemas = schemas
        self.records = []

    def deliver(self, records):
        self.records.extend(records)
        return [None] * len(records)


def payload(**values):
    [event] = builder().build(txn(update(order(), order(state="sale", **values))))
    return event


def test_registers_new_subjects_with_backward_and_frames_messages(registry_server):
    schemas = Schemas(Client(registry_server["url"]), "json")
    prepare([FakeProducerSink(schemas)], [CONFIRMED], fake_odoo.registry())
    subject = "erpdoo.SaleOrderConfirmed"
    assert registry_server["config"] == {subject: "BACKWARD"}
    assert registry_server["subjects"] == {subject: [1]}
    assert json.loads(registry_server["schemas"][0]["schema"])["title"] == subject
    event = payload()
    value = schemas.serialize(event.event_type, event.payload)
    assert value[:5] == struct.pack(">bI", 0, 1) and json.loads(value[5:]) == event.payload

    # The next start finds the same schema: nothing new is registered.
    again = Schemas(Client(registry_server["url"]), "json")
    prepare([FakeProducerSink(again)], [CONFIRMED], fake_odoo.registry())
    assert registry_server["subjects"] == {subject: [1]} and again.registered["SaleOrderConfirmed"].schema_id == 1


def test_avro_messages_decode_with_the_registered_schema(registry_server):
    schemas = Schemas(Client(registry_server["url"]), "avro", "acme.erpdoo")
    sink = FakeProducerSink(schemas)
    prepare([sink], [CONFIRMED], fake_odoo.registry())
    assert list(registry_server["subjects"]) == ["acme.erpdoo.SaleOrderConfirmed"]
    registered = registry_server["schemas"][0]
    assert registered["schemaType"] == "AVRO"
    event = payload()
    sink.send([event])
    [(key, value, headers)] = sink.records
    assert key == b"sale.order:5" and value[:5] == struct.pack(">bI", 0, 1)
    decoded = fastavro.schemaless_reader(io.BytesIO(value[5:]), fastavro.parse_schema(json.loads(registered["schema"])))
    assert decoded["record"] == event.payload["record"]


def test_a_breaking_change_registers_nothing_and_says_why(registry_server):
    json_sink = FakeProducerSink(Schemas(Client(registry_server["url"]), "json"))
    avro_sink = FakeProducerSink(Schemas(Client(registry_server["url"]), "avro", "erpdoo.avro"))
    prepare([json_sink, avro_sink], [CONFIRMED], fake_odoo.registry())
    smaller = CONFIRMED.__class__(**{**CONFIRMED.__dict__, "fields": ("name",)})
    registry_server["refuse"] = {"erpdoo.SaleOrderConfirmed"}
    with pytest.raises(SchemaError) as error:
        prepare([json_sink, avro_sink], [smaller], fake_odoo.registry())
    message = str(error.value)
    assert ("  SaleOrderConfirmed (subject erpdoo.SaleOrderConfirmed): The new has a closed content model and is"
            " missing a property or item present at path record.amount_total in the old schema") in message
    assert "/config/erpdoo.SaleOrderConfirmed" in message and "oldSchema" not in message
    # Neither sink registered the new schema (the Avro one was compatible).
    assert registry_server["subjects"] == {"erpdoo.SaleOrderConfirmed": [1], "erpdoo.avro.SaleOrderConfirmed": [2]}
    assert json_sink.schemas.registered["SaleOrderConfirmed"].schema_id == 1


def test_a_level_set_by_hand_is_kept(registry_server, caplog):
    registry_server["config"]["erpdoo.SaleOrderConfirmed"] = "NONE"
    with caplog.at_level(logging.WARNING):
        prepare([FakeProducerSink(Schemas(Client(registry_server["url"]), "json"))], [CONFIRMED],
                fake_odoo.registry())
    assert registry_server["config"] == {"erpdoo.SaleOrderConfirmed": "NONE"}
    assert "has compatibility NONE, set by hand" in caplog.text


def test_dead_letter_wrapper_passes_schemas_through(registry_server):
    inner = FakeProducerSink(Schemas(Client(registry_server["url"]), "json"))
    prepare([DeadLetterSink(inner, "kafka", "dlq", "unused", queue=object())], [CONFIRMED], fake_odoo.registry())
    assert "SaleOrderConfirmed" in inner.schemas.registered


def test_an_event_that_does_not_fit_is_failed_not_sent(registry_server):
    sink = FakeProducerSink(Schemas(Client(registry_server["url"]), "avro"))
    prepare([sink], [CONFIRMED], fake_odoo.registry())
    good, bad = payload(), payload()
    bad.payload["record"]["amount_total"] = {"not": "text"}
    other = Event("x", "Unknown", "sale.order", 1, {})
    with pytest.raises(DeliveryError) as error:
        sink.send([good, bad, other])
    assert len(sink.records) == 1
    assert [(e.event_type, re.sub(r":.*", "", reason[7:])) for e, reason in error.value.failed] == [
        ("SaleOrderConfirmed", "SaleOrderConfirmed does not fit its Avro schema"),
        ("Unknown", "no schema registered for Unknown")]


def test_unreachable_registry_is_retried():
    with pytest.raises(Unreachable, match="schema registry http://127.0.0.1:1"):
        Client("http://127.0.0.1:1", timeout=1).compatibility("erpdoo.X")


def test_kafka_sink_options():
    base = {"type": "kafka", "bootstrap_servers": "127.0.0.1:1", "topic": "t"}
    for options, error in [
        ({"format": "avro"}, "format avro needs schema_registry"),
        ({"format": "protobuf"}, "format must be one of json, avro"),
        ({"schema_registry": {"uri": "x"}}, "schema_registry takes namespace, password, url, username"),
        ({"schema_registry": {"url": "x", "namespace": "my-events"}}, "schema namespace 'my-events'"),
    ]:
        with pytest.raises(ConfigError, match=error):
            make_sink({**base, **options})


def test_compatibility_messages_are_readable():
    assert describe(INCOMPATIBLE[0]).endswith("at path record.amount_total in the old schema")
    assert [describe(m) for m in INCOMPATIBLE[1:]] == [None, None, None]
    avro = ("{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field"
            " at path '/fields/1/type/1' in the old schema', additionalInfo:'reader union lacking writer type: LONG'}")
    assert describe(avro) == ("The new schema is missing a type inside a union field at path '/fields/1/type/1' in"
                              " the old schema (reader union lacking writer type: LONG)")
    assert describe("Incompatible because of different schema type") == "Incompatible because of different schema type"
