import io

import fastavro
import pytest
from jsonschema import Draft7Validator

import fake_odoo
from erpdoo.models import ModelRegistry
from erpdoo.rules import parse_rule
from erpdoo.schemas import SchemaError, avro_datum, avro_schema, event_shape, json_schema
from test_events import CONFIRMED, builder, order, txn, update


def confirmed_event(**values):
    [event] = builder().build(txn(update(order(), order(state="sale", **values))))
    return event.payload


def errors(schema, payload):
    return [e.message for e in Draft7Validator(schema).iter_errors(payload)]


def avro_round_trip(rule, registry, payload):
    schema = fastavro.parse_schema(avro_schema(rule, registry))
    out = io.BytesIO()
    fastavro.schemaless_writer(out, schema, avro_datum(event_shape(rule, registry), payload))
    out.seek(0)
    return fastavro.schemaless_reader(out, schema)


def as_json(decoded, payload):
    """An Avro-decoded event without the nulls Avro adds for keys the JSON
    payload leaves out."""
    if isinstance(decoded, dict) and isinstance(payload, dict):
        return {k: as_json(v, payload.get(k)) for k, v in decoded.items() if k in payload or v is not None}
    if isinstance(decoded, list) and isinstance(payload, list):
        return [as_json(d, p) for d, p in zip(decoded, payload)]
    return decoded


def test_events_are_valid_against_their_json_schema():
    schema = json_schema(CONFIRMED, fake_odoo.registry())
    Draft7Validator.check_schema(schema)
    assert schema["title"] == "erpdoo.SaleOrderConfirmed"
    assert errors(schema, confirmed_event()) == []
    # A related record not in the state store yet has only its id.
    assert errors(schema, confirmed_event(partner_id=99)) == []


def test_json_schema_catches_wrong_types_and_unknown_fields():
    schema = json_schema(CONFIRMED, fake_odoo.registry())
    payload = confirmed_event()
    payload["record"]["partner_id"]["name"] = False
    payload["record"]["amount_total"] = 70.0  # numeric columns are text
    payload["record"]["surprise"] = 1
    assert sorted(errors(schema, payload)) == [
        "70.0 is not of type 'string', 'null'",
        "Additional properties are not allowed ('surprise' was unexpected)",
        "False is not of type 'string', 'null'",
    ]


def test_events_round_trip_through_avro():
    registry = fake_odoo.registry()
    payload = confirmed_event()
    decoded = avro_round_trip(CONFIRMED, registry, payload)
    assert as_json(decoded, payload) == payload
    # Keys the payload leaves out come back as null.
    assert decoded["source"]["backfill"] is None
    partial = confirmed_event(partner_id=99)
    assert avro_round_trip(CONFIRMED, registry, partial)["record"]["partner_id"] == {"id": 99, "name": None}


def test_avro_rejects_what_the_schema_does_not_have():
    payload = confirmed_event()
    payload["record"]["surprise"] = 1
    with pytest.raises(SchemaError, match=r"event.record: \['surprise'\] not in the schema"):
        avro_datum(event_shape(CONFIRMED, fake_odoo.registry()), payload)


def test_types_follow_the_postgres_column():
    tables = {**fake_odoo.TABLES, "sale_order": {
        "id": "int4", "name": "varchar", "state": "varchar", "invoice_status": "varchar",
        "amount_total": "numeric", "partner_id": "int4", "probability": "float8", "date_order": "timestamp",
        "validity_date": "date", "locked": "bool", "notes": "jsonb"}}
    fields = fake_odoo.FIELDS + [
        fake_odoo.f("sale.order", "probability", "float"), fake_odoo.f("sale.order", "date_order", "datetime"),
        fake_odoo.f("sale.order", "validity_date", "date"), fake_odoo.f("sale.order", "locked", "boolean"),
        fake_odoo.f("sale.order", "notes", "json")]
    registry = ModelRegistry.from_rows(fake_odoo.MODELS, fields, tables)
    rule = parse_rule("Typed", {"model": "sale.order", "when": {"state": {"to": "sale"}}, "fields": [
        "amount_total", "probability", "date_order", "validity_date", "locked", "notes", "partner_id"]})
    record = json_schema(rule, registry)["properties"]["record"]["properties"]
    assert {name: s.get("type") for name, s in record.items()} == {
        "id": "integer", "amount_total": ["string", "null"], "probability": ["number", "null"],
        "date_order": ["string", "null"], "validity_date": ["string", "null"], "locked": ["boolean", "null"],
        "notes": None, "partner_id": ["integer", "null"]}
    payload = {"event_id": "e", "event_type": "Typed", "model": "sale.order", "record_id": 5,
               "occurred_at": "2026-01-01T00:00:00+00:00", "changes": {"state": {"from": "draft", "to": "sale"}},
               "record": {"id": 5, "amount_total": "1500.00", "probability": 100.0,
                          "date_order": "2026-10-21 00:04:04.388605", "validity_date": "2026-11-20",
                          "locked": True, "notes": {"a": [1]}, "partner_id": 10},
               "source": {"table": "sale_order", "xid": 1, "commit_lsn": "0/1"}}
    assert errors(json_schema(rule, registry), payload) == []
    decoded = as_json(avro_round_trip(rule, registry, payload), payload)
    # Avro has no type for any JSON value: it carries the JSON text.
    assert decoded == {**payload, "record": {**payload["record"], "notes": '{"a": [1]}'}}
    bad = {**payload, "record": {**payload["record"], "date_order": "2026-10-21T00:04:04", "amount_total": "1,5"}}
    assert len(errors(json_schema(rule, registry), bad)) == 2


def test_translated_fields_are_text_in_records():
    rule = parse_rule("ProductRenamed", {"model": "product.template", "when": {"name": {"changed": True}},
                                         "fields": ["name"]})
    schema = json_schema(rule, fake_odoo.registry())
    assert schema["properties"]["record"]["properties"]["name"]["type"] == ["string", "null"]
    # "changes" holds the stored values: every language.
    assert "type" not in schema["properties"]["changes"]["properties"]["name"]["properties"]["to"]


def test_backfilled_and_deleted_events_are_valid():
    schema = json_schema(CONFIRMED, fake_odoo.registry())
    payload = confirmed_event()
    payload["source"] = {"table": "sale_order", "backfill": True, "approximate": True,
                         "write_date": "2026-10-21T00:44:03.120000"}
    assert errors(schema, payload) == []
    deleted = parse_rule("SaleOrderDeleted", {"model": "sale.order", "on": "delete", "fields": ["name"]})
    b = builder(rules=[deleted])
    b.store.load("sale_order", [order()], lsn=0x100)
    from erpdoo.replication import Change
    t = txn(Change("delete", "sale_order", "sale.order", {"id": 5}, True, None))
    [event] = b.build(t, b.store.apply_transaction(t))
    assert errors(json_schema(deleted, fake_odoo.registry()), event.payload) == []


def test_usual_rule_edits_only_add_optional_properties():
    registry = fake_odoo.registry()
    wider = parse_rule("SaleOrderConfirmed", {
        "model": "sale.order", "when": {"state": {"to": "sale"}, "invoice_status": {"changed": True}},
        "fields": ["name", "amount_total", "invoice_status"],
        "include": {"partner_id": ["name", "email"], "order_line": ["name", "price_subtotal", "display_type"],
                    "order_line.product_id": ["name", "default_code"], "tag_ids": ["name"]}})
    old, new = json_schema(CONFIRMED, registry), json_schema(wider, registry)

    def required(schema, path=""):
        found = {path: schema.get("required", [])}
        for name, s in schema.get("properties", {}).items():
            found.update(required(s.get("items", s), f"{path}.{name}"))
        return found

    # The same required keys wherever the old schema has them.
    assert {k: v for k, v in required(new).items() if k in required(old)} == required(old)
    # In Avro every added field has a default.
    old_avro, new_avro = avro_schema(CONFIRMED, registry), avro_schema(wider, registry)
    record = {f["name"]: f for f in new_avro["fields"]}["record"]["type"]["fields"]
    assert all("default" in f for f in record if f["name"] != "id")
    assert {f["name"]: f for f in old_avro["fields"]}.keys() == {f["name"]: f for f in new_avro["fields"]}.keys()


def test_avro_needs_valid_names():
    dotted = parse_rule("Dotted", {"model": "sale.order.line", "fields": ["product_id.default_code"]})
    with pytest.raises(SchemaError, match="product_id.default_code in Dotted.record: not a valid Avro field name"):
        avro_schema(dotted, fake_odoo.registry())
    json_schema(dotted, fake_odoo.registry())  # fine in JSON
    with pytest.raises(SchemaError, match="not a valid Avro record name"):
        avro_schema(parse_rule("sale-confirmed", {"model": "sale.order"}), fake_odoo.registry())
