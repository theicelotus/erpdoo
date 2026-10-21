from collections import Counter

import pytest
from jsonschema import Draft7Validator

from erpdoo.polling import Poller, build_registry, column_types, decimal_text, micros, odoo_time
from erpdoo.rules import parse_rule
from erpdoo.schemas import avro_datum, event_shape, json_schema
from erpdoo.store import StateStore
from fake_odoo_api import FakeApi

SETTINGS = {"interval": 5.0, "overlap": 60.0, "deletes": None, "batch": 2}
CONFIRMED = parse_rule("SaleOrderConfirmed", {
    "model": "sale.order", "when": {"state": {"to": "sale"}}, "fields": ["name", "state", "amount_total"],
    "include": {"partner_id": ["name"], "order_line": ["name", "product_uom_qty"],
                "order_line.product_id": ["name", "default_code"], "tag_ids": ["name"]}})
CREATED = parse_rule("QuotationCreated", {"model": "sale.order", "on": ["create"], "when": {"state": "draft"},
                                          "fields": ["name"]})
WON = parse_rule("LeadWon", {"model": "crm.lead", "when": {"probability": {"to": 100}, "active": True},
                             "fields": ["name", "probability"]})


def poller(api, store=None, rules=(CONFIRMED, CREATED, WON)) -> Poller:
    p = Poller(api, SETTINGS, store or StateStore(":memory:"))
    p.load()
    p.activate(list(rules))
    return p


def run(p: Poller):
    """One poll, delivered and noted."""
    result = p.poll()
    p.commit(result)
    return result


@pytest.fixture
def api():
    api = FakeApi()
    api.partner = api.create("res.partner", {"name": "Azure Interior"})
    template = api.create("product.template", {"name": {"en_US": "Office Chair", "fr_FR": "Chaise de bureau"}})
    api.product = api.create("product.product", {"default_code": "CHAIR", "product_tmpl_id": template})
    api.tag = api.create("crm.tag", {"name": "VIP"})
    api.create("sale.order", {"name": "S00001", "state": "sale", "amount_total": 10, "currency_id": 1,
                              "partner_id": api.partner})
    api.lead = api.create("crm.lead", {"name": "Big deal", "probability": 10.0, "active": True})
    api.tick()
    return api


def order(api, name="S00002", lines=1) -> int:
    record = api.create("sale.order", {"name": name, "state": "draft", "amount_total": 1725, "currency_id": 1,
                                       "partner_id": api.partner, "tag_ids": [api.tag]})
    for _ in range(lines):
        api.create("sale.order.line", {"order_id": record, "name": "Chair", "product_id": api.product,
                                       "product_uom_qty": 2.0})
    return record


def test_times_and_decimals():
    assert micros("2026-10-21 08:00:01") == 1792569601_000_000
    assert odoo_time(1792569601_999_999) == "2026-10-21 08:00:01"
    # A numeric column's text, as Postgres gives it: with the field's scale if known.
    assert decimal_text(1725.0, 2) == "1725.00"
    assert decimal_text(750.0, None) == "750.0"
    assert decimal_text(1e-05, None) == "0.00001"


def test_column_types_follow_fields_get():
    # A float with digits is numeric in Postgres (text in events), one without is float8.
    types = column_types({"sale.order.line": {"product_uom_qty": {"type": "float", "digits": [16, 2]}},
                          "crm.lead": {"probability": {"type": "float"}}})
    assert types == {"sale.order.line": {"product_uom_qty": "numeric"}, "crm.lead": {"probability": "float8"}}
    api = FakeApi()
    names = [m["model"] for m in api.call("ir.model", "search_read", [], fields=["model"])]
    registry = build_registry(names, api.fields, {}, types)
    assert registry.models["crm.lead"].fields["probability"].column_type == "float8"
    assert registry.models["sale.order.line"].fields["product_uom_qty"].column_type == "numeric"
    assert registry.models["sale.order"].fields["amount_total"].column_type == "numeric"
    assert registry.models["product.template"].fields["name"].translate
    assert registry.models["sale.order"].table == "sale_order"
    assert registry.models["sale.order"].fields["tag_ids"].relation_table == "sale_order_tag_rel"


def test_baseline_then_tracked_transition(api):
    p = poller(api)
    assert run(p).events == []  # the records before polling started are the baseline
    api.tick(2)
    record = order(api)
    api.write("sale.order", record, {"state": "sale"})
    events = run(p).events
    [event] = [e for e in events if e.event_type == "SaleOrderConfirmed"]
    payload = event.payload
    assert payload["changes"] == {"state": {"from": "draft", "to": "sale"}}
    assert payload["record"] == {
        "id": record, "name": "S00002", "state": "sale", "amount_total": "1725.00",
        "partner_id": {"id": api.partner, "name": "Azure Interior"},
        "order_line": [{"id": 1, "name": "Chair", "product_uom_qty": "2.00",
                        "product_id": {"id": api.product, "name": "Office Chair", "default_code": "CHAIR"}}],
        "tag_ids": [{"id": api.tag, "name": "VIP"}],
    }
    assert payload["source"] == {"table": "sale_order", "polling": True, "approximate": False, "mail_message_id": 1}
    assert payload["occurred_at"] == "2026-10-21T08:00:03+00:00"
    # Created as a quotation: the create rule sees the state before the tracked write.
    [created] = [e for e in events if e.event_type == "QuotationCreated"]
    assert created.payload["source"]["approximate"] is False
    # The overlap is read again on the next polls: nothing is sent twice.
    api.tick(1)
    assert run(p).events == []
    api.tick(30)
    assert run(p).events == []


def test_every_tracked_transition_between_two_polls(api):
    cancelled = parse_rule("SaleOrderCancelled", {"model": "sale.order", "when": {"state": {"to": "cancel"}},
                                                  "fields": ["state"]})
    p = poller(api, rules=[CONFIRMED, cancelled])
    run(p)
    record = order(api)
    api.write("sale.order", record, {"state": "sale"})
    api.write("sale.order", record, {"state": "cancel"})
    events = run(p).events
    assert [(e.event_type, e.payload["changes"]["state"]) for e in events] == [
        ("SaleOrderConfirmed", {"from": "draft", "to": "sale"}),
        ("SaleOrderCancelled", {"from": "sale", "to": "cancel"}),
    ]
    # Each with the values of its own transition.
    assert [e.payload["record"]["state"] for e in events] == ["sale", "cancel"]


def test_tracked_fields_as_of_each_transition(api):
    # Confirmed, then a tracked field of the same record changed again by a
    # later write: the first event has that field as it was then.
    states = parse_rule("Any", {"model": "sale.order", "on": ["update"], "when": {"state": {"changed": True}},
                                "fields": ["state"]})
    p = poller(api, rules=[states])
    run(p)
    record = order(api)
    api.write("sale.order", record, {"state": "sent"})
    api.write("sale.order", record, {"state": "sale"})
    assert [(e.payload["changes"]["state"], e.payload["record"]["state"]) for e in run(p).events] == [
        ({"from": "draft", "to": "sent"}, "sent"), ({"from": "sent", "to": "sale"}, "sale")]


def test_untracked_fields_compare_with_the_previous_poll(api):
    p = poller(api)
    run(p)
    api.tick()
    api.write("crm.lead", api.lead, {"probability": 100.0})
    [event] = run(p).events
    assert event.payload["changes"] == {"probability": {"from": 10.0, "to": 100.0}}
    assert event.payload["source"]["approximate"] is True
    assert event.payload["source"]["write_date"] == "2026-10-21 08:00:02"
    # Won and lost again between two polls: the intermediate state is not seen.
    api.tick()
    api.write("crm.lead", api.lead, {"probability": 10.0})
    run(p)
    api.write("crm.lead", api.lead, {"probability": 100.0})
    api.write("crm.lead", api.lead, {"probability": 20.0})
    assert run(p).events == []


def test_a_poll_not_noted_gives_the_same_events_again(api):
    p = poller(api)
    run(p)
    record = order(api)
    api.write("sale.order", record, {"state": "sale"})
    api.write("crm.lead", api.lead, {"probability": 100.0})
    first = p.poll()  # delivered, then erpdoo stopped before noting it
    again = p.poll()
    assert [e.event_id for e in first.events] == [e.event_id for e in again.events]
    assert len(first.events) == 3
    p.commit(again)
    assert run(p).events == []


def test_restart_resumes_from_the_cursor(api, tmp_path):
    path = str(tmp_path / "state.db")
    p = poller(api, StateStore(path))
    run(p)
    first = order(api, "S00002")
    api.write("sale.order", first, {"state": "sale"})
    assert len(run(p).events) == 2
    p.store.db.close()
    # Stopped: the changes meanwhile are found after the restart, the ones
    # before are not sent again, and no baseline is taken again.
    api.tick(600)
    second = order(api, "S00003")
    api.write("sale.order", second, {"state": "sale"})
    p = poller(api, StateStore(path))
    events = run(p).events
    assert sorted((e.event_type, e.record_id) for e in events) == [
        ("QuotationCreated", second), ("SaleOrderConfirmed", second)]


def test_related_records_are_read_in_batches(api):
    p = poller(api)
    run(p)
    api.log.clear()
    records = [order(api, f"S1{i:04}", lines=3) for i in range(4)]
    for record in records:
        api.write("sale.order", record, {"state": "sale"})
    events = run(p).events
    assert len([e for e in events if e.event_type == "SaleOrderConfirmed"]) == 4
    calls = Counter(api.log)
    # One call per model and level, whatever the number of orders and lines
    # (batch is 2 here, so the 12 lines and the order pages take more).
    assert calls[("res.partner", "read")] == 1
    assert calls[("product.product", "read")] == 1
    assert calls[("product.template", "read")] == 1
    assert calls[("crm.tag", "read")] == 1
    # The four orders' second, read by ids (a page is full of it), then their tag_ids, 2 per call.
    assert calls[("sale.order", "read")] == 3
    assert calls[("sale.order.line", "search_read")] == 2


def test_a_second_with_more_records_than_a_page(api):
    p = poller(api)
    run(p)
    api.tick()
    leads = [api.create("crm.lead", {"name": f"Lead {i}", "probability": 10.0, "active": True}) for i in range(5)]
    run(p)
    for lead in leads:  # five writes in one second, pages of two
        api.write("crm.lead", lead, {"probability": 100.0})
    assert sorted(e.record_id for e in run(p).events) == leads


def test_polled_events_fit_their_schemas(api):
    p = poller(api)
    run(p)
    record = order(api)
    api.write("sale.order", record, {"state": "sale"})
    api.write("crm.lead", api.lead, {"probability": 100.0})
    events = run(p).events
    assert {e.event_type for e in events} == {"SaleOrderConfirmed", "QuotationCreated", "LeadWon"}
    rules = {r.name: r for r in (CONFIRMED, CREATED, WON)}
    for event in events:
        rule = rules[event.event_type]
        validator = Draft7Validator(json_schema(rule, p.registry))
        assert [e.message for e in validator.iter_errors(event.payload)] == []
        avro_datum(event_shape(rule, p.registry), event.payload)  # raises for a key the schema lacks
