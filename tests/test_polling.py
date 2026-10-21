from collections import Counter
from datetime import timezone

import pytest
from jsonschema import Draft7Validator

from erpdoo.polling import Poller, build_registry, column_types, decimal_text, micros, odoo_time
from erpdoo.rules import parse_rule
from erpdoo.schemas import avro_datum, event_shape, json_schema
from erpdoo.store import InboundLog, StateStore
from fake_odoo_api import FMT, FakeApi

SETTINGS = {"interval": 5.0, "overlap": 60.0, "deletes": None, "batch": 2}
CONFIRMED = parse_rule("SaleOrderConfirmed", {
    "model": "sale.order", "when": {"state": {"to": "sale"}}, "fields": ["name", "state", "amount_total"],
    "include": {"partner_id": ["name"], "order_line": ["name", "product_uom_qty"],
                "order_line.product_id": ["name", "default_code"], "tag_ids": ["name"]}})
CREATED = parse_rule("QuotationCreated", {"model": "sale.order", "on": ["create"], "when": {"state": "draft"},
                                          "fields": ["name"]})
WON = parse_rule("LeadWon", {"model": "crm.lead", "when": {"probability": {"to": 100}, "active": True},
                             "fields": ["name", "probability"]})


def poller(api, store=None, rules=(CONFIRMED, CREATED, WON), **settings) -> Poller:
    p = Poller(api, {**SETTINGS, **settings}, store or StateStore(":memory:"))
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


def test_deletes_are_found_by_comparing_ids(api):
    deleted = parse_rule("LeadDeleted", {"model": "crm.lead", "on": ["delete"], "fields": ["name", "probability"]})
    p = poller(api, rules=[WON, deleted], deletes=1e-9)  # look for deletes at every poll
    run(p)
    api.delete("crm.lead", api.lead)
    result = run(p)
    [event] = result.events
    assert (event.event_type, event.record_id) == ("LeadDeleted", api.lead)
    # The values of the last poll; when is not known, only when it was found.
    assert event.payload["record"] == {"id": api.lead, "name": "Big deal", "probability": 10.0}
    assert event.payload["source"] == {"table": "crm_lead", "polling": True, "approximate": True}
    assert [(c.op, c.old["id"]) for c in result.changes] == [("delete", api.lead)]
    assert run(p).events == []
    # Without looking for deletes, delete rules are not used.
    assert [r.name for r in poller(api, rules=[WON, deleted], deletes=None).rules] == ["LeadWon"]


def test_events_caused_by_inbound_messages_are_marked(api, tmp_path):
    path = str(tmp_path / "state.db")
    p = poller(api, StateStore(path))
    run(p)
    inbound, bot = InboundLog(path), 5
    # The integration user wins a lead while erpdoo carries out a message.
    now = micros(api.now.strftime(FMT)) + api.now.microsecond
    attempt = inbound.begin("crm.events", "msg-1", "LeadWinning", bot, now - 100_000)
    api.write("crm.lead", api.lead, {"probability": 100.0}, uid=bot)
    inbound.end(attempt, now + 400_000, ("crm.lead", api.lead))
    [event] = run(p).events
    assert event.payload["source"]["inbound"] == {"message_id": "msg-1", "topic": "crm.events", "rule": "LeadWinning"}
    # Not by the integration user, or not during a message: not marked.
    api.tick(30)
    other = api.create("crm.lead", {"name": "Other", "probability": 10.0, "active": True})
    third = api.create("crm.lead", {"name": "Third", "probability": 10.0, "active": True})
    run(p)
    api.write("crm.lead", other, {"probability": 100.0}, uid=2)
    api.write("crm.lead", third, {"probability": 100.0}, uid=bot)
    events = run(p).events
    assert [(e.record_id, e.payload["source"].get("inbound")) for e in events] == [(other, None), (third, None)]


def test_read_only_check(api):
    p = poller(api)
    assert p.check_read_only() == []
    api.writable = {"sale.order"}
    assert p.check_read_only() == ["sale.order (write, create, unlink)"]


def test_models_changed(api):
    p = poller(api)
    assert not p.models_changed()
    api.fields.append({**api.fields[-1], "id": len(api.fields) + 1, "name": "x_note", "write_date": api.now})
    assert p.models_changed()


def test_an_idle_poll_costs_one_call_per_model_and_one_for_tracking(api):
    # A condition on a related record needs a read, but only for records that changed.
    line_added = parse_rule("ChairLineAdded", {"model": "sale.order.line", "on": ["create"],
                                               "when": {"product_id.default_code": "CHAIR"}, "fields": ["name"]})
    p = poller(api, rules=[CONFIRMED, WON, line_added], batch=500)
    run(p)
    record = order(api)
    api.write("sale.order", record, {"state": "sale"})
    assert {e.event_type for e in run(p).events} == {"SaleOrderConfirmed", "ChairLineAdded"}
    api.tick()
    calls = api.calls
    result = run(p)  # reads the same records again (the overlap), nothing changed
    assert (result.events, result.changes) == ([], [])
    assert api.calls - calls == result.calls == 4  # sale.order, sale.order.line, crm.lead, tracking


def test_a_model_installed_later_is_polled_from_its_creation(api):
    """An app installed while erpdoo polls: changes made before erpdoo
    notices it are backfilled, exactly once, then polling goes on."""
    raised = parse_rule("PriorityRaised", {"model": "crm.lead", "when": {"priority": {"to": "1"}}, "fields": ["name"]})
    api.hidden = {"crm.lead"}
    p = Poller(api, SETTINGS, StateStore(":memory:"))
    p.clock = lambda: api.now.replace(tzinfo=timezone.utc).timestamp()  # Odoo time is UTC
    p.load()
    assert [r.name for r in p.activate([CONFIRMED, raised, WON])] == ["PriorityRaised", "LeadWon"]
    run(p)
    api.tick()
    api.install("crm.lead")
    api.tick()
    lead = api.create("crm.lead", {"name": "Late deal", "probability": 10.0, "active": True, "priority": "0"})
    for priority in ("1", "0", "1"):  # raised twice, tracked
        api.tick()
        api.write("crm.lead", lead, {"priority": priority})
    api.tick()
    api.write("crm.lead", lead, {"probability": 100.0})  # won, not tracked
    api.tick()
    assert p.models_changed()
    p.load()
    assert p.activate([CONFIRMED, raised, WON]) == []
    api.tick()
    api.write("crm.lead", lead, {"priority": "0"})
    api.tick()
    api.write("crm.lead", lead, {"priority": "1"})  # after the activation: polled as usual
    api.tick()
    events = run(p).events
    got = [(e.event_type, e.payload["source"].get("backfill", False), e.payload["source"]["approximate"])
           for e in events]
    # The lead is new to erpdoo, so its win is a create, dated when it was
    # created, as for any record created between two polls.
    assert got == [("LeadWon", True, True), ("PriorityRaised", True, False), ("PriorityRaised", True, False),
                   ("PriorityRaised", False, False)]
    assert events[0].payload["changes"] == {"probability": {"from": None, "to": 100.0}}
    # Ids from the tracking messages, as erpdoo backfill gives them.
    assert len({e.event_id for e in events}) == 4
    api.tick(70)
    assert run(p).events == []  # nothing again, past the overlap too
    api.write("crm.lead", lead, {"priority": "0"})
    api.tick()
    api.write("crm.lead", lead, {"priority": "1"})
    api.tick()
    assert [e.payload["source"].get("backfill", False) for e in run(p).events] == [False]


def test_a_model_installed_in_the_second_polling_started_is_caught_up(api):
    # Odoo gives the install time to the second; polling started later in that second.
    api.now = api.now.replace(microsecond=900000)
    api.hidden = {"crm.lead"}
    p = Poller(api, SETTINGS, StateStore(":memory:"))
    p.clock = lambda: api.now.replace(tzinfo=timezone.utc).timestamp()
    p.load()
    p.activate([CONFIRMED])
    api.now = api.now.replace(microsecond=950000)
    api.install("crm.lead")
    api.create("crm.lead", {"name": "Won at once", "probability": 100.0, "active": True})
    api.tick()
    p.load()
    p.activate([CONFIRMED, WON])
    assert [e.payload["source"].get("backfill") for e in run(p).events] == [True]


def test_a_model_from_before_polling_started_gets_a_baseline(api):
    api.hidden = {"crm.lead"}
    store = StateStore(":memory:")
    p = poller(api, store, rules=[CONFIRMED])
    api.installed["crm.lead"] = api.now.replace(year=2025)  # installed long ago, its rule added now
    api.hidden = set()
    api.write("crm.lead", api.lead, {"probability": 100.0})
    api.tick()
    p.load()
    p.activate([CONFIRMED, WON])
    assert run(p).events == []
