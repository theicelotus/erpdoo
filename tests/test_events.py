from datetime import datetime, timezone

import fake_odoo
from erpdoo.events import EventBuilder
from erpdoo.replication import Change, Transaction
from erpdoo.rules import parse_rule

CONFIRMED = parse_rule("SaleOrderConfirmed", {
    "model": "sale.order",
    "when": {"state": {"to": "sale"}},
    "fields": ["name", "amount_total"],
    "include": {
        "partner_id": ["name"],
        "order_line": {"fields": ["name", "price_subtotal"], "where": {"display_type": None}},
        "order_line.product_id": ["name", "default_code"],
    },
})


def builder(lang="en_US", rules=(CONFIRMED,)):
    return EventBuilder(list(rules), fake_odoo.registry(), fake_odoo.store(), lang)


def txn(*changes: Change, xid: int = 900) -> Transaction:
    return Transaction(xid, 0x2000, 0x2100, datetime(2026, 1, 1, tzinfo=timezone.utc), list(changes))


def order(**values):
    return {"id": 5, "name": "S00005", "state": "draft", "invoice_status": "no",
            "amount_total": "70.00", "partner_id": 10, **values}


def update(old: dict | None, new: dict) -> Change:
    return Change("update", "sale_order", "sale.order", old, False, new)


def test_confirmed_order_with_related_records():
    [event] = builder().build(txn(update(order(), order(state="sale"))))
    assert event.key == "sale.order:5"
    assert event.payload["changes"] == {"state": {"from": "draft", "to": "sale"}}
    assert event.payload["record"] == {
        "id": 5, "name": "S00005", "amount_total": "70.00",
        "partner_id": {"id": 10, "name": "Azure Interior"},
        # The section line is filtered out by "where".
        "order_line": [{"id": 100, "name": "Office Chair", "price_subtotal": "70.00",
                        "product_id": {"id": 7, "name": "Office Chair", "default_code": "CHAIR"}}],
    }


def test_translated_related_field_uses_language():
    [event] = builder(lang="fr_FR").build(txn(update(order(), order(state="sale"))))
    assert event.payload["record"]["order_line"][0]["product_id"]["name"] == "Chaise de bureau"


def test_several_writes_in_one_transaction_give_one_event():
    t = txn(update(order(), order(state="sale")),
            update(order(state="sale"), order(state="sale", amount_total="80.00")))
    [event] = builder().build(t)
    assert event.payload["record"]["amount_total"] == "80.00"


def test_edit_of_confirmed_order_does_not_fire():
    assert builder().build(txn(update(order(state="sale"), order(state="sale", name="X")))) == []


def test_created_and_confirmed_in_one_transaction():
    t = txn(Change("insert", "sale_order", "sale.order", None, False, order()),
            update(order(), order(state="sale")))
    [event] = builder().build(t)
    assert event.payload["changes"]["state"]["from"] is None


def test_unknown_previous_row_does_not_fire():
    assert builder().build(txn(update(None, order(state="sale")))) == []


def test_missing_related_record_keeps_its_id():
    [event] = builder().build(txn(update(order(partner_id=99), order(state="sale", partner_id=99))))
    assert event.payload["record"]["partner_id"] == {"id": 99}


def test_event_id_is_deterministic():
    change = update(order(), order(state="sale"))
    a = builder().build(txn(change))[0].event_id
    assert a == builder().build(txn(change))[0].event_id
    assert a != builder().build(txn(change, xid=901))[0].event_id


def test_condition_on_related_value_reads_the_store():
    line_rule = parse_rule("ChairLineAdded", {
        "model": "sale.order.line", "on": "create", "when": {"product_id.name": "Office Chair"}})
    line = {"id": 102, "order_id": 5, "name": "x", "display_type": None, "product_id": 7, "price_subtotal": "1"}
    insert = Change("insert", "sale_order_line", "sale.order.line", None, False, line)
    assert [e.event_type for e in builder(rules=[line_rule]).build(txn(insert))] == ["ChairLineAdded"]
    other = Change("insert", "sale_order_line", "sale.order.line", None, False, {**line, "product_id": 8})
    assert builder(rules=[line_rule]).build(txn(other)) == []
