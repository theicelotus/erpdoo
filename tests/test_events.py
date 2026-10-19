from datetime import datetime, timezone

from erpdoo.events import sale_order_confirmed
from erpdoo.replication import Change, Transaction


def txn(*changes: Change, xid: int = 900) -> Transaction:
    return Transaction(xid, 0x2000, 0x2100, datetime(2026, 1, 1, tzinfo=timezone.utc), list(changes))


def update(old: dict | None, new: dict) -> Change:
    return Change("update", "sale_order", old, False, new)


def test_draft_to_sale_fires_once():
    t = txn(
        update({"id": 5, "state": "draft", "amount_total": "10.00"}, {"id": 5, "state": "sale", "amount_total": "10.00"}),
        # Odoo writes the order again in the same transaction (computed fields).
        update({"id": 5, "state": "sale", "amount_total": "10.00"}, {"id": 5, "state": "sale", "amount_total": "12.00"}),
    )
    [event] = sale_order_confirmed(t)
    assert event.key == "sale.order:5"
    assert event.payload["changes"] == {"state": {"from": "draft", "to": "sale"}}
    assert event.payload["record"]["amount_total"] == "12.00"


def test_sent_quotation_confirmed_fires():
    [event] = sale_order_confirmed(txn(update({"id": 5, "state": "sent"}, {"id": 5, "state": "sale"})))
    assert event.payload["changes"]["state"]["from"] == "sent"


def test_already_confirmed_order_edit_does_not_fire():
    assert sale_order_confirmed(txn(update({"id": 5, "state": "sale"}, {"id": 5, "state": "sale"}))) == []


def test_created_and_confirmed_in_one_transaction():
    t = txn(
        Change("insert", "sale_order", None, False, {"id": 6, "state": "draft"}),
        update({"id": 6, "state": "draft"}, {"id": 6, "state": "sale"}),
    )
    [event] = sale_order_confirmed(t)
    assert event.payload["changes"]["state"]["from"] is None


def test_unknown_previous_state_does_not_fire():
    assert sale_order_confirmed(txn(update(None, {"id": 5, "state": "sale"}))) == []


def test_event_id_is_deterministic():
    change = update({"id": 5, "state": "draft"}, {"id": 5, "state": "sale"})
    a = sale_order_confirmed(txn(change))[0].event_id
    assert a == sale_order_confirmed(txn(change))[0].event_id
    assert a != sale_order_confirmed(txn(change, xid=901))[0].event_id
