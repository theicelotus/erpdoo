import time
from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from erpdoo.events import Event  # noqa: E402
from erpdoo.replication import Change, Transaction  # noqa: E402
from erpdoo.viewer import Viewer, describe, describe_change  # noqa: E402


def test_update_shows_changed_fields_from_old_row():
    c = Change("update", "sale_order", "sale.order", {"id": 5, "state": "draft", "write_date": "a"}, False,
               {"id": 5, "state": "sale", "write_date": "b"})
    assert describe_change(c)["changed"] == {"state": ["draft", "sale"]}


def test_update_without_old_row_shows_no_changes():
    c = Change("update", "res_partner", "res.partner", None, False, {"id": 10, "name": "New"})
    assert describe_change(c)["changed"] is None


def test_insert_shows_some_values():
    c = Change("insert", "sale_order_line", "sale.order.line", None, False,
               {"id": 1, "name": "Desk", "create_uid": 2, "price_unit": "10.0", "discount": None})
    assert describe_change(c) == {"op": "insert", "table": "sale_order_line", "model": "sale.order.line", "id": 1,
                                  "values": {"name": "Desk", "price_unit": "10.0"}, "more": 0}


def test_page_and_recent_items():
    viewer = Viewer()
    txn = Transaction(900, 0x2000, 0x2100, datetime(2026, 1, 1, tzinfo=timezone.utc),
                      [Change("insert", "crm_lead", "crm.lead", None, False, {"id": 3, "name": "Deal"})])
    event = Event("id-1", "LeadWon", "crm.lead", 3, {"event_type": "LeadWon", "record_id": 3})
    with TestClient(viewer.app) as client:
        assert "erpdoo live" in client.get("/").text
        viewer.publish(describe(txn, [event]))
        for _ in range(50):
            items = client.get("/recent").json()
            if items:
                break
            time.sleep(0.02)
    assert items[0]["xid"] == 900 and items[0]["commit_lsn"] == "0/2000"
    assert items[0]["events"] == [{"event_type": "LeadWon", "record_id": 3}]


def test_field_changes_that_fired_a_rule_link_to_the_event():
    txn = Transaction(901, 0x3000, 0x3100, datetime(2026, 1, 1, tzinfo=timezone.utc), [
        Change("update", "sale_order", "sale.order", {"id": 5, "state": "draft", "note": "a"}, False,
               {"id": 5, "state": "sale", "note": "b"}),
        Change("update", "sale_order_line", "sale.order.line", {"id": 9, "write_date": "x"}, False,
               {"id": 9, "write_date": "y"}),
        Change("update", "sale_order_line", "sale.order.line", {"id": 10, "state": "draft"}, False,
               {"id": 10, "state": "sale"}),
    ])
    event = Event("ev-1", "SaleOrderConfirmed", "sale.order", 5,
                  {"event_id": "ev-1", "changes": {"state": {"from": "draft", "to": "sale"}}})
    item = describe(txn, [event])
    order, internal, line = item["changes"]
    assert order["fires"] == {"state": ["ev-1"]}  # note changed too, but did not fire anything
    assert internal["internal"] and "fires" not in internal
    assert "fires" not in line  # same field name, different record
    assert (item["change_count"], item["table_count"]) == (3, 2)
