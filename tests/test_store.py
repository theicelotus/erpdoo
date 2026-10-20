from datetime import datetime, timezone

from erpdoo.replication import Change, Transaction
from erpdoo.store import StateStore


def store() -> StateStore:
    s = StateStore(":memory:")
    s.load("sale_order_line", [{"id": 1, "order_id": 7, "name": "Desk", "note": "long text"},
                               {"id": 2, "order_id": 7, "name": "Chair", "note": None}], lsn=0x100)
    s.index("order_id")
    return s


def change(op, old=None, new=None, table="sale_order_line", key_only=False):
    return Change(op, table, "sale.order.line", old, key_only, new)


def txn(lsn, *changes):
    """A transaction committed at ``lsn`` (ending 0x10 later)."""
    return Transaction(lsn, lsn, lsn + 0x10, datetime(2026, 1, 1, tzinfo=timezone.utc), list(changes))


def test_insert_update_delete():
    s = store()
    s.apply_transaction(txn(0x200, change("insert", new={"id": 3, "order_id": 8, "name": "Lamp", "note": None})))
    # The new row of an update leaves out unchanged TOASTed columns (here "note").
    s.apply_transaction(txn(0x300, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})))
    s.apply_transaction(txn(0x400, change("delete", old={"id": 2}, key_only=True)))
    assert s.get("sale_order_line", 1) == {"id": 1, "order_id": 7, "name": "Desk XL", "note": "long text"}
    assert s.get("sale_order_line", 2) is None
    assert [r["id"] for r in s.find("sale_order_line", "order_id", 8)] == [3]


def test_changes_get_their_previous_row():
    s = store()
    update = change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})
    delete = change("delete", old={"id": 2}, key_only=True)
    s.apply_transaction(txn(0x200, update, delete))
    assert (update.old, update.old_is_key_only) == ({"id": 1, "order_id": 7, "name": "Desk", "note": "long text"}, False)
    # Postgres sends only the key of a deleted row; the store has the rest.
    assert (delete.old, delete.old_is_key_only) == ({"id": 2, "order_id": 7, "name": "Chair", "note": None}, False)


def test_a_row_sent_by_postgres_is_kept():
    s = store()
    old = {"id": 1, "order_id": 7, "name": "Desk (from WAL)", "note": "long text"}
    update = change("update", old=old, new={"id": 1, "order_id": 7, "name": "Desk XL"})
    s.apply_transaction(txn(0x200, update))
    assert update.old == old


def test_changes_before_the_snapshot_get_no_previous_row():
    # The snapshot may already include them, so the stored row could be the new one.
    s = store()
    update = change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})
    s.apply_transaction(txn(0x80, update))
    assert update.old is None
    assert s.get("sale_order_line", 1)["name"] == "Desk XL"


def test_replayed_transaction_gets_previous_rows_from_versions():
    s = store()
    s.apply_transaction(txn(0x200, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})))
    s.apply_transaction(txn(0x300, change("update", new={"id": 1, "order_id": 7, "name": "Desk XXL"})))
    s.commit()
    # After a crash before the slot was confirmed, both are streamed again.
    first = change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})
    assert s.apply_transaction(txn(0x200, first)) is True
    assert first.old["name"] == "Desk"
    second = change("update", new={"id": 1, "order_id": 7, "name": "Desk XXL"})
    assert s.apply_transaction(txn(0x300, second)) is True
    assert second.old["name"] == "Desk XL"
    # Not applied twice.
    assert s.get("sale_order_line", 1)["name"] == "Desk XXL"


def test_versions_are_dropped_once_confirmed():
    s = store()
    s.apply_transaction(txn(0x200, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})))
    s.prune(0x210)
    replayed = change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})
    assert s.apply_transaction(txn(0x200, replayed)) is True
    assert replayed.old is None  # delivered already; not evaluated again


def test_untracked_tables_are_ignored():
    s = store()
    s.apply_transaction(txn(0x200, change("insert", new={"id": 1}, table="res_partner")))
    assert s.get("res_partner", 1) is None


def test_truncate_and_forget():
    s = store()
    s.apply_transaction(txn(0x200, change("truncate")))
    assert s.find("sale_order_line", "order_id", 7) == []
    assert "sale_order_line" in s.tables
    s.forget("sale_order_line")
    assert "sale_order_line" not in s.tables


def test_find_orders_by_id():
    assert [r["name"] for r in store().find("sale_order_line", "order_id", 7)] == ["Desk", "Chair"]


def test_tracking_survives_reopen(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    s.load("res_partner", [{"id": 1, "name": "Azure Interior"}], lsn=0x100)
    s.apply_transaction(txn(0x200, Change("update", "res_partner", "res.partner", None, False,
                                          {"id": 1, "name": "Azure"})))
    s.commit()
    reopened = StateStore(path)
    assert reopened.tables == {"res_partner"}
    assert reopened.loaded == {"res_partner": 0x100}
    assert reopened.applied_lsn == 0x210
    assert reopened.get("res_partner", 1)["name"] == "Azure"


def test_find_uses_the_column_index():
    # Ordering in SQL made SQLite scan the table via the primary key instead,
    # which got slower with every row (18 ms per lookup at 8000 lines).
    s = StateStore(":memory:")
    s.load("sale_order_line", [{"id": i, "order_id": i // 3} for i in range(3000)])
    s.index("order_id")
    plan = s.db.execute("EXPLAIN QUERY PLAN SELECT id, data FROM records WHERE tbl = ? "
                        "AND json_extract(data, '$.order_id') = ?", ["sale_order_line", 5]).fetchall()
    assert "records_order_id" in plan[0][3]
    assert [r["id"] for r in s.find("sale_order_line", "order_id", 5)] == [15, 16, 17]
