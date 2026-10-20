from erpdoo.replication import Change
from erpdoo.store import StateStore


def store() -> StateStore:
    s = StateStore(":memory:")
    s.load("sale_order_line", [{"id": 1, "order_id": 7, "name": "Desk", "note": "long text"},
                               {"id": 2, "order_id": 7, "name": "Chair", "note": None}])
    s.index("order_id")
    return s


def change(op, old=None, new=None, table="sale_order_line"):
    return Change(op, table, "sale.order.line", old, False, new)


def test_insert_update_delete():
    s = store()
    s.apply(change("insert", new={"id": 3, "order_id": 8, "name": "Lamp", "note": None}))
    # The new row of an update leaves out unchanged TOASTed columns (here "note").
    s.apply(change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"}))
    s.apply(change("delete", old={"id": 2}))
    assert s.get("sale_order_line", 1) == {"id": 1, "order_id": 7, "name": "Desk XL", "note": "long text"}
    assert s.get("sale_order_line", 2) is None
    assert [r["id"] for r in s.find("sale_order_line", "order_id", 8)] == [3]


def test_find_orders_by_id():
    assert [r["name"] for r in store().find("sale_order_line", "order_id", 7)] == ["Desk", "Chair"]


def test_untracked_tables_are_ignored():
    s = store()
    s.apply(change("insert", new={"id": 1}, table="res_partner"))
    assert s.get("res_partner", 1) is None


def test_truncate_and_forget():
    s = store()
    s.apply(change("truncate"))
    assert s.find("sale_order_line", "order_id", 7) == []
    assert "sale_order_line" in s.tables
    s.forget("sale_order_line")
    assert "sale_order_line" not in s.tables


def test_tracking_survives_reopen(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    s.load("res_partner", [{"id": 1, "name": "Azure Interior"}])
    reopened = StateStore(path)
    assert reopened.tables == {"res_partner"}
    assert reopened.get("res_partner", 1)["name"] == "Azure Interior"


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
