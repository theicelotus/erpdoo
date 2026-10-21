from datetime import datetime, timezone

from erpdoo.replication import Change, Transaction
from erpdoo.store import StateStore, TableSpec, Xids


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


def test_snapshot_ids_tell_which_transactions_a_copy_has():
    xids = Xids.parse("1000:1005:1002,1004")
    assert str(xids) == "1000:1005:1002,1004"
    assert [x for x in range(998, 1008) if xids.visible(x)] == [998, 999, 1000, 1001, 1003]
    # The stream gives 32-bit ids; the snapshot 64-bit ones, here in a later epoch.
    later = Xids.parse(f"{2**32 + 10}:{2**32 + 20}:")
    assert later.visible(15) and not later.visible(25)
    assert later.visible(2**32 - 5)  # from just before the wraparound


def test_the_snapshot_not_the_wal_position_decides(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    s.load("sale_order_line", [{"id": 1, "order_id": 7, "name": "Desk"}], lsn=0x100, xids=Xids.parse("1000:1005:1002"))
    # Committed before the copy's position, but still committing when the
    # snapshot was taken (1002), so not in the copy: it gets its previous row.
    committing = change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})
    t = Transaction(1002, 0x80, 0x90, datetime(2026, 1, 1, tzinfo=timezone.utc), [committing])
    assert not s.before_copy("sale_order_line", t)
    s.apply_transaction(t)
    assert committing.old == {"id": 1, "order_id": 7, "name": "Desk"}
    # Seen by the snapshot, though streamed after the copy: already in it.
    assert s.before_copy("sale_order_line", Transaction(1001, 0x80, 0x90, t.commit_time, []))
    s.commit()
    reopened = StateStore(path)
    assert reopened.snapshots["sale_order_line"] == Xids.parse("1000:1005:1002")
    assert not reopened.before_copy("sale_order_line", Transaction(1005, 0x200, 0x210, t.commit_time, []))


def test_backfilled_events_wait_until_delivered(tmp_path):
    from erpdoo.events import Event

    path = str(tmp_path / "state.db")
    s = StateStore(path)
    event = Event("e1", "LeadWon", "crm.lead", 4, {"event_id": "e1", "source": {"backfill": True}})
    s.add_pending_events([event, Event("e2", "LeadWon", "crm.lead", 5, {"event_id": "e2"})])
    s.commit()
    s.reset()  # a lost slot keeps them
    [(seq, first), _] = StateStore(path).pending_events()
    assert Event(**first) == event
    s.clear_pending_events([seq])
    assert [e["event_id"] for _, e in StateStore(path).pending_events()] == ["e2"]


def test_events_a_kafka_transaction_delivered_are_forgotten(tmp_path):
    from erpdoo.events import Event

    s = StateStore(str(tmp_path / "state.db"))
    s.add_pending_events([Event(f"e{n}", "LeadWon", "crm.lead", n, {"event_id": f"e{n}"}) for n in (1, 2, 3)])
    s.commit()
    assert s.clear_delivered_events(["e1", "e3", "other"]) == 2
    assert [e["event_id"] for _, e in s.pending_events()] == ["e2"]


def test_the_instance_id_is_kept(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    instance = s.instance
    s.reset()  # e.g. a recreated slot: the same erpdoo, so the same transactional.id
    assert StateStore(path).instance == instance != StateStore(str(tmp_path / "other.db")).instance


def test_streaming_since_is_forgotten_with_the_copy(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    assert s.streaming_since is None
    s.set_streaming_since(datetime(2026, 10, 21, 8, 0, tzinfo=timezone.utc))
    assert StateStore(path).streaming_since == datetime(2026, 10, 21, 8, 0, tzinfo=timezone.utc)
    s.reset()
    assert s.streaming_since is None


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


def test_rows_as_of_an_earlier_position():
    s = store()
    s.apply_transaction(txn(0x200, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})))
    s.apply_transaction(txn(0x300, change("update", new={"id": 1, "order_id": 7, "name": "Desk XXL"})))
    assert s.get("sale_order_line", 1, as_of=0x210)["name"] == "Desk XL"   # after the first
    assert s.get("sale_order_line", 1, as_of=0x110)["name"] == "Desk"      # before both
    assert s.get("sale_order_line", 1, as_of=0x310)["name"] == "Desk XXL"  # now
    assert s.get("sale_order_line", 1)["name"] == "Desk XXL"


def test_find_as_of_an_earlier_position():
    s = store()
    s.apply_transaction(txn(0x200, change("insert", new={"id": 3, "order_id": 7, "name": "Lamp", "note": None}),
                            change("delete", old={"id": 2}, key_only=True),
                            change("update", new={"id": 1, "order_id": 8, "name": "Desk"})))
    assert [r["name"] for r in s.find("sale_order_line", "order_id", 7)] == ["Lamp"]
    # Before that transaction: Lamp did not exist, Chair was not deleted, Desk was on order 7.
    assert [r["name"] for r in s.find("sale_order_line", "order_id", 7, as_of=0x110)] == ["Desk", "Chair"]
    assert s.find("sale_order_line", "order_id", 8, as_of=0x110) == []


def on_demand_store(postgres):
    """A store whose sale_order_line rows are fetched from ``postgres`` (a
    list of rows standing in for the table) when first needed."""
    s = StateStore(":memory:")
    reads = []

    def read(table, column, value):
        reads.append((table, column, value))
        return [{k: v for k, v in row.items() if k in COLUMNS} for row in postgres if row[column] == value]

    s._read_postgres = read
    s.track("sale_order", TableSpec(frozenset({"id", "state"}), False))
    s.track("sale_order_line", TableSpec(COLUMNS, False, frozenset({("order_id", "sale_order")})))
    return s, reads


COLUMNS = frozenset({"id", "order_id", "name"})
POSTGRES = [{"id": 1, "order_id": 7, "name": "Desk", "note": "x"}, {"id": 2, "order_id": 7, "name": "Chair", "note": "y"}]


def test_rows_are_fetched_once_with_the_kept_columns():
    s, reads = on_demand_store(POSTGRES)
    assert s.get("sale_order_line", 1) == {"id": 1, "order_id": 7, "name": "Desk"}  # no "note"
    assert s.get("sale_order_line", 1)["name"] == "Desk"
    assert reads == [("sale_order_line", "id", 1)]
    # From then on the stream keeps the row current.
    s.apply_transaction(txn(0x200, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL", "note": "z"})))
    assert s.get("sale_order_line", 1) == {"id": 1, "order_id": 7, "name": "Desk XL"}
    assert len(reads) == 1


def test_lookups_are_fetched_once_then_follow_the_stream():
    s, reads = on_demand_store(POSTGRES)
    assert [r["name"] for r in s.find("sale_order_line", "order_id", 7)] == ["Desk", "Chair"]
    s.apply_transaction(txn(0x200, change("insert", new={"id": 3, "order_id": 7, "name": "Lamp", "note": None})))
    assert [r["name"] for r in s.find("sale_order_line", "order_id", 7)] == ["Desk", "Chair", "Lamp"]
    assert reads == [("sale_order_line", "order_id", 7)]


def test_records_created_while_tracking_need_no_lookup():
    s, reads = on_demand_store([])
    s.apply_transaction(txn(0x200, Change("insert", "sale_order", "sale.order", None, False, {"id": 8, "state": "draft"}),
                            change("insert", new={"id": 3, "order_id": 8, "name": "Lamp", "note": None})))
    assert [r["name"] for r in s.find("sale_order_line", "order_id", 8)] == ["Lamp"]
    assert reads == []


def test_updates_of_rows_never_fetched():
    s, reads = on_demand_store(POSTGRES)
    # Every kept column is in the new row: kept. Without them all: left to a fetch.
    s.apply_transaction(txn(0x200, change("update", new={"id": 4, "order_id": 9, "name": "Shelf"})))
    s.apply_transaction(txn(0x300, change("update", new={"id": 5, "order_id": 9})))
    assert s._current("sale_order_line", 4) == {"id": 4, "order_id": 9, "name": "Shelf"}
    assert s._current("sale_order_line", 5) is None


def test_changed_columns_start_a_table_over():
    s, _ = on_demand_store(POSTGRES)
    s.get("sale_order_line", 1)
    s.sync("dsn", {"sale_order_line": TableSpec(COLUMNS | {"note"}, False)})
    assert s._current("sale_order_line", 1) is None
    assert s.specs["sale_order_line"].columns == COLUMNS | {"note"}


def test_snapshot_keeps_only_the_kept_columns():
    s = StateStore(":memory:")
    s.load("sale_order", [{"id": 1, "state": "draft", "note": "long"}], lsn=0x100,
           spec=TableSpec(frozenset({"id", "state"}), True))
    assert s.get("sale_order", 1) == {"id": 1, "state": "draft"}
    reopened_spec = s.specs["sale_order"]
    assert reopened_spec.full and reopened_spec.columns == {"id", "state"}


def test_fewer_columns_are_trimmed_in_place():
    # A store that kept whole rows (or more columns) keeps its copy and versions.
    s = store()
    s.apply_transaction(txn(0x200, change("update", new={"id": 1, "order_id": 7, "name": "Desk XL"})))
    s.sync("dsn", {"sale_order_line": TableSpec(frozenset({"id", "order_id", "name"}), True)})
    assert s.loaded["sale_order_line"] == 0x100
    assert s.get("sale_order_line", 1) == {"id": 1, "order_id": 7, "name": "Desk XL"}
    assert s.get("sale_order_line", 1, as_of=0x110) == {"id": 1, "order_id": 7, "name": "Desk"}


def test_a_store_in_an_older_format_starts_over(tmp_path):
    path = str(tmp_path / "state.db")
    s = StateStore(path)
    s.load("res_partner", [{"id": 1, "active": False}], lsn=0x100)
    s.db.execute("DELETE FROM meta WHERE key = 'format'")  # as erpdoo wrote it before formats
    s.db.commit()
    upgraded = StateStore(path)
    assert upgraded.tables == set()
    upgraded.load("res_partner", [{"id": 1, "active": True}], lsn=0x200)
    assert StateStore(path).tables == {"res_partner"}  # only once


def test_inbound_log_and_the_attempt_a_transaction_falls_in(tmp_path):
    from erpdoo.store import InboundLog

    path = str(tmp_path / "state.db")
    store, inbound = StateStore(path), InboundLog(path)
    assert inbound.processed("payments", "m1") is None
    first = inbound.begin("payments", "m1", "PaymentReceived", 8, 1000)
    assert store.inbound_cause(1200) == {"topic": "payments", "message_id": "m1", "rule": "PaymentReceived", "uid": 8}
    inbound.end(first, 1500)
    inbound.finish("payments", "m1", "PaymentReceived", "done", 42)
    assert inbound.processed("payments", "m1") == ("done", 42)
    # Commits before, after and between attempts belong to none.
    assert [store.inbound_cause(lsn) for lsn in (1000, 1600)] == [None, None]
    second = inbound.begin("warehouse", "w1", "DeliveryShipped", 8, 2000)  # still running
    assert store.inbound_cause(2500)["message_id"] == "w1"
    # erpdoo stopped during it: the next attempt closes it.
    inbound.begin("warehouse", "w2", "DeliveryShipped", 8, 3000)
    assert store.inbound_cause(2500)["message_id"] == "w1" and store.inbound_cause(3100)["message_id"] == "w2"
    assert second
    # Once the slot is confirmed past an attempt, it is no longer needed.
    store.prune(2600)
    assert store.inbound_cause(1200) is None and store.inbound_cause(2500)["message_id"] == "w1"
    # reset() (a new slot) keeps the processed messages.
    store.reset()
    assert inbound.processed("payments", "m1") == ("done", 42)
    assert inbound.prune(-10) == 1 and inbound.processed("payments", "m1") is None
