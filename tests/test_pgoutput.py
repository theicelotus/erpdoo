import struct
from datetime import datetime, timezone

from erpdoo import pgoutput
from erpdoo.pgoutput import UNCHANGED
from pgmsg import SALE_ORDER, begin, commit, insert, tuple_data, update


def test_begin_and_commit():
    msg = pgoutput.parse(begin(742, final_lsn=0x1_0000_00A0, micros=1_000_000))
    assert msg == pgoutput.Begin(0x1_0000_00A0, datetime(2000, 1, 1, 0, 0, 1, tzinfo=timezone.utc), 742)
    msg = pgoutput.parse(commit(0xA0, 0xC8))
    assert (msg.commit_lsn, msg.end_lsn) == (0xA0, 0xC8)
    assert pgoutput.format_lsn(0x1_0000_00A0) == "1/A0"


def test_relation():
    rel = pgoutput.parse(SALE_ORDER)
    assert rel.name == "sale_order" and rel.replica_identity == "f"
    assert [(c.name, c.type_oid, c.is_key) for c in rel.columns] == [
        ("id", 23, True), ("state", 1043, False), ("note", 3802, False)]


def test_update_with_full_old_row_and_unchanged_toast():
    rel = pgoutput.parse(SALE_ORDER)
    msg = pgoutput.parse(update(16384, ("7", "draft", '{"en_US": "hi"}'), ("7", "sale", UNCHANGED)))
    assert isinstance(msg, pgoutput.Update) and not msg.old_is_key_only
    assert pgoutput.to_row(rel, msg.old) == {"id": 7, "state": "draft", "note": {"en_US": "hi"}}
    assert pgoutput.to_row(rel, msg.new) == {"id": 7, "state": "sale"}


def test_delete_with_key_only_old_row():
    rel = pgoutput.parse(SALE_ORDER)
    msg = pgoutput.parse(b"D" + struct.pack("!I", 16384) + b"K" + tuple_data("7", None, None))
    assert msg.old_is_key_only
    assert pgoutput.to_row(rel, msg.old, key_only=True) == {"id": 7}


def test_insert_and_truncate():
    assert pgoutput.parse(insert(16384, "8", None, None)).new == ("8", None, None)
    trunc = pgoutput.parse(b"T" + struct.pack("!ib", 2, 1) + struct.pack("!II", 1, 2))
    assert trunc == pgoutput.Truncate((1, 2), cascade=True, restart_identity=False)
