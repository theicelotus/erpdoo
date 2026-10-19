import struct
from datetime import datetime, timezone

from erpdoo import pgoutput
from erpdoo.pgoutput import UNCHANGED


def cstr(s: str) -> bytes:
    return s.encode() + b"\0"


def tuple_data(*values) -> bytes:
    out = struct.pack("!h", len(values))
    for v in values:
        if v is None:
            out += b"n"
        elif v is UNCHANGED:
            out += b"u"
        else:
            out += b"t" + struct.pack("!i", len(v.encode())) + v.encode()
    return out


RELATION = (
    b"R" + struct.pack("!I", 16384) + cstr("public") + cstr("sale_order") + b"f" + struct.pack("!h", 3)
    + struct.pack("!b", 1) + cstr("id") + struct.pack("!Ii", 23, -1)
    + struct.pack("!b", 0) + cstr("state") + struct.pack("!Ii", 1043, -1)
    + struct.pack("!b", 0) + cstr("note") + struct.pack("!Ii", 3802, -1)
)


def test_begin_and_commit():
    begin = pgoutput.parse(b"B" + struct.pack("!QqI", 0x1_0000_00A0, 1_000_000, 742))
    assert begin == pgoutput.Begin(0x1_0000_00A0, datetime(2000, 1, 1, 0, 0, 1, tzinfo=timezone.utc), 742)
    commit = pgoutput.parse(b"C" + struct.pack("!bQQq", 0, 0xA0, 0xC8, 0))
    assert (commit.commit_lsn, commit.end_lsn) == (0xA0, 0xC8)
    assert pgoutput.format_lsn(0x1_0000_00A0) == "1/A0"


def test_relation():
    rel = pgoutput.parse(RELATION)
    assert rel.name == "sale_order" and rel.replica_identity == "f"
    assert [(c.name, c.type_oid, c.is_key) for c in rel.columns] == [
        ("id", 23, True), ("state", 1043, False), ("note", 3802, False)]


def test_update_with_full_old_row_and_unchanged_toast():
    rel = pgoutput.parse(RELATION)
    msg = pgoutput.parse(
        b"U" + struct.pack("!I", 16384)
        + b"O" + tuple_data("7", "draft", '{"en_US": "hi"}')
        + b"N" + tuple_data("7", "sale", UNCHANGED)
    )
    assert isinstance(msg, pgoutput.Update) and not msg.old_is_key_only
    assert pgoutput.to_row(rel, msg.old) == {"id": 7, "state": "draft", "note": {"en_US": "hi"}}
    assert pgoutput.to_row(rel, msg.new) == {"id": 7, "state": "sale"}


def test_delete_with_key_only_old_row():
    rel = pgoutput.parse(RELATION)
    msg = pgoutput.parse(b"D" + struct.pack("!I", 16384) + b"K" + tuple_data("7", None, None))
    assert msg.old_is_key_only
    assert pgoutput.to_row(rel, msg.old, key_only=True) == {"id": 7}


def test_insert_and_truncate():
    msg = pgoutput.parse(b"I" + struct.pack("!I", 16384) + b"N" + tuple_data("8", None, None))
    assert msg.new == ("8", None, None)
    trunc = pgoutput.parse(b"T" + struct.pack("!ib", 2, 1) + struct.pack("!II", 1, 2))
    assert trunc == pgoutput.Truncate((1, 2), cascade=True, restart_identity=False)
