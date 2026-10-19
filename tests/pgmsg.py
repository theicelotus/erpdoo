"""Build raw pgoutput messages for tests."""

import struct

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


def begin(xid: int, final_lsn: int = 0x100, micros: int = 0) -> bytes:
    return b"B" + struct.pack("!QqI", final_lsn, micros, xid)


def commit(commit_lsn: int, end_lsn: int, micros: int = 0) -> bytes:
    return b"C" + struct.pack("!bQQq", 0, commit_lsn, end_lsn, micros)


def relation(oid: int, name: str, columns: list[tuple[str, int, bool]], identity: bytes = b"f") -> bytes:
    out = b"R" + struct.pack("!I", oid) + cstr("public") + cstr(name) + identity + struct.pack("!h", len(columns))
    for col_name, type_oid, is_key in columns:
        out += struct.pack("!b", int(is_key)) + cstr(col_name) + struct.pack("!Ii", type_oid, -1)
    return out


def insert(oid: int, *values) -> bytes:
    return b"I" + struct.pack("!I", oid) + b"N" + tuple_data(*values)


def update(oid: int, old: tuple | None, new: tuple, key_only: bool = False) -> bytes:
    out = b"U" + struct.pack("!I", oid)
    if old is not None:
        out += (b"K" if key_only else b"O") + tuple_data(*old)
    return out + b"N" + tuple_data(*new)


SALE_ORDER = relation(16384, "sale_order", [("id", 23, True), ("state", 1043, False), ("note", 3802, False)])
