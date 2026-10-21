"""Decoder for PostgreSQL's pgoutput logical replication protocol, version 1.

Message formats:
https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html

Tuple values are kept as the raw text Postgres sends; ``to_row`` converts them
to Python values using the column types from the matching ``Relation``.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class _Unchanged:
    """An unchanged TOASTed column: pgoutput sends a marker instead of the value."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNCHANGED"


UNCHANGED = _Unchanged()

Value = str | None | _Unchanged


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    is_key: bool


@dataclass(frozen=True)
class Relation:
    oid: int
    schema: str
    name: str
    replica_identity: str  # 'd' default, 'n' nothing, 'f' full, 'i' index
    columns: tuple[Column, ...]


@dataclass(frozen=True)
class Begin:
    final_lsn: int
    commit_time: datetime
    xid: int


@dataclass(frozen=True)
class Commit:
    commit_lsn: int
    end_lsn: int
    commit_time: datetime


@dataclass(frozen=True)
class Origin:
    lsn: int
    name: str


@dataclass(frozen=True)
class Type:
    oid: int
    schema: str
    name: str


@dataclass(frozen=True)
class Insert:
    relation_oid: int
    new: tuple[Value, ...]


@dataclass(frozen=True)
class Update:
    relation_oid: int
    # Only sent with REPLICA IDENTITY FULL ('O') or when the key changed ('K').
    old: tuple[Value, ...] | None
    old_is_key_only: bool
    new: tuple[Value, ...]


@dataclass(frozen=True)
class Delete:
    relation_oid: int
    old: tuple[Value, ...]
    old_is_key_only: bool


@dataclass(frozen=True)
class Truncate:
    relation_oids: tuple[int, ...]
    cascade: bool
    restart_identity: bool


Message = Begin | Commit | Origin | Relation | Type | Insert | Update | Delete | Truncate


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def unpack(self, fmt: str) -> tuple:
        s = struct.Struct("!" + fmt)
        values = s.unpack_from(self.data, self.pos)
        self.pos += s.size
        return values

    def byte(self) -> bytes:
        b = self.data[self.pos : self.pos + 1]
        self.pos += 1
        return b

    def string(self) -> str:
        end = self.data.index(b"\0", self.pos)
        s = self.data[self.pos : end].decode()
        self.pos = end + 1
        return s

    def timestamp(self) -> datetime:
        (micros,) = self.unpack("q")
        return PG_EPOCH + timedelta(microseconds=micros)

    def tuple_data(self) -> tuple[Value, ...]:
        (ncols,) = self.unpack("h")
        values: list[Value] = []
        for _ in range(ncols):
            kind = self.byte()
            if kind == b"n":
                values.append(None)
            elif kind == b"u":
                values.append(UNCHANGED)
            elif kind == b"t":
                (length,) = self.unpack("i")
                values.append(self.data[self.pos : self.pos + length].decode())
                self.pos += length
            else:
                raise ValueError(f"unsupported tuple value kind {kind!r}")
        return tuple(values)


def parse(data: bytes) -> Message:
    r = _Reader(data)
    kind = r.byte()
    if kind == b"B":
        (final_lsn,) = r.unpack("Q")
        commit_time = r.timestamp()
        (xid,) = r.unpack("I")
        return Begin(final_lsn, commit_time, xid)
    if kind == b"C":
        _flags, commit_lsn, end_lsn = r.unpack("bQQ")
        return Commit(commit_lsn, end_lsn, r.timestamp())
    if kind == b"O":
        (lsn,) = r.unpack("Q")
        return Origin(lsn, r.string())
    if kind == b"R":
        (oid,) = r.unpack("I")
        schema = r.string() or "pg_catalog"
        name = r.string()
        replica_identity = chr(r.unpack("b")[0])
        (ncols,) = r.unpack("h")
        columns = []
        for _ in range(ncols):
            (flags,) = r.unpack("b")
            col_name = r.string()
            type_oid, _typmod = r.unpack("Ii")
            columns.append(Column(col_name, type_oid, bool(flags & 1)))
        return Relation(oid, schema, name, replica_identity, tuple(columns))
    if kind == b"Y":
        (oid,) = r.unpack("I")
        return Type(oid, r.string() or "pg_catalog", r.string())
    if kind == b"I":
        (oid,) = r.unpack("I")
        if r.byte() != b"N":
            raise ValueError("malformed Insert message")
        return Insert(oid, r.tuple_data())
    if kind == b"U":
        (oid,) = r.unpack("I")
        old = None
        old_is_key_only = False
        marker = r.byte()
        if marker in (b"K", b"O"):
            old_is_key_only = marker == b"K"
            old = r.tuple_data()
            marker = r.byte()
        if marker != b"N":
            raise ValueError("malformed Update message")
        return Update(oid, old, old_is_key_only, r.tuple_data())
    if kind == b"D":
        (oid,) = r.unpack("I")
        marker = r.byte()
        if marker not in (b"K", b"O"):
            raise ValueError("malformed Delete message")
        return Delete(oid, r.tuple_data(), marker == b"K")
    if kind == b"T":
        nrels, options = r.unpack("ib")
        oids = r.unpack(f"{nrels}I")
        return Truncate(tuple(oids), bool(options & 1), bool(options & 2))
    raise ValueError(f"unsupported pgoutput message {kind!r}")


_INT_OIDS = {20, 21, 23, 26}  # int8, int2, int4, oid
_FLOAT_OIDS = {700, 701}  # float4, float8
_JSON_OIDS = {114, 3802}  # json, jsonb
_BOOL_OID = 16


def convert(type_oid: int, text: Value) -> object:
    """Convert one text value to Python. numeric and date/time types stay as
    text so no precision or timezone information is lost."""
    if text is None or text is UNCHANGED:
        return text
    if type_oid in _INT_OIDS:
        return int(text)
    if type_oid == _BOOL_OID:
        # pgoutput sends t/f; bool::text, as snapshots read it, gives true/false.
        return text in ("t", "true")
    if type_oid in _FLOAT_OIDS:
        return float(text)
    if type_oid in _JSON_OIDS:
        return json.loads(text)
    return text


def to_row(relation: Relation, values: tuple[Value, ...], key_only: bool = False) -> dict[str, object]:
    """Map tuple values to column names. Unchanged TOAST values are left out,
    as are non-key columns of a key-only old tuple (Postgres sends them as null)."""
    row = {}
    for col, value in zip(relation.columns, values):
        if value is UNCHANGED or (key_only and not col.is_key):
            continue
        row[col.name] = convert(col.type_oid, value)
    return row


def format_lsn(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"
