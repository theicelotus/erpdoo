"""Logical replication reader: owns erpdoo's publication and slot, and returns
whole committed transactions.

psycopg 3 does not implement the streaming replication protocol, so changes are
read with the SQL interface: ``pg_logical_slot_peek_binary_changes`` reads
without consuming, and the slot is moved with ``pg_replication_slot_advance``
only after the caller has delivered the resulting events (at-least-once).
After a restart, reading resumes from the slot's confirmed position.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import psycopg
from psycopg import sql

from erpdoo import pgoutput
from erpdoo.pgoutput import format_lsn

log = logging.getLogger(__name__)


@dataclass
class Change:
    op: str  # "insert" | "update" | "delete" | "truncate"
    table: str
    # Row before the change. For updates it is None unless the table has
    # REPLICA IDENTITY FULL (or the key changed, then it holds only key columns).
    old: dict[str, object] | None
    old_is_key_only: bool
    # Row after the change. Unchanged TOASTed columns are left out.
    new: dict[str, object] | None


@dataclass
class Transaction:
    xid: int
    commit_lsn: int
    end_lsn: int
    commit_time: datetime
    changes: list[Change] = field(default_factory=list)


@dataclass
class Batch:
    transactions: list[Transaction]
    # Advance the slot here once every transaction in the batch is delivered.
    ack_lsn: int | None


class ReplicationReader:
    def __init__(self, conn: psycopg.Connection, slot: str, publication: str) -> None:
        if not conn.autocommit:
            raise ValueError("ReplicationReader needs an autocommit connection")
        self.conn = conn
        self.slot = slot
        self.publication = publication

    def setup(self, tables: list[str], replica_identity_full: list[str] = ()) -> None:
        """Create or update the publication and create the slot if missing."""
        table_list = sql.SQL(", ").join(sql.Identifier("public", t) for t in tables)
        pub = sql.Identifier(self.publication)
        exists = self.conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s", [self.publication]
        ).fetchone()
        if exists:
            current = {
                r[0]
                for r in self.conn.execute(
                    "SELECT tablename FROM pg_publication_tables WHERE pubname = %s",
                    [self.publication],
                )
            }
            if current != set(tables):
                self.conn.execute(sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(pub, table_list))
                log.info("updated publication %s: %s", self.publication, ", ".join(tables))
        else:
            self.conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(pub, table_list))
            log.info("created publication %s: %s", self.publication, ", ".join(tables))

        for table in replica_identity_full:
            (ident,) = self.conn.execute(
                "SELECT relreplident FROM pg_class WHERE oid = %s::regclass", [f"public.{table}"]
            ).fetchone()
            if ident != "f":
                self.conn.execute(
                    sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier("public", table))
                )
                log.info("set REPLICA IDENTITY FULL on %s", table)

        row = self.conn.execute(
            "SELECT plugin, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s",
            [self.slot],
        ).fetchone()
        if row is None:
            self.conn.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", [self.slot])
            log.info("created replication slot %s", self.slot)
        elif row[0] != "pgoutput":
            raise RuntimeError(f"slot {self.slot} uses plugin {row[0]}, expected pgoutput")
        else:
            log.info("resuming slot %s from %s", self.slot, row[1])

    def poll(self, max_changes: int = 10_000) -> Batch:
        """Read committed transactions after the slot's confirmed position."""
        # Postgres decodes at least up to this point. If the read is not cut
        # short by max_changes, everything before it has been seen, so the
        # slot can move here even when no published table changed. Without
        # that, the slot would hold back WAL while Odoo writes other tables.
        (flush_lsn,) = self.conn.execute("SELECT pg_current_wal_flush_lsn()::text").fetchone()
        rows = self.conn.execute(
            "SELECT data FROM pg_logical_slot_peek_binary_changes("
            "%s, %s::pg_lsn, %s, 'proto_version', '1', 'publication_names', %s)",
            [self.slot, flush_lsn, max_changes, self.publication],
        ).fetchall()

        relations: dict[int, pgoutput.Relation] = {}
        transactions: list[Transaction] = []
        current: Transaction | None = None
        for (data,) in rows:
            msg = pgoutput.parse(data)
            if isinstance(msg, pgoutput.Begin):
                current = Transaction(msg.xid, msg.final_lsn, 0, msg.commit_time)
            elif isinstance(msg, pgoutput.Commit):
                assert current is not None
                current.commit_lsn = msg.commit_lsn
                current.end_lsn = msg.end_lsn
                transactions.append(current)
                current = None
            elif isinstance(msg, pgoutput.Relation):
                relations[msg.oid] = msg
            elif isinstance(msg, (pgoutput.Insert, pgoutput.Update, pgoutput.Delete, pgoutput.Truncate)):
                assert current is not None
                current.changes.extend(_to_changes(msg, relations))

        if len(rows) < max_changes:
            ack_lsn = max(_parse_lsn(flush_lsn), transactions[-1].end_lsn if transactions else 0)
        else:
            ack_lsn = transactions[-1].end_lsn if transactions else None
        return Batch(transactions, ack_lsn)

    def ack(self, lsn: int) -> None:
        """Confirm everything before ``lsn`` as delivered."""
        self.conn.execute(
            "SELECT pg_replication_slot_advance(slot_name, %s::pg_lsn) FROM pg_replication_slots"
            " WHERE slot_name = %s AND confirmed_flush_lsn < %s::pg_lsn",
            [format_lsn(lsn), self.slot, format_lsn(lsn)],
        )


def _to_changes(msg, relations: dict[int, pgoutput.Relation]) -> list[Change]:
    if isinstance(msg, pgoutput.Truncate):
        return [Change("truncate", relations[oid].name, None, False, None) for oid in msg.relation_oids]
    rel = relations[msg.relation_oid]
    if isinstance(msg, pgoutput.Insert):
        return [Change("insert", rel.name, None, False, pgoutput.to_row(rel, msg.new))]
    if isinstance(msg, pgoutput.Update):
        old = pgoutput.to_row(rel, msg.old, msg.old_is_key_only) if msg.old is not None else None
        return [Change("update", rel.name, old, msg.old_is_key_only, pgoutput.to_row(rel, msg.new))]
    old = pgoutput.to_row(rel, msg.old, msg.old_is_key_only)
    return [Change("delete", rel.name, old, msg.old_is_key_only, None)]


def _parse_lsn(text: str) -> int:
    hi, lo = text.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)
