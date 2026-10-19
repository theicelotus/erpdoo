"""Logical replication reader: owns erpdoo's publication and slot, streams
pgoutput over the replication protocol and returns whole committed transactions.

The slot's confirmed position only moves when the caller calls ack(), which it
does after the resulting events are delivered (at-least-once). After a restart,
streaming resumes from the slot's confirmed position, so anything delivered but
not yet acknowledged is sent again.
"""

from __future__ import annotations

import logging
import select
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from erpdoo import pgoutput

log = logging.getLogger(__name__)


@dataclass
class Change:
    op: str  # "insert" | "update" | "delete" | "truncate"
    table: str
    model: str | None  # Odoo model name, if the table belongs to one
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


class TransactionAssembler:
    """Builds committed transactions from a stream of pgoutput messages.

    Postgres sends each Relation message once per streaming session, so the
    relation cache lives as long as the session.
    """

    def __init__(self, resolve_model: Callable[[str], str | None] = lambda table: None) -> None:
        self.resolve_model = resolve_model
        self.relations: dict[int, pgoutput.Relation] = {}
        self.current: Transaction | None = None

    @property
    def in_transaction(self) -> bool:
        return self.current is not None

    def feed(self, data: bytes) -> Transaction | None:
        """Process one message; return the transaction it completes, if any."""
        msg = pgoutput.parse(data)
        if isinstance(msg, pgoutput.Begin):
            self.current = Transaction(msg.xid, msg.final_lsn, 0, msg.commit_time)
        elif isinstance(msg, pgoutput.Commit):
            txn, self.current = self.current, None
            assert txn is not None, "Commit without Begin"
            txn.commit_lsn = msg.commit_lsn
            txn.end_lsn = msg.end_lsn
            return txn
        elif isinstance(msg, pgoutput.Relation):
            self.relations[msg.oid] = msg
        elif isinstance(msg, (pgoutput.Insert, pgoutput.Update, pgoutput.Delete, pgoutput.Truncate)):
            assert self.current is not None, "row change outside a transaction"
            self.current.changes.extend(self._to_changes(msg))
        return None

    def _change(self, op, rel, old, old_is_key_only, new) -> Change:
        return Change(op, rel.name, self.resolve_model(rel.name), old, old_is_key_only, new)

    def _to_changes(self, msg) -> list[Change]:
        if isinstance(msg, pgoutput.Truncate):
            return [self._change("truncate", self.relations[oid], None, False, None) for oid in msg.relation_oids]
        rel = self.relations[msg.relation_oid]
        if isinstance(msg, pgoutput.Insert):
            return [self._change("insert", rel, None, False, pgoutput.to_row(rel, msg.new))]
        if isinstance(msg, pgoutput.Update):
            old = pgoutput.to_row(rel, msg.old, msg.old_is_key_only) if msg.old is not None else None
            return [self._change("update", rel, old, msg.old_is_key_only, pgoutput.to_row(rel, msg.new))]
        old = pgoutput.to_row(rel, msg.old, msg.old_is_key_only)
        return [self._change("delete", rel, old, msg.old_is_key_only, None)]


class PublicationError(Exception):
    pass


def _replica_identity_problem(cur, table: str) -> str | None:
    """Why ``table`` cannot be published safely, or None if it can.

    A published table must identify the rows of an UPDATE or DELETE: by its
    primary key (REPLICA IDENTITY DEFAULT), a chosen unique index (USING
    INDEX) or the whole row (FULL). Otherwise Postgres rejects those
    statements on the table, which would break Odoo.
    """
    cur.execute(
        "SELECT c.relreplident,"
        " EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),"
        " EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident AND i.indisvalid)"
        " FROM pg_class c WHERE c.oid = to_regclass(%s)",
        [f"public.{table}"],
    )
    row = cur.fetchone()
    if row is None:
        return f"{table}: no such table"
    ident, has_pk, has_ident_index = row
    if ident == "f" or (ident == "d" and has_pk) or (ident == "i" and has_ident_index):
        return None
    if ident == "n":
        return f"{table}: REPLICA IDENTITY NOTHING"
    if ident == "i":
        return f"{table}: REPLICA IDENTITY USING INDEX, but the index is missing or invalid"
    return f"{table}: no primary key (REPLICA IDENTITY DEFAULT needs one)"


class ReplicationReader:
    def __init__(
        self,
        dsn: str,
        slot: str,
        publication: str,
        resolve_model: Callable[[str], str | None] = lambda table: None,
    ) -> None:
        self.dsn = dsn
        self.slot = slot
        self.publication = publication
        self.resolve_model = resolve_model
        self.conn = None
        self.cur = None
        self.assembler: TransactionAssembler | None = None

    def setup(self, tables: list[str], replica_identity_full: list[str] = ()) -> None:
        """Create or update the publication and create the slot if missing."""
        with closing(psycopg2.connect(self.dsn, application_name="erpdoo-setup")) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                self._setup(cur, tables, replica_identity_full)

    def _setup(self, cur, tables, replica_identity_full) -> None:
        for table in replica_identity_full:
            cur.execute("SELECT relreplident FROM pg_class WHERE oid = %s::regclass", [f"public.{table}"])
            if cur.fetchone()[0] != "f":
                cur.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier("public", table)))
                log.info("set REPLICA IDENTITY FULL on %s", table)

        problems = [p for t in tables if (p := _replica_identity_problem(cur, t))]
        if problems:
            raise PublicationError(
                "refusing to publish these tables, because Postgres would then reject Odoo's"
                " UPDATE and DELETE on them:\n  " + "\n  ".join(problems)
                + "\nGive each table a primary key, or set REPLICA IDENTITY FULL or USING INDEX."
            )

        table_list = sql.SQL(", ").join(sql.Identifier("public", t) for t in tables)
        pub = sql.Identifier(self.publication)
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", [self.publication])
        if cur.fetchone():
            cur.execute("SELECT tablename FROM pg_publication_tables WHERE pubname = %s", [self.publication])
            if {r[0] for r in cur.fetchall()} != set(tables):
                cur.execute(sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(pub, table_list))
                log.info("updated publication %s: %s", self.publication, ", ".join(tables))
        else:
            cur.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(pub, table_list))
            log.info("created publication %s: %s", self.publication, ", ".join(tables))

        cur.execute(
            "SELECT plugin, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s", [self.slot]
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", [self.slot])
            log.info("created replication slot %s", self.slot)
        elif row[0] != "pgoutput":
            raise RuntimeError(f"slot {self.slot} uses plugin {row[0]}, expected pgoutput")
        else:
            log.info("resuming slot %s from %s", self.slot, row[1])

    def start(self) -> None:
        """Open the replication connection and start streaming from the slot's
        confirmed position."""
        self.conn = psycopg2.connect(
            self.dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
            application_name="erpdoo",
        )
        self.cur = self.conn.cursor()
        self.cur.start_replication(
            slot_name=self.slot,
            decode=False,
            options={"proto_version": "1", "publication_names": self.publication},
        )
        self.assembler = TransactionAssembler(self.resolve_model)

    def poll(self, timeout: float = 1.0, max_changes: int = 10_000) -> Batch:
        """Return the transactions committed since the last call, waiting up to
        ``timeout`` seconds for the first one."""
        transactions: list[Transaction] = []
        changes = 0
        deadline = time.monotonic() + timeout
        # Never stop in the middle of a transaction just because of max_changes.
        while changes < max_changes or self.assembler.in_transaction:
            msg = self.cur.read_message()
            if msg is None:
                if transactions:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                select.select([self.cur], [], [], remaining)
                continue
            txn = self.assembler.feed(msg.payload)
            if txn is not None:
                transactions.append(txn)
                changes += len(txn.changes)

        ack_lsn = transactions[-1].end_lsn if transactions else None
        if not self.assembler.in_transaction and self.cur.wal_end:
            # The server has sent everything up to wal_end, and transactions
            # that touch no published table are not sent at all. So once the
            # transactions above are delivered, the slot can move to wal_end.
            # Without that it would hold back WAL while Odoo writes other tables.
            ack_lsn = max(ack_lsn or 0, self.cur.wal_end)
        return Batch(transactions, ack_lsn)

    def ack(self, lsn: int, now: bool = False) -> None:
        """Confirm everything before ``lsn`` as delivered. With ``now`` the
        position goes to the server immediately; otherwise psycopg2 sends it
        on its status interval (10 s)."""
        self.cur.send_feedback(flush_lsn=lsn, force=now)

    def close(self) -> None:
        if self.conn is None:
            return
        try:
            # Send the last acknowledged position now, so a clean stop does
            # not redeliver events.
            self.cur.send_feedback(force=True)
        finally:
            self.conn.close()
            self.conn = self.cur = None
