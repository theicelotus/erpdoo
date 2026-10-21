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

from erpdoo import health, pgoutput
from erpdoo.backoff import reason
from erpdoo.health import SlotHealth

log = logging.getLogger(__name__)


@dataclass
class Change:
    op: str  # "insert" | "update" | "delete" | "truncate"
    table: str
    model: str | None  # Odoo model name, if the table belongs to one
    # Row before the change. As streamed, for updates it is None unless the
    # table has REPLICA IDENTITY FULL (or the key changed, then it holds only
    # key columns), and for deletes it holds only the key. For tables in the
    # state store, StateStore.apply_transaction fills in the whole row.
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


class StreamError(Exception):
    """The replication stream broke, e.g. Postgres restarted."""


@dataclass(frozen=True)
class TableState:
    quoted: str  # the table's name as SQL, quoted if it needs to be
    replica_identity: str  # pg_class.relreplident: d (default), f (full), i (index) or n (nothing)
    has_primary_key: bool
    has_identity_index: bool  # a valid index chosen with REPLICA IDENTITY USING INDEX
    may_alter: bool  # erpdoo's role owns the table, directly or through another role
    readable: bool  # erpdoo's role may SELECT from it


@dataclass(frozen=True)
class PublicationState:
    owned: bool  # owned by erpdoo's own role: erpdoo keeps it to exactly the tables it needs
    may_alter: bool  # erpdoo's role may alter it (owner, member of the owner, or superuser)
    all_tables: bool  # FOR ALL TABLES
    publishes_changes: bool  # publishes inserts, updates and deletes
    tables: frozenset[str]


@dataclass(frozen=True)
class Step:
    sql: str
    allowed: bool  # erpdoo's role may run it
    done: str  # log message once run


def replica_identity_problem(table: str, state: TableState | None) -> str | None:
    """Why ``table`` cannot be published safely, or None if it can.

    A published table must identify the rows of an UPDATE or DELETE: by its
    primary key (REPLICA IDENTITY DEFAULT), a chosen unique index (USING
    INDEX) or the whole row (FULL). Otherwise Postgres rejects those
    statements on the table, which would break Odoo.
    """
    if state is None:
        return f"{table}: no such table"
    ident = state.replica_identity
    if ident == "f" or (ident == "d" and state.has_primary_key) or (ident == "i" and state.has_identity_index):
        return None
    if ident == "n":
        return f"{table}: REPLICA IDENTITY NOTHING"
    if ident == "i":
        return f"{table}: REPLICA IDENTITY USING INDEX, but the index is missing or invalid"
    return f"{table}: no primary key (REPLICA IDENTITY DEFAULT needs one)"


def plan_setup(
    publication: str,
    role: str,
    tables: list[str],
    replica_identity_full: list[str],
    states: dict[str, TableState],
    pub: PublicationState | None,
    may_create: bool,
) -> tuple[list[Step], list[str]]:
    """The statements that make ``publication`` stream ``tables`` with the
    replica identities erpdoo needs, and what is missing, in words.

    ``publication`` and ``role`` are SQL-quoted names; ``states`` covers
    ``tables`` and the tables already in the publication. A publication
    owned by erpdoo's role is kept to exactly ``tables``; one created by an
    admin is only ever added to.
    """
    steps: list[Step] = []
    missing: list[str] = []

    def table_list(names):
        return ", ".join(f"public.{states[t].quoted}" for t in names)

    need_full = [t for t in replica_identity_full if states[t].replica_identity != "f"]
    for t in need_full:
        steps.append(Step(f"ALTER TABLE public.{states[t].quoted} REPLICA IDENTITY FULL",
                          states[t].may_alter, f"set REPLICA IDENTITY FULL on {t}"))
    if need_full:
        missing.append("need REPLICA IDENTITY FULL, so that updates carry the previous row: "
                       + ", ".join(need_full))

    if pub is None:
        steps.append(Step(f"CREATE PUBLICATION {publication} FOR TABLE {table_list(tables)}",
                          may_create and all(states[t].may_alter for t in tables),
                          f"created publication {publication}: {', '.join(tables)}"))
        missing.append(f"publication {publication} does not exist")
    else:
        absent = [] if pub.all_tables else [t for t in tables if t not in pub.tables]
        if absent:
            steps.append(Step(f"ALTER PUBLICATION {publication} ADD TABLE {table_list(absent)}",
                              pub.may_alter and all(states[t].may_alter for t in absent),
                              f"added to publication {publication}: {', '.join(absent)}"))
            missing.append(f"not in publication {publication}: " + ", ".join(absent))
        extra = sorted(pub.tables - set(tables)) if pub.owned and not pub.all_tables else []
        if extra:
            steps.append(Step(f"ALTER PUBLICATION {publication} DROP TABLE {table_list(extra)}",
                              True, f"removed from publication {publication}: {', '.join(extra)}"))
        if not pub.publishes_changes:
            steps.append(Step(f"ALTER PUBLICATION {publication} SET (publish = 'insert, update, delete, truncate')",
                              pub.may_alter, f"publication {publication} now publishes all changes"))
            missing.append(f"publication {publication} does not publish every insert, update and delete")

    unreadable = [t for t in tables if not states[t].readable]
    if unreadable:
        steps.append(Step(f"GRANT SELECT ON {table_list(unreadable)} TO {role}", False, ""))
        missing.append(f"not readable by {role}: " + ", ".join(unreadable))
    return steps, missing


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
        self._resume_logged = False
        # Set by setup() when Postgres had invalidated the slot and it was created again.
        self.slot_was_lost = False
        self._health_conn = None

    def setup(self, tables: list[str], replica_identity_full: list[str] = ()) -> bool:
        """Create or update the publication and create the slot if missing.
        Returns True if the slot was created."""
        with closing(psycopg2.connect(self.dsn, application_name="erpdoo-setup")) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                return self._setup(cur, tables, replica_identity_full)

    def _setup(self, cur, tables, replica_identity_full) -> bool:
        cur.execute(
            "SELECT quote_ident(%s), quote_ident(current_user),"
            " has_database_privilege(current_database(), 'CREATE')",
            [self.publication],
        )
        publication, role, may_create = cur.fetchone()
        cur.execute(
            "SELECT p.pubowner = r.oid, pg_has_role(p.pubowner, 'USAGE'), p.puballtables,"
            " p.pubinsert AND p.pubupdate AND p.pubdelete,"
            " ARRAY(SELECT tablename FROM pg_publication_tables t"
            "       WHERE t.pubname = p.pubname AND t.schemaname = 'public')"
            " FROM pg_publication p, pg_roles r WHERE p.pubname = %s AND r.rolname = current_user",
            [self.publication],
        )
        row = cur.fetchone()
        pub = PublicationState(*row[:4], frozenset(row[4])) if row else None

        cur.execute(
            "SELECT c.relname, quote_ident(c.relname), c.relreplident,"
            " EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),"
            " EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident AND i.indisvalid),"
            " pg_has_role(c.relowner, 'USAGE'), has_table_privilege(c.oid, 'SELECT')"
            " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'"
            " WHERE c.relname = ANY(%s) AND c.relkind IN ('r', 'p')",
            [sorted(set(tables) | (pub.tables if pub else set()))],
        )
        states = {row[0]: TableState(*row[1:]) for row in cur.fetchall()}
        full = set(replica_identity_full)
        # Tables that get REPLICA IDENTITY FULL need nothing else.
        problems = [p for t in tables
                    if not (t in full and t in states) and (p := replica_identity_problem(t, states.get(t)))]
        if problems:
            raise PublicationError(
                "refusing to publish these tables, because Postgres would then reject Odoo's"
                " UPDATE and DELETE on them:\n  " + "\n  ".join(problems)
                + "\nGive each table a primary key, or set REPLICA IDENTITY FULL or USING INDEX."
            )

        steps, missing = plan_setup(publication, role, tables, replica_identity_full, states, pub, may_create)
        if not all(step.allowed for step in steps):
            raise PublicationError(
                f"erpdoo needs these changes in Postgres, which its role {role} is not allowed to make:\n  "
                + "\n  ".join(missing)
                + "\nRun this SQL as an admin (a superuser, or the owner of Odoo's tables and of the"
                " publication):\n\n" + "\n".join(f"  {step.sql};" for step in steps)
            )
        for step in steps:
            cur.execute(step.sql)
            log.info("%s", step.done)

        cur.execute(
            "SELECT plugin, confirmed_flush_lsn, wal_status FROM pg_replication_slots WHERE slot_name = %s",
            [self.slot],
        )
        row = cur.fetchone()
        if row is not None and row[2] == "lost":
            # Postgres removed WAL the slot still needed (max_slot_wal_keep_size);
            # it cannot stream from it any more.
            cur.execute("SELECT pg_drop_replication_slot(%s)", [self.slot])
            self.slot_was_lost = True
            row = None
        if row is None:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", [self.slot])
            log.info("created replication slot %s", self.slot)
            self._resume_logged = True
            return True
        if row[0] != "pgoutput":
            raise RuntimeError(f"slot {self.slot} uses plugin {row[0]}, expected pgoutput")
        # setup() runs again when Odoo's models change; say this once.
        log.log(logging.DEBUG if self._resume_logged else logging.INFO, "resuming slot %s from %s", self.slot, row[1])
        self._resume_logged = True
        return False

    def start(self) -> None:
        """Open the replication connection and start streaming from the slot's
        confirmed position."""
        self.conn = psycopg2.connect(
            self.dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
            application_name="erpdoo",
        )
        self.cur = self.conn.cursor()
        try:
            self.cur.start_replication(
                slot_name=self.slot,
                decode=False,
                options={"proto_version": "1", "publication_names": self.publication},
            )
        except psycopg2.errors.ObjectNotInPrerequisiteState as e:
            # Invalidated since setup(); the next start creates it again.
            raise StreamError(f"cannot stream from slot {self.slot} ({reason(e)})") from e
        self.assembler = TransactionAssembler(self.resolve_model)

    def poll(self, timeout: float = 1.0, max_changes: int = 10_000) -> Batch:
        """Return the transactions committed since the last call, waiting up to
        ``timeout`` seconds for the first one."""
        transactions: list[Transaction] = []
        changes = 0
        deadline = time.monotonic() + timeout
        # Never stop in the middle of a transaction just because of max_changes.
        while changes < max_changes or self.assembler.in_transaction:
            try:
                msg = self.cur.read_message()
            except psycopg2.DatabaseError as e:
                # A server that goes away mid-stream shows as "no COPY in progress".
                raise StreamError(f"lost the replication stream from Postgres ({reason(e)})") from e
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

    def health(self) -> SlotHealth | None:
        """How far the slot trails Postgres' WAL (see erpdoo.health)."""
        if self._health_conn is None:
            self._health_conn = psycopg2.connect(self.dsn, application_name="erpdoo-health")
            self._health_conn.autocommit = True
        with self._health_conn.cursor() as cur:
            return health.query(cur, self.slot)

    def close(self) -> None:
        if self._health_conn is not None:
            self._health_conn.close()
            self._health_conn = None
        if self.conn is None:
            return
        try:
            # Send the last acknowledged position now, so a clean stop does
            # not redeliver events.
            self.cur.send_feedback(force=True)
        except psycopg2.DatabaseError:
            pass  # the stream is gone; the slot keeps the last position it got
        finally:
            self.conn.close()
            self.conn = self.cur = None
