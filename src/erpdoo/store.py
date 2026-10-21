"""SQLite state store: a local copy of the Odoo data that rules and events
use (orders, partners, products, order lines, ...), kept current from the
replication stream so events carry related records without querying Odoo
for each one.

The store keeps only the columns the rules use. The rules' own tables are
copied whole when tracking starts (a snapshot), because they supply each
change's previous row. Related tables are not copied: a row is fetched from
Postgres the first time an event needs it, and for one2many and many2many
lookups the rows linked to a record are fetched together, the first time.
From then on the stream keeps them current. A fetched row is as Postgres
has it at that moment, which can be newer than the transaction the event
is about when erpdoo is behind.

Changes from before a snapshot or fetch can be streamed again after it (the
slot may be behind); applying them in order still ends at the current rows,
so the copy converges.

The store also gives each change the row it replaces when Postgres does not
send it (updates and deletes without REPLICA IDENTITY FULL). For that it
keeps the previous version of every row a transaction changed, until the
replication slot is confirmed past that transaction: a transaction streamed
again after a crash is already in the store, and its previous rows then
come from these versions, and so do related records as of its commit.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import closing
from dataclasses import dataclass

import psycopg2
from psycopg2 import sql

from erpdoo import pgoutput
from erpdoo.replication import Change, Transaction

log = logging.getLogger(__name__)

_COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")
_UNKNOWN = object()  # no stored version: the slot was confirmed past it, or the table was not tracked
FORMAT = 2  # of the stored rows; an older store starts over
PROGRESS_SECONDS = 5  # how often a long snapshot logs its progress
SNAPSHOT_BATCH = 5000  # rows fetched from Postgres, and written to SQLite, at a time


def row_key(row: dict) -> int:
    """A row's id. Rows of many2many join tables have none, only the ids of
    the two records they link (both are their primary key, so Postgres sends
    them for deletes too); their key combines the two, in column name order."""
    if "id" in row:
        return row["id"]
    first, second = sorted(row)
    return (row[first] << 32) | row[second]


@dataclass(frozen=True)
class TableSpec:
    """What the store keeps of one table."""

    columns: frozenset[str] | None = None  # None: every column
    full: bool = True  # copied whole up front; otherwise rows are fetched when first needed
    # Columns find() looks rows up by (one2many inverse, many2many join
    # column), each with the table of the records they point to.
    lookups: frozenset[tuple[str, str]] = frozenset()

    @property
    def shape(self) -> dict:
        """What decides the rows stored; when it changes, the table starts over."""
        return {"columns": sorted(self.columns) if self.columns is not None else None, "full": self.full}


# Inbound messages (erpdoo.inbound_consumer), in the same file. reset() and
# a format change leave them alone: they say which messages Odoo has had.
INBOUND_SCHEMA = """
    -- Messages carried out (or found carried out already), to skip repeats.
    CREATE TABLE IF NOT EXISTS inbound_messages (
        topic TEXT NOT NULL, message_id TEXT NOT NULL, rule TEXT NOT NULL, status TEXT NOT NULL,
        record_id INTEGER, at INTEGER NOT NULL, PRIMARY KEY (topic, message_id));
    -- Each attempt at a message: the WAL position before it and after it
    -- (NULL while it runs), and the Odoo user it acts as. Messages are
    -- carried out one at a time, so what that user committed in between was
    -- caused by the message. In polling mode the positions are erpdoo's
    -- clock (microseconds since 1970), and record_model and record_id say
    -- which record the message acted on.
    CREATE TABLE IF NOT EXISTS inbound_attempts (
        id INTEGER PRIMARY KEY, start_lsn INTEGER NOT NULL, end_lsn INTEGER, topic TEXT NOT NULL,
        message_id TEXT NOT NULL, rule TEXT NOT NULL, uid INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS inbound_attempts_start ON inbound_attempts (start_lsn);
"""


def inbound_schema(db: sqlite3.Connection) -> None:
    db.executescript(INBOUND_SCHEMA)
    columns = [r[1] for r in db.execute("PRAGMA table_info(inbound_attempts)")]
    if "record_model" not in columns:  # stores from before polling mode
        db.execute("ALTER TABLE inbound_attempts ADD COLUMN record_model TEXT")
        db.execute("ALTER TABLE inbound_attempts ADD COLUMN record_id INTEGER")
SOURCES = {"replication": 1, "polling": 2}  # meta 'source': which mode built the store
SQLITE_TIMEOUT = 30  # seconds to wait for the other connection's write (inbound and outbound share the file)


class StateStore:
    def __init__(self, path: str, dsn: str | None = None) -> None:
        self.db = sqlite3.connect(path, timeout=SQLITE_TIMEOUT)
        self.db.execute("PRAGMA journal_mode=WAL")
        inbound_schema(self.db)
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                tbl TEXT NOT NULL, id INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (tbl, id));
            CREATE TABLE IF NOT EXISTS loaded_tables (tbl TEXT PRIMARY KEY);
            -- The row as it was before the transaction committed at lsn changed
            -- it ('null' if it did not exist yet).
            CREATE TABLE IF NOT EXISTS versions (
                tbl TEXT NOT NULL, id INTEGER NOT NULL, lsn INTEGER NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY (tbl, id, lsn));
            CREATE INDEX IF NOT EXISTS versions_lsn ON versions (lsn);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            -- find() lookups whose rows are all in the store (fetched, or the
            -- record they point to was created after tracking started).
            CREATE TABLE IF NOT EXISTS lookups (
                tbl TEXT NOT NULL, col TEXT NOT NULL, value INTEGER NOT NULL, PRIMARY KEY (tbl, col, value));
            """
        )
        columns = [r[1] for r in self.db.execute("PRAGMA table_info(loaded_tables)")]
        if "lsn" not in columns:  # stores from before versions were kept
            self.db.execute("ALTER TABLE loaded_tables ADD COLUMN lsn INTEGER NOT NULL DEFAULT 0")
        if "spec" not in columns:  # stores that kept whole tables; NULL reads as that
            self.db.execute("ALTER TABLE loaded_tables ADD COLUMN spec TEXT")
        # Table -> the WAL position its snapshot was taken at (0 for tables
        # fetched on demand), and what is kept of it.
        self.loaded: dict[str, int] = {}
        self.specs: dict[str, TableSpec] = {}
        for table, lsn, spec in self.db.execute("SELECT tbl, lsn, spec FROM loaded_tables"):
            self.loaded[table] = lsn
            shape = json.loads(spec) if spec else {"columns": None, "full": True}
            columns = frozenset(shape["columns"]) if shape["columns"] is not None else None
            self.specs[table] = TableSpec(columns, shape["full"])
        row = self.db.execute("SELECT value FROM meta WHERE key = 'format'").fetchone()
        if self.loaded and (row is None or row[0] < FORMAT):
            # Format 1 read booleans from snapshots and fetches as false.
            log.info("state store: written by an older erpdoo, copying it again")
            for name in ("records", "loaded_tables", "versions", "lookups"):
                self.db.execute(f"DELETE FROM {name}")
            self.loaded.clear()
            self.specs.clear()
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('format', ?)", [FORMAT])
        self.db.commit()
        row = self.db.execute("SELECT value FROM meta WHERE key = 'applied_lsn'").fetchone()
        # End of the last transaction applied; anything up to here is in the store.
        self.applied_lsn = row[0] if row else 0
        row = self.db.execute("SELECT value FROM meta WHERE key = 'applied_time'").fetchone()
        # When that transaction committed, in microseconds since 1970 (UTC).
        self.applied_time: int | None = row[0] if row else None
        self.dsn = dsn
        self.fetches = 0  # queries to Postgres for rows not in the store
        self._pg = None
        self._types: dict[str, list[tuple[str, int]]] = {}
        # Table -> the lookups that point to its records: a record created
        # while tracking has no linked rows yet, so its lookups start complete.
        self._children: dict[str, list[tuple[str, str]]] = {}
        self._wal_fallback_logged: set[str] = set()

    @property
    def tables(self) -> set[str]:
        return set(self.specs)

    def reset(self) -> None:
        """Forget everything, e.g. when the replication slot was recreated and
        changes since the last run are lost."""
        for table in ("records", "loaded_tables", "versions", "meta", "lookups"):
            self.db.execute(f"DELETE FROM {table}")
        self.db.execute("INSERT INTO meta VALUES ('format', ?)", [FORMAT])
        self.db.commit()
        self.loaded.clear()
        self.specs.clear()
        self.applied_lsn = 0
        self.applied_time = None

    def use_source(self, source: str) -> None:
        """Start over if the store was built by the other source: rows read
        through Odoo's API (polling) and from Postgres (replication) differ
        slightly, and so do the positions kept."""
        code = SOURCES[source]
        row = self.db.execute("SELECT value FROM meta WHERE key = 'source'").fetchone()
        if (row[0] if row else SOURCES["replication"]) != code and self.loaded:
            log.info("state store: built in %s mode, starting over for %s mode",
                     next(k for k, v in SOURCES.items() if v != code), source)
            self.reset()
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('source', ?)", [code])
        self.db.commit()

    def forget(self, table: str) -> None:
        """Stop tracking a table. Its rows would go stale, so they are dropped."""
        for name in ("records", "loaded_tables", "versions", "lookups"):
            self.db.execute(f"DELETE FROM {name} WHERE tbl = ?", [table])
        self.db.commit()
        self.loaded.pop(table, None)
        self.specs.pop(table, None)

    def track(self, table: str, spec: TableSpec, lsn: int = 0) -> None:
        """Start tracking a table, empty."""
        self.db.execute("INSERT OR REPLACE INTO loaded_tables (tbl, lsn, spec) VALUES (?, ?, ?)",
                        [table, lsn, json.dumps(spec.shape)])
        self.loaded[table] = lsn
        self.specs[table] = spec
        for column, parent in spec.lookups:
            self.index(column)
            if (table, column) not in self._children.setdefault(parent, []):
                self._children[parent].append((table, column))

    def load(self, table: str, rows: Iterable[dict], lsn: int = 0, spec: TableSpec | None = None) -> int:
        """Replace a table's rows with a snapshot taken at WAL position
        ``lsn`` and start tracking it."""
        spec = spec or TableSpec()
        self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
        self.db.execute("DELETE FROM versions WHERE tbl = ?", [table])
        count = 0
        for batch in _batches((table, row_key(row), json.dumps(_keep(spec, row))) for row in rows):
            self.db.executemany("INSERT INTO records VALUES (?, ?, ?)", batch)
            count += len(batch)
            self.db.commit()  # lets SQLite checkpoint its WAL during a long copy
        self.track(table, spec, lsn)
        self.db.commit()
        return count

    def index(self, column: str) -> None:
        """Index a column so find() on it is fast (one2many lookups)."""
        if not _COLUMN.match(column):
            raise ValueError(f"invalid column name {column!r}")
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS records_{column} ON records (tbl, json_extract(data, '$.{column}'))"
        )

    def apply_transaction(self, txn: Transaction) -> bool:
        """Apply a committed transaction to the tracked tables and give each
        update or delete without its previous row (``change.old``) the row
        from the store. Returns True if the store already had the
        transaction, because it is streamed again after a restart; it is
        then not applied twice, and previous rows come from the versions.

        Changes committed before a table's snapshot get no previous row: the
        snapshot may already include them.
        """
        replay = txn.end_lsn <= self.applied_lsn
        # Each row as of this point in the transaction, during a replay.
        rows: dict[tuple[str, int], object] = {}
        for change in txn.changes:
            table = change.table
            spec = self.specs.get(table)
            if spec is None:
                continue
            if change.op == "truncate":
                if not replay:
                    self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
                continue
            record_id = row_key(change.new or change.old)
            if replay:
                key = (table, record_id)
                if key not in rows:
                    rows[key] = self._version(table, record_id, txn.commit_lsn)
                previous = rows[key]
                if previous is not _UNKNOWN:
                    rows[key] = None if change.op == "delete" else {**(previous or {}), **change.new}
            else:
                previous = self._current(table, record_id)
                if previous is None and change.op != "insert" and not spec.full:
                    previous = _UNKNOWN  # never fetched: not known, rather than absent
                else:
                    self.db.execute("INSERT OR IGNORE INTO versions VALUES (?, ?, ?, ?)",
                                    [table, record_id, txn.commit_lsn, json.dumps(previous)])
            if change.op != "insert" and txn.commit_lsn >= self.loaded[table]:
                known = previous is not None and previous is not _UNKNOWN
                if known and (change.old is None or change.old_is_key_only):
                    change.old, change.old_is_key_only = previous, False
                elif not known and change.old is not None and not change.old_is_key_only and spec.full:
                    if table not in self._wal_fallback_logged:
                        self._wal_fallback_logged.add(table)
                        log.info("%s: previous row of %s not in the state store, using the one Postgres sent"
                                 " (REPLICA IDENTITY FULL)", table, record_id)
            if not replay:
                self._apply(change, None if previous is _UNKNOWN else previous, spec)
        if not replay:
            self.applied_lsn = txn.end_lsn
            self.applied_time = int(txn.commit_time.timestamp() * 1_000_000)
            self.db.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                                [("applied_lsn", txn.end_lsn), ("applied_time", self.applied_time)])
        return replay

    def _apply(self, change: Change, previous: dict | None, spec: TableSpec) -> None:
        table = change.table
        if change.op == "delete":
            self.db.execute("DELETE FROM records WHERE tbl = ? AND id = ?", [table, row_key(change.old)])
            return
        new = _keep(spec, change.new)
        if change.op == "insert":
            row = new
            for child, column in self._children.get(table, ()):
                self.db.execute("INSERT OR IGNORE INTO lookups VALUES (?, ?, ?)", [child, column, row_key(row)])
        elif previous is not None or spec.full:
            # Unchanged TOASTed columns are not in the new row of an update; keep the stored value.
            row = {**(previous or {}), **new}
        elif spec.columns is not None and spec.columns <= change.new.keys():
            row = new  # not fetched yet, but the update has every column kept
        else:
            return  # fetched in full when an event needs it
        self.db.execute(
            "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT (tbl, id) DO UPDATE SET data = excluded.data",
            [table, row_key(row), json.dumps(row)],
        )

    def _version(self, table: str, record_id: int, lsn: int):
        """The row before the transaction committed at ``lsn``: None if it did
        not exist, _UNKNOWN if that version is no longer kept."""
        row = self.db.execute("SELECT data FROM versions WHERE tbl = ? AND id = ? AND lsn = ?",
                              [table, record_id, lsn]).fetchone()
        return json.loads(row[0]) if row else _UNKNOWN

    def set_pending_backfill(self, since: int, until: int) -> None:
        """Remember a window to backfill (microseconds since 1970, UTC), so it
        is backfilled after a restart if delivering it fails."""
        self.db.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                            [("backfill_since", since), ("backfill_until", until)])
        self.db.commit()

    def pending_backfill(self) -> tuple[int, int] | None:
        rows = dict(self.db.execute("SELECT key, value FROM meta WHERE key IN ('backfill_since', 'backfill_until')"))
        return (rows["backfill_since"], rows["backfill_until"]) if len(rows) == 2 else None

    def clear_pending_backfill(self) -> None:
        self.db.execute("DELETE FROM meta WHERE key IN ('backfill_since', 'backfill_until')")
        self.db.commit()

    def prune(self, lsn: int) -> None:
        """Drop versions of transactions before ``lsn``: the slot is confirmed
        past them, so they are not streamed again. So are inbound attempts
        that ended before it."""
        self.db.execute("DELETE FROM versions WHERE lsn < ?", [lsn])
        self.db.execute("DELETE FROM inbound_attempts WHERE end_lsn < ?", [lsn])

    def inbound_cause(self, commit_lsn: int) -> dict | None:
        """The inbound attempt a transaction committed at ``commit_lsn`` falls
        in, if any: {"topic", "message_id", "rule", "uid"}."""
        row = self.db.execute(
            "SELECT topic, message_id, rule, uid FROM inbound_attempts WHERE start_lsn < ?"
            " AND (end_lsn IS NULL OR end_lsn >= ?) ORDER BY start_lsn DESC, id DESC LIMIT 1",
            [commit_lsn, commit_lsn]).fetchone()
        return dict(zip(("topic", "message_id", "rule", "uid"), row)) if row else None

    def inbound_cause_at(self, at: int, uid: int, model: str, record_id: int, slack: int) -> dict | None:
        """Polling mode: the inbound attempt of Odoo user ``uid`` that a change
        at ``at`` falls in. Odoo gives times to the second (``at`` is the
        second's start) and attempts are timed on erpdoo's clock, so the
        attempt's window is widened by ``slack`` microseconds for the two
        clocks' difference. Of several, the one that acted on this record,
        else the latest."""
        row = self.db.execute(
            "SELECT topic, message_id, rule, uid FROM inbound_attempts WHERE uid = ? AND start_lsn < ?"
            " AND (end_lsn IS NULL OR end_lsn >= ?)"
            " ORDER BY coalesce(record_model = ? AND record_id = ?, 0) DESC, start_lsn DESC, id DESC LIMIT 1",
            [uid, at + 1_000_000 + slack, at - slack, model, record_id]).fetchone()
        return dict(zip(("topic", "message_id", "rule", "uid"), row)) if row else None

    def prune_inbound_attempts(self, before: int) -> None:
        """Polling mode: forget attempts that ended before ``before`` (microseconds)."""
        self.db.execute("DELETE FROM inbound_attempts WHERE end_lsn < ?", [before])

    def commit(self) -> None:
        self.db.commit()

    def _current(self, table: str, record_id: int) -> dict | None:
        row = self.db.execute("SELECT data FROM records WHERE tbl = ? AND id = ?", [table, record_id]).fetchone()
        return json.loads(row[0]) if row else None

    def get(self, table: str, record_id: int, as_of: int | None = None) -> dict | None:
        """A row, as of WAL position ``as_of`` (the end of a transaction) if
        given: during a replay the store is ahead of the stream, and the row
        then comes from the versions kept since the slot's confirmed position.
        A row of a table fetched on demand is read from Postgres the first time."""
        if as_of is not None and as_of < self.applied_lsn:
            row = self.db.execute("SELECT data FROM versions WHERE tbl = ? AND id = ? AND lsn >= ?"
                                  " ORDER BY lsn LIMIT 1", [table, record_id, as_of]).fetchone()
            if row:
                return json.loads(row[0])
        current = self._current(table, record_id)
        spec = self.specs.get(table)
        if current is None and spec is not None and not spec.full:
            fetched = self._fetch(table, "id", record_id)
            current = fetched[0] if fetched else None
        return current

    def find(self, table: str, column: str, value: object, as_of: int | None = None) -> list[dict]:
        """Rows of ``table`` whose ``column`` equals ``value``, by id, as of
        WAL position ``as_of`` if given (see get). For a table fetched on
        demand, the rows are read from Postgres the first time."""
        if not _COLUMN.match(column):
            raise ValueError(f"invalid column name {column!r}")
        spec = self.specs.get(table)
        if spec is not None and not spec.full and not self.db.execute(
                "SELECT 1 FROM lookups WHERE tbl = ? AND col = ? AND value = ?", [table, column, value]).fetchone():
            self._fetch(table, column, value)
            self.db.execute("INSERT OR IGNORE INTO lookups VALUES (?, ?, ?)", [table, column, value])
        # No ORDER BY: with it, SQLite prefers the primary key (tbl, id) and
        # scans the whole table instead of using the column's index.
        rows = dict(self.db.execute(
            f"SELECT id, data FROM records WHERE tbl = ? AND json_extract(data, '$.{column}') = ?",
            [table, value],
        ))
        if as_of is not None and as_of < self.applied_lsn:
            # Rows changed since then were, at that point, as their oldest
            # version after it says (null: not created yet).
            for record_id, data in self.db.execute(
                "SELECT id, data FROM versions v WHERE tbl = ? AND lsn = ("
                " SELECT min(lsn) FROM versions w WHERE w.tbl = v.tbl AND w.id = v.id AND w.lsn >= ?)",
                [table, as_of],
            ):
                then = json.loads(data)
                if then is not None and then.get(column) == value:
                    rows[record_id] = data
                else:
                    rows.pop(record_id, None)
        return [json.loads(data) for _, data in sorted(rows.items())]

    def _trim(self, table: str, spec: TableSpec) -> None:
        for name in ("records", "versions"):
            key = "id" if name == "records" else "id, lsn"
            for batch in _batches(self.db.execute(f"SELECT {key}, data FROM {name} WHERE tbl = ?", [table]).fetchall()):
                self.db.executemany(
                    f"UPDATE {name} SET data = ? WHERE tbl = ? AND " + " AND ".join(f"{k} = ?" for k in key.split(", ")),
                    [(json.dumps(_keep(spec, json.loads(data)) if data != "null" else None), table, *keys)
                     for *keys, data in batch])
        self.track(table, spec, self.loaded[table])
        self.db.commit()

    def _fetch(self, table: str, column: str, value: object) -> list[dict]:
        """Read the rows of ``table`` whose ``column`` is ``value`` from
        Postgres, as it has them now, and keep them."""
        rows = self._read_postgres(table, column, value)
        self.fetches += 1
        self.db.executemany(
            "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT (tbl, id) DO UPDATE SET data = excluded.data",
            [(table, row_key(row), json.dumps(row)) for row in rows])
        return rows

    def _read_postgres(self, table: str, column: str, value: object) -> list[dict]:
        if self._pg is None:
            self._pg = psycopg2.connect(self.dsn, application_name="erpdoo-fetch")
            self._pg.autocommit = True
        if table not in self._types:
            self._types[table] = _column_types(self._pg, table, self.specs[table].columns)
        columns = self._types[table]
        query = sql.SQL("SELECT {} FROM {} WHERE {} = %s").format(
            sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(name)) for name, _ in columns),
            sql.Identifier("public", table), sql.Identifier(column))
        with self._pg.cursor() as cur:
            cur.execute(query, [value])
            return [{name: pgoutput.convert(oid, v) for (name, oid), v in zip(columns, values)} for values in cur]

    def sync(self, dsn: str, specs: dict[str, TableSpec]) -> None:
        """Track exactly the tables in ``specs``: drop tables no longer
        needed, start over with tables whose kept columns changed, and copy
        the tables to be copied whole."""
        self.dsn = dsn
        self._types.clear()
        for table in sorted(self.tables - specs.keys()):
            self.forget(table)
            log.info("state store: stopped tracking %s", table)
        for table, spec in sorted(specs.items()):
            have = self.specs.get(table)
            if have is None or have.shape == spec.shape:
                continue
            if _covers(have, spec):
                # Fewer columns (or fetched on demand from now on): trim the
                # rows in place, keeping the copy's position and the versions.
                self._trim(table, spec)
                log.info("state store: %s now keeps %s", table,
                         "every column" if spec.columns is None else f"{len(spec.columns)} columns")
            else:
                self.forget(table)
                log.info("state store: %s needs columns it did not keep, starting it over", table)
        self._children.clear()
        for table, spec in sorted(specs.items()):
            if table in self.specs or not spec.full:
                self.track(table, spec, self.loaded.get(table, 0))
        self.db.commit()
        missing = sorted(t for t, spec in specs.items() if t not in self.specs)
        if missing:
            with closing(psycopg2.connect(dsn, application_name="erpdoo-snapshot")) as conn:
                conn.set_session(isolation_level="REPEATABLE READ")
                for table in missing:
                    started = time.monotonic()
                    lsn, rows = snapshot(conn, table, specs[table].columns)
                    count = self.load(table, rows, lsn, specs[table])
                    log.info("state store: copied %d rows of %s in %.1f s", count, table, time.monotonic() - started)


def _covers(have: TableSpec, want: TableSpec) -> bool:
    """Whether a table kept as ``have`` already holds what ``want`` keeps."""
    columns = have.columns is None or (want.columns is not None and want.columns <= have.columns)
    return columns and (have.full or not want.full)


def _keep(spec: TableSpec, row: dict) -> dict:
    return row if spec.columns is None else {k: v for k, v in row.items() if k in spec.columns}


def _batches(items: Iterable, size: int = SNAPSHOT_BATCH) -> Iterator[list]:
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _column_types(conn, table: str, columns: frozenset[str] | None) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attname, atttypid FROM pg_attribute"
            " WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            [f"public.{table}"],
        )
        return [(name, oid) for name, oid in cur.fetchall() if columns is None or name in columns]


def snapshot(conn, table: str, columns: frozenset[str] | None = None) -> tuple[int, Iterator[dict]]:
    """The WAL position of a snapshot of a table, and its rows (only
    ``columns``, if given), converted exactly like streamed rows: every
    column is read as text and converted by its type, as pgoutput values are.
    ``conn`` must use REPEATABLE READ, so the rows are as of that position."""
    with conn.cursor() as cur:
        # The first statement takes the snapshot; every transaction it sees
        # committed before this position.
        cur.execute("SELECT pg_current_wal_insert_lsn() - '0/0', reltuples FROM pg_class"
                    " WHERE oid = %s::regclass", [f"public.{table}"])
        lsn, estimate = cur.fetchone()
    return int(lsn), _rows(conn, table, columns, max(int(estimate), 0))


def _rows(conn, table: str, columns: frozenset[str] | None, estimate: int) -> Iterator[dict]:
    types = _column_types(conn, table, columns)
    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(name)) for name, _ in types),
        sql.Identifier("public", table),
    )
    with conn.cursor(name=f"snapshot_{table}") as cur:
        cur.itersize = SNAPSHOT_BATCH
        cur.execute(query)
        count, logged = 0, time.monotonic()
        for values in cur:
            yield {name: pgoutput.convert(oid, v) for (name, oid), v in zip(types, values)}
            count += 1
            if time.monotonic() - logged >= PROGRESS_SECONDS:
                logged = time.monotonic()
                total = f" of about {estimate:,}" if estimate else ""
                log.info("state store: copying %s, %s%s rows so far", table, f"{count:,}", total)
    conn.commit()


class InboundLog:
    """The inbound side's own connection to the state store file."""

    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path, timeout=SQLITE_TIMEOUT, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        inbound_schema(self.db)
        self.db.commit()

    def processed(self, topic: str, message_id: str) -> tuple[str, int | None] | None:
        """(status, record id) if the message was carried out already."""
        return self.db.execute("SELECT status, record_id FROM inbound_messages WHERE topic = ? AND message_id = ?",
                               [topic, message_id]).fetchone()

    def begin(self, topic: str, message_id: str, rule: str, uid: int, lsn: int) -> int:
        """Note an attempt starting at WAL position ``lsn``, before calling
        Odoo. An attempt still open (erpdoo stopped during it) ends here."""
        self.db.execute("UPDATE inbound_attempts SET end_lsn = ? WHERE end_lsn IS NULL", [lsn])
        attempt = self.db.execute(
            "INSERT INTO inbound_attempts (start_lsn, topic, message_id, rule, uid) VALUES (?, ?, ?, ?, ?)",
            [lsn, topic, message_id, rule, uid]).lastrowid
        self.db.commit()
        return attempt

    def end(self, attempt: int, lsn: int | None, record: tuple[str, int] | None = None) -> None:
        """Note where an attempt ended (None: not known, left open until the
        next one), and the record it acted on, if it succeeded."""
        if record is not None:
            self.db.execute("UPDATE inbound_attempts SET record_model = ?, record_id = ? WHERE id = ?",
                            [*record, attempt])
        if lsn is not None:
            self.db.execute("UPDATE inbound_attempts SET end_lsn = ? WHERE id = ?", [lsn, attempt])
        self.db.commit()

    def finish(self, topic: str, message_id: str, rule: str, status: str, record_id: int | None) -> None:
        self.db.execute("INSERT OR REPLACE INTO inbound_messages VALUES (?, ?, ?, ?, ?, ?)",
                        [topic, message_id, rule, status, record_id, int(time.time())])
        self.db.commit()

    def prune(self, keep_seconds: float) -> int:
        """Forget processed messages older than ``keep_seconds``."""
        count = self.db.execute("DELETE FROM inbound_messages WHERE at < ?",
                                [int(time.time() - keep_seconds)]).rowcount
        self.db.commit()
        return count

    def close(self) -> None:
        self.db.close()
