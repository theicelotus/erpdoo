"""SQLite state store: a local copy of the Odoo tables that events include
(partners, products, order lines, ...), kept current from the replication
stream so events carry related records without querying Odoo.

Each tracked table is loaded once from a snapshot, then updated by streamed
changes. Changes from before the snapshot can be streamed again after it
(the slot may be behind); applying them in order still ends at the current
rows, so the copy converges.

The store also gives each change the row it replaces when Postgres does not
send it (updates and deletes without REPLICA IDENTITY FULL). For that it
keeps the previous version of every row a transaction changed, until the
replication slot is confirmed past that transaction: a transaction streamed
again after a crash is already in the store, and its previous rows then
come from these versions.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing

import psycopg2
from psycopg2 import sql

from erpdoo import pgoutput
from erpdoo.replication import Change, Transaction

log = logging.getLogger(__name__)

_COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")
_UNKNOWN = object()  # no stored version: the slot was confirmed past it, or the table was not tracked


class StateStore:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
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
            """
        )
        columns = [r[1] for r in self.db.execute("PRAGMA table_info(loaded_tables)")]
        if "lsn" not in columns:  # stores from before versions were kept
            self.db.execute("ALTER TABLE loaded_tables ADD COLUMN lsn INTEGER NOT NULL DEFAULT 0")
        # Table -> the WAL position its snapshot was taken at.
        self.loaded = dict(self.db.execute("SELECT tbl, lsn FROM loaded_tables"))
        row = self.db.execute("SELECT value FROM meta WHERE key = 'applied_lsn'").fetchone()
        # End of the last transaction applied; anything up to here is in the store.
        self.applied_lsn = row[0] if row else 0
        self._wal_fallback_logged: set[str] = set()

    @property
    def tables(self) -> set[str]:
        return set(self.loaded)

    def reset(self) -> None:
        """Forget everything, e.g. when the replication slot was recreated and
        changes since the last run are lost."""
        for table in ("records", "loaded_tables", "versions", "meta"):
            self.db.execute(f"DELETE FROM {table}")
        self.db.commit()
        self.loaded.clear()
        self.applied_lsn = 0

    def forget(self, table: str) -> None:
        """Stop tracking a table. Its rows would go stale, so they are dropped."""
        for name in ("records", "loaded_tables", "versions"):
            self.db.execute(f"DELETE FROM {name} WHERE tbl = ?", [table])
        self.db.commit()
        self.loaded.pop(table, None)

    def load(self, table: str, rows: Iterable[dict], lsn: int = 0) -> int:
        """Replace a table's rows with a snapshot taken at WAL position
        ``lsn`` and start tracking it."""
        self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
        self.db.execute("DELETE FROM versions WHERE tbl = ?", [table])
        count = 0
        batch = []
        for row in rows:
            batch.append((table, row["id"], json.dumps(row)))
            if len(batch) >= 1000:
                self.db.executemany("INSERT INTO records VALUES (?, ?, ?)", batch)
                count += len(batch)
                batch.clear()
        self.db.executemany("INSERT INTO records VALUES (?, ?, ?)", batch)
        count += len(batch)
        self.db.execute("INSERT OR REPLACE INTO loaded_tables (tbl, lsn) VALUES (?, ?)", [table, lsn])
        self.db.commit()
        self.loaded[table] = lsn
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
            if table not in self.loaded:
                continue
            if change.op == "truncate":
                if not replay:
                    self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
                continue
            record_id = (change.new or change.old)["id"]
            if replay:
                key = (table, record_id)
                if key not in rows:
                    rows[key] = self._version(table, record_id, txn.commit_lsn)
                previous = rows[key]
                if previous is not _UNKNOWN:
                    rows[key] = None if change.op == "delete" else {**(previous or {}), **change.new}
            else:
                previous = self.get(table, record_id)
                self.db.execute("INSERT OR IGNORE INTO versions VALUES (?, ?, ?, ?)",
                                [table, record_id, txn.commit_lsn, json.dumps(previous)])
            if change.op != "insert" and txn.commit_lsn >= self.loaded[table]:
                known = previous is not None and previous is not _UNKNOWN
                if known and (change.old is None or change.old_is_key_only):
                    change.old, change.old_is_key_only = previous, False
                elif not known and change.old is not None and not change.old_is_key_only:
                    if table not in self._wal_fallback_logged:
                        self._wal_fallback_logged.add(table)
                        log.info("%s: previous row of %s not in the state store, using the one Postgres sent"
                                 " (REPLICA IDENTITY FULL)", table, record_id)
            if not replay:
                self._apply(change, previous)
        if not replay:
            self.applied_lsn = txn.end_lsn
            self.db.execute("INSERT OR REPLACE INTO meta VALUES ('applied_lsn', ?)", [txn.end_lsn])
        return replay

    def _apply(self, change: Change, previous: dict | None) -> None:
        if change.op == "delete":
            self.db.execute("DELETE FROM records WHERE tbl = ? AND id = ?", [change.table, change.old["id"]])
            return
        # Unchanged TOASTed columns are not in the new row of an update; keep the stored value.
        row = {**(previous or {}), **change.new} if change.op == "update" else change.new
        self.db.execute(
            "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT (tbl, id) DO UPDATE SET data = excluded.data",
            [change.table, row["id"], json.dumps(row)],
        )

    def _version(self, table: str, record_id: int, lsn: int):
        """The row before the transaction committed at ``lsn``: None if it did
        not exist, _UNKNOWN if that version is no longer kept."""
        row = self.db.execute("SELECT data FROM versions WHERE tbl = ? AND id = ? AND lsn = ?",
                              [table, record_id, lsn]).fetchone()
        return json.loads(row[0]) if row else _UNKNOWN

    def prune(self, lsn: int) -> None:
        """Drop versions of transactions before ``lsn``: the slot is confirmed
        past them, so they are not streamed again."""
        self.db.execute("DELETE FROM versions WHERE lsn < ?", [lsn])

    def commit(self) -> None:
        self.db.commit()

    def get(self, table: str, record_id: int) -> dict | None:
        row = self.db.execute("SELECT data FROM records WHERE tbl = ? AND id = ?", [table, record_id]).fetchone()
        return json.loads(row[0]) if row else None

    def find(self, table: str, column: str, value: object) -> list[dict]:
        """Rows of ``table`` whose ``column`` equals ``value``, by id."""
        if not _COLUMN.match(column):
            raise ValueError(f"invalid column name {column!r}")
        # No ORDER BY: with it, SQLite prefers the primary key (tbl, id) and
        # scans the whole table instead of using the column's index.
        rows = self.db.execute(
            f"SELECT id, data FROM records WHERE tbl = ? AND json_extract(data, '$.{column}') = ?",
            [table, value],
        )
        return [json.loads(data) for _, data in sorted(rows)]

    def sync(self, dsn: str, tables: dict[str, set[str]]) -> None:
        """Track exactly ``tables`` (table -> columns to index): drop tables no
        longer needed and load a snapshot of new ones."""
        for table in sorted(self.tables - tables.keys()):
            self.forget(table)
            log.info("state store: stopped tracking %s", table)
        for columns in tables.values():
            for column in columns:
                self.index(column)
        missing = sorted(tables.keys() - self.tables)
        if missing:
            with closing(psycopg2.connect(dsn, application_name="erpdoo-snapshot")) as conn:
                conn.set_session(isolation_level="REPEATABLE READ")
                for table in missing:
                    lsn, rows = snapshot(conn, table)
                    count = self.load(table, rows, lsn)
                    log.info("state store: loaded %d rows of %s", count, table)


def snapshot(conn, table: str) -> tuple[int, Iterator[dict]]:
    """The WAL position of a snapshot of a table, and its rows, converted
    exactly like streamed rows: every column is read as text and converted by
    its type, as pgoutput values are. ``conn`` must use REPEATABLE READ, so
    the rows are as of that position."""
    with conn.cursor() as cur:
        # The first statement takes the snapshot; every transaction it sees
        # committed before this position.
        cur.execute("SELECT pg_current_wal_insert_lsn() - '0/0'")
        lsn = int(cur.fetchone()[0])
    return lsn, _rows(conn, table)


def _rows(conn, table: str) -> Iterator[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attname, atttypid FROM pg_attribute"
            " WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            [f"public.{table}"],
        )
        columns = cur.fetchall()
    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(name)) for name, _ in columns),
        sql.Identifier("public", table),
    )
    with conn.cursor(name=f"snapshot_{table}") as cur:
        cur.itersize = 5000
        cur.execute(query)
        for values in cur:
            yield {name: pgoutput.convert(oid, v) for (name, oid), v in zip(columns, values)}
    conn.commit()
