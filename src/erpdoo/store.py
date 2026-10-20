"""SQLite state store: a local copy of the Odoo tables that events include
(partners, products, order lines, ...), kept current from the replication
stream so events carry related records without querying Odoo.

Each tracked table is loaded once from a snapshot, then updated by streamed
changes. Changes from before the snapshot can be streamed again after it
(the slot may be behind); applying them in order still ends at the current
rows, so the copy converges.
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
from erpdoo.replication import Change

log = logging.getLogger(__name__)

_COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")


class StateStore:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                tbl TEXT NOT NULL, id INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (tbl, id));
            CREATE TABLE IF NOT EXISTS loaded_tables (tbl TEXT PRIMARY KEY);
            """
        )
        self.tables = {r[0] for r in self.db.execute("SELECT tbl FROM loaded_tables")}

    def reset(self) -> None:
        """Forget everything, e.g. when the replication slot was recreated and
        changes since the last run are lost."""
        self.db.execute("DELETE FROM records")
        self.db.execute("DELETE FROM loaded_tables")
        self.db.commit()
        self.tables.clear()

    def forget(self, table: str) -> None:
        """Stop tracking a table. Its rows would go stale, so they are dropped."""
        self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
        self.db.execute("DELETE FROM loaded_tables WHERE tbl = ?", [table])
        self.db.commit()
        self.tables.discard(table)

    def load(self, table: str, rows: Iterable[dict]) -> int:
        """Replace a table's rows with a snapshot and start tracking it."""
        self.db.execute("DELETE FROM records WHERE tbl = ?", [table])
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
        self.db.execute("INSERT OR IGNORE INTO loaded_tables VALUES (?)", [table])
        self.db.commit()
        self.tables.add(table)
        return count

    def index(self, column: str) -> None:
        """Index a column so find() on it is fast (one2many lookups)."""
        if not _COLUMN.match(column):
            raise ValueError(f"invalid column name {column!r}")
        self.db.execute(
            f"CREATE INDEX IF NOT EXISTS records_{column} ON records (tbl, json_extract(data, '$.{column}'))"
        )

    def apply(self, change: Change) -> None:
        """Apply one streamed change, if its table is tracked."""
        if change.table not in self.tables:
            return
        if change.op == "truncate":
            self.db.execute("DELETE FROM records WHERE tbl = ?", [change.table])
        elif change.op == "delete":
            self.db.execute("DELETE FROM records WHERE tbl = ? AND id = ?", [change.table, change.old["id"]])
        else:
            row = change.new
            if change.op == "update":
                # Unchanged TOASTed columns are not in the new row; keep the stored value.
                row = {**(self.get(change.table, row["id"]) or {}), **row}
            self.db.execute(
                "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT (tbl, id) DO UPDATE SET data = excluded.data",
                [change.table, row["id"], json.dumps(row)],
            )

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
                for table in missing:
                    count = self.load(table, snapshot(conn, table))
                    log.info("state store: loaded %d rows of %s", count, table)


def snapshot(conn, table: str) -> Iterator[dict]:
    """Read all rows of a table, converted exactly like streamed rows: every
    column is read as text and converted by its type, as pgoutput values are."""
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
