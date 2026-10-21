"""Backfill: rebuild the events the rules would have produced in a past
time window, from what Odoo itself recorded, when the replication stream
cannot provide them (an invalidated slot, erpdoo installed later).

    erpdoo backfill --since 2026-10-21T00:44:03Z --until 2026-10-21T00:45:00Z

For a rule whose transition fields Odoo tracks (``tracking=True``, shown in
the chatter), each tracked change in the window is one candidate: Odoo's
mail_tracking_value rows give the field's old and new values, and the
mail_message they belong to gives the record and the time. The rule is
checked against the record's current row with those values in place, so
other fields (and conditions on related records) are as they are now.

For other rules (a transition field Odoo does not track, or a create rule),
the candidates are the records whose write_date (create_date) falls in the
window and whose current values match the rule; their previous values are
unknown, so "from" conditions are not checked and these events are marked
approximate. Delete rules cannot be backfilled.

Every backfilled event has "source": {"backfill": true, "approximate": ...}
and an id derived from the rule, the record and the tracking message (or
write_date), so running the same backfill again gives the same ids. They
differ from the ids of events built from the stream.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from erpdoo.events import Event, EventBuilder
from erpdoo.models import ModelRegistry
from erpdoo.rules import Rule, tracked_tables
from erpdoo.store import StateStore, TableSpec

log = logging.getLogger(__name__)

BACKFILL_NAMESPACE = uuid.UUID("9d1b6a3e-5c2f-4f0e-8b7a-2e4c6d8f1a03")

# What the backfill reads besides the tables erpdoo already reads. Only these
# columns of mail_message: not the messages themselves.
GRANTS = ("GRANT SELECT (id, model, res_id, date) ON mail_message TO {role};\n"
          "GRANT SELECT ON mail_tracking_value, ir_model_fields_selection TO {role};")


class BackfillError(Exception):
    pass


@dataclass
class Backfill:
    events: list[Event] = field(default_factory=list)
    tracked: int = 0  # events from Odoo's field tracking
    approximate: int = 0  # events from write_date and current values
    skipped: list[str] = field(default_factory=list)  # rules that cannot be backfilled, and why

    def summary(self) -> str:
        return (f"{len(self.events)} events ({self.tracked} from Odoo's field tracking,"
                f" {self.approximate} approximate)")


def parse_time(text: str) -> datetime:
    """An ISO 8601 time; without a timezone it is UTC, as Odoo stores times."""
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def window(since: datetime, until: datetime) -> tuple[datetime, datetime]:
    """The window widened to whole seconds: Odoo stores message and write
    times without fractions, so a change in the second ``since`` falls in
    would otherwise be missed. Changes in that second may be sent twice."""
    since = since.astimezone(timezone.utc).replace(microsecond=0)
    if until.microsecond:
        until = until.replace(microsecond=0) + timedelta(seconds=1)
    return since, until.astimezone(timezone.utc)


def run(rules: list[Rule], registry: ModelRegistry, dsn: str, since: datetime, until: datetime,
        lang: str = "en_US") -> Backfill:
    """The events ``rules`` would have produced between ``since`` and ``until``."""
    since, until = window(since, until)
    store = StateStore(":memory:", dsn)
    # Everything is read from Postgres as it is now, row by row.
    for table, spec in tracked_tables(rules, registry).items():
        store.track(table, TableSpec(spec.columns, False, spec.lookups))
    builder = EventBuilder(rules, registry, store, lang)
    result = Backfill()
    with closing(psycopg2.connect(dsn, application_name="erpdoo-backfill")) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            _check_privileges(cur)
            for rule in rules:
                transitions = [c.field for c in rule.conditions if c.is_transition]
                tracked = _tracked_fields(cur, rule.model, transitions)
                if transitions and "update" in rule.on and set(transitions) <= tracked.keys():
                    _from_tracking(cur, builder, rule, tracked, since, until, result)
                elif rule.on & {"create", "update"}:
                    _from_write_date(cur, builder, rule, since, until, result)
                else:
                    result.skipped.append(f"{rule.name}: delete rules cannot be backfilled")
    result.events.sort(key=lambda e: (e.payload["occurred_at"], e.event_id))
    return result


def _check_privileges(cur) -> None:
    cur.execute("SELECT current_user AS role, has_table_privilege('mail_tracking_value', 'SELECT') AND"
                " has_table_privilege('ir_model_fields_selection', 'SELECT') AND"
                " has_column_privilege('mail_message', 'id', 'SELECT') AND"
                " has_column_privilege('mail_message', 'model', 'SELECT') AND"
                " has_column_privilege('mail_message', 'res_id', 'SELECT') AND"
                " has_column_privilege('mail_message', 'date', 'SELECT') AS ok")
    row = cur.fetchone()
    if not row["ok"]:
        raise BackfillError("backfill reads Odoo's field tracking, which erpdoo's role may not read. Run this as"
                            " an admin:\n\n" + GRANTS.format(role=row["role"]))


def _tracked_fields(cur, model: str, names: list[str]) -> dict[str, dict]:
    """Of ``names``, the fields of ``model`` Odoo tracks, with their id, type
    and, for selections, value by label (in any language)."""
    cur.execute("SELECT id, name, ttype FROM ir_model_fields WHERE model = %s AND name = ANY(%s)"
                " AND tracking IS NOT NULL", [model, names])
    fields = {r["name"]: {"id": r["id"], "type": r["ttype"], "labels": {}} for r in cur.fetchall()}
    for f in fields.values():
        if f["type"] == "selection":
            cur.execute("SELECT value, name FROM ir_model_fields_selection WHERE field_id = %s ORDER BY sequence, id",
                        [f["id"]])
            for r in cur.fetchall():
                for label in (r["name"] or {}).values():
                    f["labels"].setdefault(label, r["value"])
    return fields


_UNKNOWN = object()


def _value(f: dict, row: dict, side: str):
    """A tracked value, as erpdoo's rows hold it."""
    kind = f["type"]
    if kind in ("integer", "many2one"):
        return row[f"{side}_value_integer"]
    if kind == "boolean":
        value = row[f"{side}_value_integer"]
        return None if value is None else bool(value)
    if kind in ("float", "monetary"):
        return row[f"{side}_value_float"]
    if kind in ("date", "datetime"):
        value = row[f"{side}_value_datetime"]
        return None if value is None else str(value.date() if kind == "date" else value)
    if kind == "text":
        return row[f"{side}_value_text"]
    label = row[f"{side}_value_char"]
    if kind == "selection" and label is not None:
        # Odoo tracks a selection's label, not its value.
        return f["labels"].get(label, _UNKNOWN)
    return label


def _from_tracking(cur, builder: EventBuilder, rule: Rule, tracked: dict, since, until, result: Backfill) -> None:
    by_id = {f["id"]: (name, f) for name, f in tracked.items()}
    cur.execute(
        "SELECT m.id AS message_id, m.res_id, m.date, v.*"
        " FROM mail_tracking_value v JOIN mail_message m ON m.id = v.mail_message_id"
        " WHERE v.field_id = ANY(%s) AND m.model = %s AND m.date >= %s AND m.date < %s"
        " ORDER BY m.date, m.id, v.id",
        [list(by_id), rule.model, since.replace(tzinfo=None), until.replace(tzinfo=None)],
    )
    # One message per write: the fields it changed, old and new.
    writes: dict[int, dict] = {}
    for row in cur.fetchall():
        write = writes.setdefault(row["message_id"], {"res_id": row["res_id"], "date": row["date"], "values": {}})
        name, f = by_id[row["field_id"]]
        write["values"][name] = (_value(f, row, "old"), _value(f, row, "new"))
    table = builder.registry.models[rule.model].table
    for message_id, write in writes.items():
        if any(_UNKNOWN in pair for pair in write["values"].values()):
            log.warning("backfill %s: %s %s: a tracked label in message %s matches no selection value, skipped",
                        rule.name, rule.model, write["res_id"], message_id)
            continue
        current = builder.store.get(table, write["res_id"])
        if current is None:
            continue  # deleted since
        before = {**current, **{k: old for k, (old, _) in write["values"].items()}}
        after = {**current, **{k: new for k, (_, new) in write["values"].items()}}
        if not rule.matches(before, builder._with_related(rule, after)):
            continue
        eid = str(uuid.uuid5(BACKFILL_NAMESPACE, f"{rule.name}/{rule.model}/{write['res_id']}/message/{message_id}"))
        result.events.append(builder.event(
            rule, write["res_id"], before, after, eid, write["date"].replace(tzinfo=timezone.utc),
            {"backfill": True, "approximate": False, "mail_message_id": message_id}))
        result.tracked += 1


def _from_write_date(cur, builder: EventBuilder, rule: Rule, since, until, result: Backfill) -> None:
    column = "write_date" if "update" in rule.on else "create_date"
    table = builder.registry.models[rule.model].table
    cur.execute(sql.SQL("SELECT id, {col} AS at FROM {table} WHERE {col} >= %s AND {col} < %s ORDER BY {col}, id")
                .format(col=sql.Identifier(column), table=sql.Identifier("public", table)),
                [since.replace(tzinfo=None), until.replace(tzinfo=None)])
    for row in cur.fetchall():
        current = builder.store.get(table, row["id"])
        if current is None or not _matches_now(rule, builder._with_related(rule, current)):
            continue
        eid = str(uuid.uuid5(BACKFILL_NAMESPACE, f"{rule.name}/{rule.model}/{row['id']}/{column}/{row['at'].isoformat()}"))
        # The previous values are unknown: "from" is null.
        event = builder.event(rule, row["id"], {}, current, eid, row["at"].replace(tzinfo=timezone.utc),
                              {"backfill": True, "approximate": True, column: row["at"].isoformat()})
        result.events.append(event)
        result.approximate += 1


def _matches_now(rule: Rule, row: dict) -> bool:
    """Whether a record's current values fit the rule, as far as they can
    tell: plain values, and the "to" of transitions."""
    for c in rule.conditions:
        value = row.get(c.field)
        if c.equals is not None and value not in c.equals:
            return False
        if c.to is not None and value not in c.to:
            return False
    return True
