"""Polling mode: read Odoo's changes through its external JSON-RPC API
instead of Postgres, for hosted Odoo where erpdoo cannot reach the database.
erpdoo then needs no Postgres access at all, only a read-only Odoo user:

    source:
      type: polling
      interval: 5
      odoo: {url: https://example.odoo.com, db: example, login: erpdoo-reader, api_key: ...}

Every ``interval`` seconds, for the model of each rule, erpdoo reads the
records whose write_date is at or after its cursor minus ``overlap`` seconds.
The cursor is the newest write_date seen, on Odoo's clock. Odoo sets
write_date when a transaction starts, so a long transaction can commit after
the cursor has passed its write_date: reading the overlap again finds it, if
it took less than ``overlap`` seconds.

How a rule sees a transition:

- A rule whose transition fields Odoo tracks (tracking=True, shown in the
  chatter) takes them from Odoo's mail_tracking_value, as erpdoo backfill
  does: each tracking message is one write, with the exact old and new
  values and its time, so every transition is seen, also several between
  two polls. The messages handled are noted, so reading the overlap again
  sends nothing twice.
- Other rules compare the record with the values erpdoo stored for it at
  the previous poll. Several writes between two polls look like one, so
  intermediate states are lost; these events are marked approximate.

A record created since the previous poll is a create; its tracked fields
start at the old value of their first tracking message. When a model is
first polled, erpdoo stores the current values of all its records as a
baseline (as replication mode copies the rule tables); earlier changes give
no event.

Events carry "source": {"polling": true, "approximate": ..., and
"mail_message_id" or "write_date"}, and an id from the rule, the record and
the tracking message (the id erpdoo backfill gives the same change) or the
write_date and values. So an event sent again after a restart, because the
poll was delivered but not yet noted, has the same id.

Related records (includes, related fields) are read through the API for each
poll in batches, one call per model and level of includes, as Odoo has them
at the poll, not as of the change.

Limits, besides intermediate states of untracked fields:

- Latency is the poll interval (plus the poll itself); occurred_at is to
  the second, Odoo's write_date or tracking time rather than the commit.
- Deletes leave no write_date. For models with delete rules, every
  ``deletes`` seconds erpdoo compares the ids Odoo has with those it
  stored; a missing one is a delete, dated when it was found, with the
  values of the last poll. A record created and deleted between two polls
  is never seen, and one the user can no longer read looks deleted.
- Every poll costs API calls: one search_read per rule model, two for
  tracking messages when there are any, and a few per level of includes
  when events are built. Each poll that found something logs its count,
  and a summary is logged every minute.
- An event caused by an inbound message is marked as in replication mode,
  but matched by time to the second and by the record the message acted
  on, rather than by WAL position; a later write by someone else before the
  poll hides it.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from erpdoo.backfill import BACKFILL_NAMESPACE
from erpdoo.events import Event, EventBuilder
from erpdoo.models import TABLE_OVERRIDES, ModelRegistry, _default_table
from erpdoo.odoo_rpc import OdooClient
from erpdoo.replication import Change
from erpdoo.rules import Rule, related_hops, split_by_model, tracked_tables, validate
from erpdoo.store import StateStore, TableSpec, _covers, _keep

log = logging.getLogger(__name__)

POLLING_NAMESPACE = uuid.UUID("3b7e2f0c-8d14-4c6a-a5f9-61d2e0b7c4a8")
DATETIME = "%Y-%m-%d %H:%M:%S"
SECOND = 1_000_000  # microseconds
# Read with every rule record: the cursor, and who wrote it (the inbound loop marker).
META = ("write_date", "write_uid", "create_date")
FIELD_COLUMNS = ["model", "name", "ttype", "relation", "relation_field", "relation_table", "column1", "column2",
                 "store", "related", "translate", "tracking"]
TRACKING_COLUMNS = ["field_id", "mail_message_id"] + [
    f"{side}_value_{kind}" for side in ("old", "new") for kind in ("integer", "float", "char", "text", "datetime")]
# The Postgres column type Odoo creates for each field type. A float is
# numeric if it has digits, float8 if not; fields_get tells (see column_types).
COLUMN_TYPES = {
    "boolean": "bool", "integer": "int4", "many2one": "int4", "many2one_reference": "int4",
    "float": "numeric", "monetary": "numeric", "date": "date", "datetime": "timestamp",
    "json": "jsonb", "properties": "jsonb", "properties_definition": "jsonb",
    "char": "varchar", "selection": "varchar", "reference": "varchar", "text": "text", "html": "text",
}
PROGRESS_SECONDS = 5
SLACK = 2 * SECOND  # difference allowed between erpdoo's and Odoo's clocks, to tell inbound messages
POLL_SCHEMA = """
    -- Odoo tracking messages whose transitions were handled, by message id
    -- and its date (microseconds since 1970, UTC), so the overlap read again
    -- gives no event twice.
    CREATE TABLE IF NOT EXISTS polled_messages (id INTEGER PRIMARY KEY, at INTEGER NOT NULL);
"""
_UNKNOWN = object()


def micros(text: str) -> int:
    """An Odoo datetime ("YYYY-MM-DD HH:MM:SS", UTC) in microseconds since 1970."""
    return int(datetime.strptime(text[:19], DATETIME).replace(tzinfo=timezone.utc).timestamp()) * SECOND


def odoo_time(us: int) -> str:
    """Microseconds since 1970 as an Odoo datetime, to the second below."""
    return datetime.fromtimestamp(us // SECOND, timezone.utc).strftime(DATETIME)


def decimal_text(value: float, scale: int | None) -> str:
    """A numeric column's value as text, as Postgres gives it to replication:
    with the field's decimal places if known, else as few digits as needed."""
    if scale is not None:
        return f"{value:.{scale}f}"
    return format(Decimal(repr(float(value))), "f")


def chunks(items: Iterable, size: int) -> Iterator[list]:
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start:start + size]


def read_models(client: OdooClient, page: int = 2000) -> tuple[list[str], list[dict]]:
    """ir.model's names and ir.model.fields' rows, through the API."""
    names = [r["model"] for r in client.call("ir.model", "search_read", [], fields=["model"])]
    rows, last = [], 0
    while True:
        found = client.call("ir.model.fields", "search_read", [("id", ">", last)], fields=FIELD_COLUMNS,
                            order="id", limit=page)
        rows += found
        if len(found) < page:
            return names, rows
        last = found[-1]["id"]


def build_registry(names: list[str], fields: list[dict], overrides: dict[str, str],
                   column_types: dict[str, dict[str, str]]) -> ModelRegistry:
    """The registry polling works with. Without Postgres' catalog, a model's
    table is named by Odoo's rule (or ``tables:`` in erpdoo.yml); it only
    labels the model's rows. Column types come from the field types, and
    from ``column_types`` (model -> field -> type) where fields_get told more."""
    field_rows, tables = [], {}
    for r in fields:
        model, name, ttype = r["model"], r["name"], r["ttype"]
        field_rows.append((model, name, ttype, r["relation"] or None, r["relation_field"] or None,
                           r["relation_table"] or None, r["column1"] or None, r["column2"] or None, r["store"],
                           r["related"] or None, bool(r["translate"])))
        if r["store"] and ttype not in ("one2many", "many2many"):
            table = overrides.get(model) or TABLE_OVERRIDES.get(model, _default_table(model))
            tables.setdefault(table, {})[name] = (column_types.get(model, {}).get(name)
                                                  or ("jsonb" if r["translate"] else COLUMN_TYPES.get(ttype, "varchar")))
    return ModelRegistry.from_rows(names, field_rows, tables, overrides)


def column_types(info: dict[str, dict[str, dict]]) -> dict[str, dict[str, str]]:
    """Float columns by what fields_get says: Odoo makes a float with digits
    numeric (kept as text in events) and one without float8 (a number)."""
    return {model: {name: "numeric" if "digits" in desc else "float8"
                    for name, desc in fields.items() if desc.get("type") == "float"}
            for model, fields in info.items()}


class Reader:
    """Reads records through the API as erpdoo's rows hold them in
    replication mode: many2one as the id, numeric columns as text, empty
    values as None, translated fields in ``lang``. Archived records are
    included (active_test off), as the database has them."""

    def __init__(self, client: OdooClient, registry: ModelRegistry, info: dict[str, dict[str, dict]],
                 lang: str, batch: int) -> None:
        self.client = client
        self.registry = registry
        self.info = info
        self.batch = batch
        self.context = {"lang": lang, "active_test": False}
        self.places: dict[int, int] | None = None  # decimal places of each currency

    def search(self, model: str, domain: list, order: str | None = None) -> list[int]:
        return self.client.call(model, "search", domain, order=order, context=self.context)

    def search_read(self, model: str, domain: list, fields: list[str], order: str | None = None,
                    limit: int | None = None) -> list[dict]:
        raw = self.client.call(model, "search_read", domain, fields=fields, order=order, limit=limit,
                               context=self.context)
        return [self.row(model, r) for r in raw]

    def read(self, model: str, ids: Iterable[int], fields: list[str]) -> list[dict]:
        rows = []
        for chunk in chunks(sorted(set(ids)), self.batch):
            rows += [self.row(model, r) for r in self.client.call(model, "read", chunk, fields, context=self.context)]
        return rows

    def row(self, model: str, raw: dict) -> dict:
        return {name: self.value(model, name, value, raw) for name, value in raw.items()}

    def value(self, model: str, name: str, value, raw: dict | None = None):
        f = self.registry.models[model].fields.get(name)
        if name == "id" or f is None:
            return value
        if f.type == "boolean":
            return bool(value)
        if f.type in ("one2many", "many2many"):
            return list(value or [])
        if value is False or value is None:
            return None  # Odoo's API gives False for empty values
        if f.type == "many2one":
            return value[0] if isinstance(value, list) else value
        if f.column_type == "numeric":
            return decimal_text(value, self.scale(model, name, raw or {}))
        return value

    def scale(self, model: str, name: str, raw: dict) -> int | None:
        """Decimal places of a numeric field: its digits, or its currency's."""
        desc = self.info.get(model, {}).get(name, {})
        if desc.get("digits"):
            return desc["digits"][1]
        currency = raw.get(desc.get("currency_field") or "")
        if desc.get("type") != "monetary" or not currency:
            return None
        currency = currency[0] if isinstance(currency, list) else currency
        if self.places is None or currency not in self.places:
            self.places = {r["id"]: r["decimal_places"] for r in self.client.call(
                "res.currency", "search_read", [], fields=["decimal_places"], context=self.context)}
        return self.places.get(currency)


class Related:
    """Records read through the API for one poll: get() and find() as
    EventBuilder uses them on the state store. prefetch() reads what a rule
    renders for a set of records in batches, one call per model and level;
    anything still missing is read when asked for."""

    def __init__(self, reader: Reader, columns: dict[str, list[str]]) -> None:
        self.reader = reader
        self.registry = reader.registry
        self.columns = columns  # model -> fields to read
        self.rows: dict[str, dict[int, dict | None]] = {}  # table -> id -> row, None if it does not exist
        self.links: dict[tuple[str, str, int], list[dict]] = {}  # find() results

    def fields(self, model: str) -> list[str]:
        if model not in self.columns:
            self.columns[model] = ["id", *(n for n, f in self.registry.models[model].fields.items() if f.column)]
        return self.columns[model]

    def get(self, table: str, record_id: int, as_of=None) -> dict | None:
        rows = self.rows.setdefault(table, {})
        if record_id not in rows:
            self.load(self.registry.model_for_table(table), [record_id])
        return rows.get(record_id)

    def find(self, table: str, column: str, value: int, as_of=None) -> list[dict]:
        if (table, column, value) not in self.links:
            self._find(table, column, [value])
        return self.links[(table, column, value)]

    def load(self, model: str, ids: Iterable[int]) -> list[dict]:
        """The rows of these ids, reading those not read yet in one batch."""
        ids = {i for i in ids if i}
        rows = self.rows.setdefault(self.registry.models[model].table, {})
        missing = ids - rows.keys()
        if missing:
            for row in self.reader.read(model, missing, self.fields(model)):
                rows[row["id"]] = row
            for i in missing:
                rows.setdefault(i, None)  # deleted, or not visible to the user
        return [rows[i] for i in sorted(ids) if rows[i] is not None]

    def _find(self, table: str, column: str, values: list[int]) -> None:
        """One2many: the rows of a model whose column is one of ``values``,
        found by that column (so not filtered by the one2many's own domain,
        as the database link is not). Many2many: the join table's rows,
        from the many2many field of the records ``values``."""
        joins = [(model, f) for model, f in self.registry.join_tables.get(table, []) if f.column1 == column]
        if joins:
            model, f = joins[0]
            for row in self.reader.read(model, values, [f.name]):
                self.links[(table, column, row["id"])] = [{f.column1: row["id"], f.column2: target}
                                                          for target in sorted(row[f.name])]
        else:
            model = self.registry.model_for_table(table)
            rows = self.rows.setdefault(table, {})
            for chunk in chunks(values, self.reader.batch):
                found = self.reader.search_read(model, [(column, "in", chunk)], self.fields(model), order="id")
                for row in found:
                    rows[row["id"]] = row
                    self.links.setdefault((table, column, row[column]), []).append(row)
        for value in values:
            self.links.setdefault((table, column, value), [])

    def prefetch(self, model: str, rows: list[dict], names, includes, conditions=()) -> None:
        """Read in batches what rendering these rows of ``model`` needs: the
        records along related fields and conditions, and the includes."""
        m = self.registry.models[model]
        for name in [*(names if names is not None else [n for n, f in m.fields.items() if f.column]), *conditions]:
            current = rows
            for _, hop in related_hops(self.registry, model, name)[:-1]:
                current = self.load(hop.relation, {r.get(hop.column) for r in current})
        for inc in includes.values():
            f = m.fields[inc.field]
            target = self.registry.models[f.relation]
            ids = [r["id"] for r in rows]
            if f.type == "many2one":
                children = self.load(target.name, {r.get(f.column) for r in rows})
            elif f.type == "one2many":
                key = (target.table, target.fields[f.inverse].column)
                self._find(*key, [i for i in ids if (*key, i) not in self.links])
                children = [row for i in ids for row in self.links[(*key, i)]]
            else:
                key = (f.relation_table, f.column1)
                self._find(*key, [i for i in ids if (*key, i) not in self.links])
                children = self.load(target.name, {link[f.column2] for i in ids for link in self.links[(*key, i)]})
            self.prefetch(target.name, children, inc.fields, inc.children)


@dataclass
class Plan:
    """How one rule model is polled."""

    model: str
    table: str
    spec: TableSpec  # what is stored of its records
    columns: list[str]  # fields read
    rules: list[Rule]
    tracked: dict[str, dict]  # tracked fields the rules use: name -> {"id", "type", "labels"}
    by_tracking: set[str]  # rules whose transitions come from tracking messages

    @property
    def deletes(self) -> bool:
        return any("delete" in r.on for r in self.rules)


@dataclass
class Write:
    """One tracking message: a write of tracked fields on a record."""

    message_id: int
    model: str
    record_id: int
    at: int  # microseconds since 1970, UTC
    uid: int | None
    values: dict[str, tuple]  # field -> (old, new)


@dataclass
class Poll:
    """What one poll found, to deliver and then note (Poller.commit)."""

    number: int
    events: list[Event] = field(default_factory=list)
    changes: list[Change] = field(default_factory=list)  # for the viewer
    rows: dict[str, dict[int, dict]] = field(default_factory=dict)  # table -> rows to store
    deleted: dict[str, list[int]] = field(default_factory=dict)  # table -> ids gone from Odoo
    cursors: dict[str, int] = field(default_factory=dict)
    message_cursor: int | None = None
    messages: dict[int, int] = field(default_factory=dict)  # handled tracking messages: id -> date
    calls: int = 0  # API calls
    read: int = 0  # records read
    seconds: float = 0.0

    def summary(self) -> str:
        return (f"{self.read} records read, {len(self.changes)} changed, {len(self.events)} events,"
                f" {self.calls} API calls in {self.seconds:.1f} s")


class Poller:
    def __init__(self, client: OdooClient, settings: dict, store: StateStore, lang: str = "en_US",
                 overrides: dict[str, str] | None = None) -> None:
        self.client = client
        self.settings = settings
        self.store = store
        self.lang = lang
        self.overrides = overrides or {}
        self.overlap = int(settings["overlap"] * SECOND)
        self.batch = settings["batch"]
        self.store.db.executescript(POLL_SCHEMA)
        self.models: tuple[list[str], list[dict]] | None = None  # ir.model and ir.model.fields
        self.info: dict[str, dict[str, dict]] = {}  # fields_get per model
        self.registry: ModelRegistry | None = None
        self.rules: list[Rule] = []
        self.plans: dict[str, Plan] = {}
        self.builder: EventBuilder | None = None
        self.reader: Reader | None = None
        self.columns: dict[str, list[str]] = {}
        self.tracking = True  # the user may read mail.tracking.value
        self.polls = 0
        self.deletes_at = 0.0  # when deletes were last looked for (monotonic)
        self.fields_mark: tuple | None = None  # ir.model.fields' count and newest write_date

    def load(self) -> ModelRegistry:
        """Log in and read Odoo's models."""
        self.client.authenticate()
        self.fields_mark = self._fields_mark()
        self.models = read_models(self.client)
        self.registry = build_registry(*self.models, self.overrides, column_types(self.info))
        return self.registry

    def _fields_mark(self) -> tuple:
        newest = self.client.call("ir.model.fields", "search_read", [], fields=["write_date"],
                                  order="write_date desc, id desc", limit=1)
        return self.client.call("ir.model.fields", "search_count", []), newest[0]["write_date"] if newest else None

    def models_changed(self) -> bool:
        """Whether Odoo's models changed (a module was installed, updated or
        removed) since load(); two API calls."""
        return self._fields_mark() != self.fields_mark

    def activate(self, rules: list[Rule]) -> list[Rule]:
        """Poll for the rules whose model exists; returns the others. A model
        polled for the first time, or with more fields than before, gets a
        baseline of its records' current values first."""
        active, skipped = split_by_model(rules, self.registry)
        if self.settings["deletes"] is None:
            for rule in [r for r in active if "delete" in r.on]:
                log.warning("rule %s skipped: deletes are not looked for (deletes: false under source:)", rule.name)
                active.remove(rule)
        validate(active, self.registry)
        specs = tracked_tables(active, self.registry)
        used = {m for t in specs if (m := self.registry.model_for_table(t))} | {r.model for r in active}
        for model in sorted(used - self.info.keys()):
            self.info[model] = self.client.call(model, "fields_get", attributes=["type", "digits", "currency_field"])
        self.registry = build_registry(*self.models, self.overrides, column_types(self.info))
        validate(active, self.registry)
        self.tracking = self.client.call("mail.tracking.value", "has_access", [], "read")
        if not self.tracking:
            log.warning("polling: Odoo user %s may not read Odoo's field tracking (mail.tracking.value), so every"
                        " rule compares values between polls and its events are approximate; give it read access"
                        " (see \"Polling mode\" in the README)", self.client.login)
        self.columns = {}
        for table, spec in specs.items():
            model = self.registry.model_for_table(table)
            if model is None:
                continue  # a many2many join table: read through the many2many field
            names = spec.columns if spec.columns is not None else [
                n for n, f in self.registry.models[model].fields.items() if f.column]
            self.columns[model] = self._with_currencies(model, ["id", *sorted(set(names) - {"id"})])
        plans = {}
        for model in sorted({r.model for r in active}):
            table = self.registry.models[model].table
            rules_ = [r for r in active if r.model == model]
            tracked = self._tracked_fields(model, rules_) if self.tracking else {}
            by_tracking = {r.name for r in rules_ if (t := [c.field for c in r.conditions if c.is_transition])
                           and set(t) <= tracked.keys() and "update" in r.on}
            columns = [*self.columns[model], *(m for m in META if m not in self.columns[model])]
            plans[model] = Plan(model, table, specs[table], columns, rules_, tracked, by_tracking)
        for plan in plans.values():
            have = self.store.specs.get(plan.table)
            if have is None or not _covers(have, plan.spec) or self._meta(f"poll_cursor:{plan.model}") is None:
                self._baseline(plan)
        # Tracked fields not followed before (a new rule or model): their
        # messages so far are the baseline.
        new_fields = {f["id"] for plan in plans.values() for f in plan.tracked.values()
                      if self._meta(f"poll_tracking:{f['id']}") is None}
        for table in set(self.store.specs) - {p.table for p in plans.values()}:
            self.store.forget(table)  # no rule uses it any more
            self._set_meta(f"poll_cursor:{self.registry.model_for_table(table)}", None)
        self.plans = plans
        self.rules = active
        self.reader = Reader(self.client, self.registry, self.info, self.lang, self.batch)
        self.builder = EventBuilder(active, self.registry, None, self.lang)
        if new_fields:
            self._baseline_tracking(new_fields)
        self.store.commit()
        return skipped

    def check_read_only(self) -> list[str]:
        """What the Odoo user may do besides reading, on the models erpdoo
        reads (Odoo lets every internal user post chatter messages, so
        mail.message is left out). One API call per model and operation."""
        models = sorted({*self.columns, "ir.model", "ir.model.fields", *(["mail.tracking.value"] * self.tracking)})
        problems = []
        for model in models:
            allowed = [op for op in ("write", "create", "unlink") if self.client.call(model, "has_access", [], op)]
            if allowed:
                problems.append(f"{model} ({', '.join(allowed)})")
        if problems:
            log.warning("polling: Odoo user %s may change models erpdoo only reads: %s. erpdoo never does, but use"
                        " a read-only user (see \"Polling mode\" in the README)", self.client.login,
                        ", ".join(problems))
        else:
            log.info("polling: Odoo user %s may only read the %d models erpdoo reads", self.client.login, len(models))
        return problems

    def _with_currencies(self, model: str, names: list[str]) -> list[str]:
        """Add the currency fields of monetary fields, for their decimal places."""
        info = self.info.get(model, {})
        extra = [c for n in names if (c := info.get(n, {}).get("currency_field")) and c not in names
                 and c in self.registry.models[model].fields and self.registry.models[model].fields[c].column]
        return names + sorted(set(extra))

    def _tracked_fields(self, model: str, rules: list[Rule]) -> dict[str, dict]:
        """The condition fields of ``rules`` that Odoo tracks, with their
        id, type and, for selections, value by label in each active language
        (Odoo tracks a selection's label, in the language of whoever wrote)."""
        names = {c.field for r in rules for c in r.conditions}
        fields = {r["name"]: {"id": r["id"], "type": r["ttype"], "labels": {}}
                  for r in self.models[1] if r["model"] == model and r["name"] in names and r["tracking"]}
        selections = [n for n, f in fields.items() if f["type"] == "selection"]
        if selections:
            langs = [r["code"] for r in self.client.call("res.lang", "search_read", [("active", "=", True)],
                                                         fields=["code"])]
            for lang in sorted(set(langs) | {self.lang}):
                desc = self.client.call(model, "fields_get", selections, attributes=["selection"],
                                        context={"lang": lang})
                for name in selections:
                    for value, label in desc[name]["selection"]:
                        fields[name]["labels"].setdefault(label, value)
        return fields

    def _meta(self, key: str) -> int | None:
        row = self.store.db.execute("SELECT value FROM meta WHERE key = ?", [key]).fetchone()
        return row[0] if row else None

    def _set_meta(self, key: str, value: int | None) -> None:
        if value is None:
            self.store.db.execute("DELETE FROM meta WHERE key = ?", [key])
        else:
            self.store.db.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", [key, value])

    def _baseline(self, plan: Plan) -> None:
        """Store the current values of every record of the model; changes
        from now on are compared with them."""
        started, calls = time.monotonic(), self.client.calls
        reader = Reader(self.client, self.registry, self.info, self.lang, self.batch)
        top = reader.search_read(plan.model, [], ["write_date"], order="write_date desc, id desc", limit=1)
        cursor = micros(top[0]["write_date"]) if top and top[0]["write_date"] else 0
        self.store.forget(plan.table)
        count, last, logged = 0, 0, time.monotonic()
        while True:
            page = reader.search_read(plan.model, [("id", ">", last)], plan.columns, order="id", limit=self.batch)
            self._put(plan.table, [_keep(plan.spec, row) for row in page])
            count += len(page)
            if len(page) < self.batch:
                break
            last = page[-1]["id"]
            if time.monotonic() - logged >= PROGRESS_SECONDS:
                logged = time.monotonic()
                log.info("polling: reading %s for its baseline, %d records so far", plan.model, count)
        self.store.track(plan.table, plan.spec)
        self._set_meta(f"poll_cursor:{plan.model}", cursor)
        self._set_meta(f"poll_baseline:{plan.model}", cursor)
        self.store.commit()
        log.info("polling: stored the values of %d %s records as a baseline in %.1f s (%d API calls); changes"
                 " from now on give events", count, plan.model, time.monotonic() - started,
                 self.client.calls - calls)

    def _baseline_tracking(self, field_ids: set[int]) -> None:
        """Note the tracking messages of these fields in the window as
        handled, so that changes from before polling started give no event."""
        cursor = self._meta("poll_messages_cursor")
        if cursor is None:
            cursor = max((self._meta(f"poll_cursor:{p.model}") or 0 for p in self.plans.values()), default=0)
            self._set_meta("poll_messages_cursor", cursor)
        values = self._tracking_values(field_ids, cursor - self.overlap)
        ids = sorted({v["mail_message_id"][0] for v in values})
        for message in self.client.call("mail.message", "read", ids, ["date"], context=self.reader.context) if ids else ():
            self.store.db.execute("INSERT OR IGNORE INTO polled_messages VALUES (?, ?)",
                                  [message["id"], micros(message["date"])])
        for field_id in field_ids:
            self._set_meta(f"poll_tracking:{field_id}", 1)

    def _tracking_values(self, field_ids: Iterable[int], since: int) -> list[dict]:
        rows, last = [], 0
        while True:
            found = self.client.call("mail.tracking.value", "search_read", [
                ("field_id", "in", sorted(field_ids)), ("mail_message_id.date", ">=", odoo_time(since)),
                ("id", ">", last)], fields=TRACKING_COLUMNS, order="id", limit=self.batch)
            rows += found
            if len(found) < self.batch:
                return rows
            last = found[-1]["id"]

    def _put(self, table: str, rows: list[dict]) -> None:
        self.store.db.executemany(
            "INSERT INTO records VALUES (?, ?, ?) ON CONFLICT (tbl, id) DO UPDATE SET data = excluded.data",
            [(table, row["id"], json.dumps(row)) for row in rows])

    def stored(self, table: str, record_id: int) -> dict | None:
        row = self.store.db.execute("SELECT data FROM records WHERE tbl = ? AND id = ?", [table, record_id]).fetchone()
        return json.loads(row[0]) if row else None

    # Polling.

    def poll(self) -> Poll:
        """Read what changed since the last poll and build its events. Nothing
        is noted until commit(), after the events are delivered."""
        started, calls = time.monotonic(), self.client.calls
        self.polls += 1
        result = Poll(self.polls)
        related = Related(self.reader, dict(self.columns))
        self.builder.store = related
        rows = {}
        for plan in self.plans.values():
            rows[plan.model] = self._read_since(plan, self._meta(f"poll_cursor:{plan.model}") - self.overlap)
            result.read += len(rows[plan.model])
        writes = self._writes(result)
        by_record: dict[tuple[str, int], list[Write]] = {}
        for write in writes:
            by_record.setdefault((write.model, write.record_id), []).append(write)
        matches = []
        for plan in self.plans.values():
            current = rows[plan.model]
            missing = {i for m, i in by_record if m == plan.model} - current.keys()
            if missing:  # changed in a transaction that committed between the two reads
                current.update({r["id"]: r for r in self.reader.read(plan.model, missing, plan.columns)})
            conditions = [c.field for r in plan.rules for c in r.conditions]
            related.prefetch(plan.model, list(current.values()), [], {}, conditions)
            matches += self._evaluate(plan, current, by_record, result)
            seen = [micros(r["write_date"]) for r in current.values() if r.get("write_date")]
            result.cursors[plan.model] = max([self._meta(f"poll_cursor:{plan.model}"), *seen])
        if any(p.deletes for p in self.plans.values()) and (
                time.monotonic() - self.deletes_at >= self.settings["deletes"] or not self.deletes_at):
            self.deletes_at = time.monotonic()
            for plan in self.plans.values():
                if plan.deletes:
                    matches += self._deletes(plan, result)
        for rule in self.rules:
            mine = [m for m in matches if m[0] is rule]
            if mine:
                related.prefetch(rule.model, [m[3] or m[2] for m in mine], rule.fields, rule.includes)
        matches.sort(key=lambda m: (m[5], m[7], self.rules.index(m[0])))
        for rule, record_id, before, after, eid, occurred_at, source, _ in matches:
            result.events.append(self.builder.event(
                rule, record_id, before, after, eid, datetime.fromtimestamp(occurred_at / SECOND, timezone.utc),
                source))
        result.calls = self.client.calls - calls
        result.seconds = time.monotonic() - started
        return result

    def _read_since(self, plan: Plan, since: int) -> dict[int, dict]:
        """The records written at or after ``since``, by pages in order of
        write_date. Odoo's API gives write_date to the second, so each page
        starts again at the second the previous one ended in; a second with a
        page or more of records is read by ids."""
        rows: dict[int, dict] = {}
        start = odoo_time(max(since, 0))
        while True:
            page = self.reader.search_read(plan.model, [("write_date", ">=", start)], plan.columns,
                                           order="write_date, id", limit=self.batch)
            rows.update((r["id"], r) for r in page)
            if len(page) < self.batch:
                return rows
            last = page[-1]["write_date"]
            if page[0]["write_date"] == last:
                following = odoo_time(micros(last) + SECOND)
                ids = self.reader.search(plan.model, [("write_date", ">=", last), ("write_date", "<", following)])
                rows.update((r["id"], r) for r in self.reader.read(plan.model, set(ids) - rows.keys(), plan.columns))
                start = following
            else:
                start = last

    def _writes(self, result: Poll) -> list[Write]:
        """The tracking messages of the rules' tracked fields not handled yet,
        oldest first."""
        fields = {f["id"]: (plan, name, f) for plan in self.plans.values() for name, f in plan.tracked.items()}
        if not fields:
            return []
        cursor = self._meta("poll_messages_cursor") or 0
        values = self._tracking_values(fields, cursor - self.overlap)
        by_message: dict[int, list[dict]] = {}
        for v in values:
            by_message.setdefault(v["mail_message_id"][0], []).append(v)
        since = cursor - self.overlap
        handled = {r[0] for r in self.store.db.execute("SELECT id FROM polled_messages WHERE at >= ?", [since])}
        new = sorted(by_message.keys() - handled)
        messages = self.client.call("mail.message", "read", new, ["model", "res_id", "date", "create_uid"],
                                    context=self.reader.context) if new else []
        writes = []
        for message in messages:
            at = micros(message["date"])
            result.messages[message["id"]] = at
            result.message_cursor = max(result.message_cursor or cursor, at)
            values = {}
            for v in by_message[message["id"]]:
                plan, name, f = fields[v["field_id"][0]]
                old, new_ = (self._tracked_value(plan.model, name, f, v, side) for side in ("old", "new"))
                if _UNKNOWN in (old, new_):
                    log.warning("polling: %s %s: a tracked label in message %s matches no selection value of %s;"
                                " skipped", plan.model, message["res_id"], message["id"], name)
                    break
                values[name] = (old, new_)
            else:
                writes.append(Write(message["id"], plan.model, message["res_id"], at,
                                    message["create_uid"][0] if message["create_uid"] else None, values))
        writes.sort(key=lambda w: (w.at, w.message_id))
        return writes

    def _tracked_value(self, model: str, name: str, f: dict, v: dict, side: str):
        """A tracked old or new value, as erpdoo's rows hold it."""
        kind = f["type"]
        if kind in ("integer", "many2one"):
            value = v[f"{side}_value_integer"]
            return value or None if kind == "many2one" else value
        if kind == "boolean":
            return bool(v[f"{side}_value_integer"])
        if kind in ("float", "monetary"):
            value = v[f"{side}_value_float"]
            return decimal_text(value, None) if self.registry.models[model].fields[name].column_type == "numeric" else value
        if kind in ("date", "datetime"):
            value = v[f"{side}_value_datetime"]
            return (value[:10] if kind == "date" else value) if value else None
        if kind in ("text", "html"):
            return v[f"{side}_value_text"] or None
        label = v[f"{side}_value_char"] or None
        if kind == "selection" and label is not None:
            return f["labels"].get(label, _UNKNOWN)
        return label

    def _evaluate(self, plan: Plan, current: dict[int, dict], by_record: dict, result: Poll) -> list[tuple]:
        """The rules' matches among the records read, and what to store."""
        matches = []
        baseline = self._meta(f"poll_baseline:{plan.model}") or 0
        stored_rows = result.rows.setdefault(plan.table, {})
        for record_id, row in sorted(current.items()):
            kept = _keep(plan.spec, row)
            before = self.stored(plan.table, record_id)
            writes = by_record.get((plan.model, record_id), [])
            new = before is None and row.get("create_date") is not None and micros(row["create_date"]) >= baseline
            if before == kept and not writes:
                continue  # read again in the overlap, unchanged
            stored_rows[record_id] = kept
            result.changes.append(Change("insert" if new else "update", plan.table, plan.model, before, False, kept))
            if new:
                # As created: each tracked field at the old value of its first write.
                initial = dict(row)
                for write in reversed(writes):
                    initial.update({k: old for k, (old, _) in write.values.items()})
                for rule in plan.rules:
                    if "create" in rule.on and rule.matches({}, self.builder._with_related(rule, initial)):
                        matches.append(self._match(rule, row, {}, initial, "create", micros(row["create_date"]),
                                                   row.get("write_date") != row.get("create_date"),
                                                   {"write_date": row.get("write_date")}, row.get("write_uid")))
            for rule in plan.rules:
                if rule.name in plan.by_tracking:
                    matches += self._tracked_matches(rule, row, writes)
                elif before is not None:
                    matches += self._value_match(rule, row, before)
        return matches

    def _tracked_matches(self, rule: Rule, row: dict, writes: list[Write]) -> list[tuple]:
        """A tracked rule's transitions: one candidate per tracking message.
        The record is as it is now, with its tracked fields as they were right
        after the message's write (the old values of later messages undo
        later writes), and the message's own old values before it."""
        matches = []
        for i, write in enumerate(writes):
            after = dict(row)
            for later in reversed(writes[i + 1:]):
                after.update({k: old for k, (old, _) in later.values.items()})
            after.update({k: new for k, (_, new) in write.values.items()})
            before = {**after, **{k: old for k, (old, _) in write.values.items()}}
            if rule.matches(before, self.builder._with_related(rule, after)):
                eid = f"{rule.name}/{rule.model}/{row['id']}/message/{write.message_id}"
                matches.append(self._match(rule, row, before, after, eid, write.at, False,
                                           {"mail_message_id": write.message_id}, write.uid,
                                           namespace=BACKFILL_NAMESPACE, order=write.message_id))
        return matches

    def _value_match(self, rule: Rule, row: dict, before: dict) -> list[tuple]:
        """Another rule: the record now against its values at the previous poll."""
        after = dict(row)
        if not rule.matches(before, self.builder._with_related(rule, after)):
            return []
        changes = {c.field: [before.get(c.field), after.get(c.field)] for c in rule.conditions if c.is_transition}
        return [self._match(rule, row, before, after, f"{row['write_date']}/{json.dumps(changes, sort_keys=True)}",
                            micros(row["write_date"]), True, {"write_date": row.get("write_date")},
                            row.get("write_uid"))]

    def _deletes(self, plan: Plan, result: Poll) -> list[tuple]:
        """Records stored but no longer in Odoo: deleted since the last check."""
        have = {r[0] for r in self.store.db.execute("SELECT id FROM records WHERE tbl = ?", [plan.table])}
        gone = sorted(have - set(self.reader.search(plan.model, [])))
        result.deleted[plan.table] = gone
        now = int(time.time() * SECOND)
        matches = []
        for record_id in gone:
            before = self.stored(plan.table, record_id)
            result.changes.append(Change("delete", plan.table, plan.model, before, False, None))
            for rule in plan.rules:
                if rule.matches(before, None):
                    matches.append(self._match(rule, before, before, None, "delete", now, True, {}, None))
        return matches

    def _match(self, rule: Rule, row: dict, before: dict, after: dict | None, what: str, at: int,
               approximate: bool, detail: dict, uid: int | None, namespace=POLLING_NAMESPACE, order: int = 0):
        record_id = row["id"]
        name = what if namespace is BACKFILL_NAMESPACE else f"{rule.name}/{rule.model}/{record_id}/{what}"
        source = {"polling": True, "approximate": approximate, **detail}
        cause = self.store.inbound_cause_at(at, uid, rule.model, record_id, SLACK) if uid else None
        if cause:
            source["inbound"] = {"message_id": cause["message_id"], "topic": cause["topic"], "rule": cause["rule"]}
        return (rule, record_id, before, after, str(uuid.uuid5(namespace, name)), at, source, order)

    def commit(self, result: Poll) -> None:
        """Note a delivered poll: the records' values, the cursors and the
        tracking messages handled."""
        for table, rows in result.rows.items():
            self._put(table, list(rows.values()))
        for table, ids in result.deleted.items():
            self.store.db.executemany("DELETE FROM records WHERE tbl = ? AND id = ?", [(table, i) for i in ids])
        for model, cursor in result.cursors.items():
            self._set_meta(f"poll_cursor:{model}", cursor)
        if result.messages:
            self.store.db.executemany("INSERT OR IGNORE INTO polled_messages VALUES (?, ?)",
                                      list(result.messages.items()))
        if result.message_cursor is not None:
            self._set_meta("poll_messages_cursor", result.message_cursor)
            # Messages before the window can no longer be read again.
            self.store.db.execute("DELETE FROM polled_messages WHERE at < ?",
                                  [result.message_cursor - self.overlap - 60 * SECOND])
        # Inbound attempts before every cursor's window cannot be matched any more.
        cursors = [c for c in (*result.cursors.values(), result.message_cursor) if c]
        if cursors:
            self.store.prune_inbound_attempts(min(cursors) - self.overlap - 600 * SECOND)
        self.store.commit()

