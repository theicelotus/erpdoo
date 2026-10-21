"""Business events built from committed transactions by YAML rules."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from erpdoo.models import ModelRegistry
from erpdoo.pgoutput import format_lsn
from erpdoo.replication import Transaction
from erpdoo.rules import Include, Rule, related_hops
from erpdoo.store import StateStore

log = logging.getLogger(__name__)

# Fixed namespace so the same transaction + rule + record always gives the same id.
EVENT_ID_NAMESPACE = uuid.UUID("6f1c3a52-2b0e-4d5c-9a51-0e7d1f6b8c34")


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    model: str
    record_id: int
    payload: dict

    @property
    def key(self) -> str:
        """Kafka key: keeps all events of one record in order on one partition."""
        return f"{self.model}:{self.record_id}"


def event_id(txn: Transaction, rule: str, model: str, record_id: int) -> str:
    # xid alone can wrap around on a long-lived database; with the commit LSN
    # the transaction identity is unique.
    name = f"{txn.xid}/{format_lsn(txn.commit_lsn)}/{rule}/{model}/{record_id}"
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))


def net_changes(txn: Transaction, model: str) -> dict[int, tuple[dict | None, dict | None]]:
    """Per record id of ``model``, the row at the start and at the end of the transaction.

    before is {} if the record was created in this transaction, and None if it
    is unknown (neither Postgres nor the state store had it). after is None if
    the record was deleted.
    """
    result: dict[int, tuple[dict | None, dict | None]] = {}
    for change in txn.changes:
        if change.model != model or change.op == "truncate":
            continue
        record_id = (change.new or change.old)["id"]
        if record_id in result:
            before, after = result[record_id]
        elif change.op == "insert":
            before, after = {}, {}
        elif change.old is not None and not change.old_is_key_only:
            before, after = change.old, dict(change.old)
        else:
            before, after = None, {}
        if change.op == "delete":
            after = None
        else:
            # Columns left out of the new row (unchanged TOAST) keep their earlier value.
            after = {**(after or {}), **change.new}
        result[record_id] = (before, after)
    return result


class EventBuilder:
    """Evaluates rules against committed transactions and renders events,
    taking related records from the state store."""

    def __init__(self, rules: list[Rule], registry: ModelRegistry, store: StateStore, lang: str = "en_US") -> None:
        self.rules = rules
        self.registry = registry
        self.store = store
        self.lang = lang
        self._hops: dict[tuple[str, str], list] = {}
        self._before_copy_logged: set[str] = set()
        self._as_of: int | None = None  # related records are read as of this WAL position

    def build(self, txn: Transaction, replay: bool = False) -> list[Event]:
        """Events for one transaction. Apply the transaction to the store
        first (StateStore.apply_transaction), so updates and deletes carry
        their previous row; ``replay`` is what that returned. Related records
        are read as of the transaction's commit, also when it is streamed
        again after a crash and the store is already further on."""
        self._as_of = txn.end_lsn
        events = []
        changes_by_model: dict[str, dict] = {}
        for rule in self.rules:
            if rule.model not in changes_by_model:
                changes_by_model[rule.model] = net_changes(txn, rule.model)
            for record_id, (before, after) in changes_by_model[rule.model].items():
                if before is None:
                    self._previous_unknown(rule.model, record_id, txn, replay)
                    continue
                if not rule.matches(before, self._with_related(rule, after)):
                    continue
                row = after if after is not None else before
                eid = event_id(txn, rule.name, rule.model, record_id)
                payload = {
                    "event_id": eid,
                    "event_type": rule.name,
                    "model": rule.model,
                    "record_id": record_id,
                    "occurred_at": txn.commit_time.isoformat(),
                    "changes": {
                        c.field: {"from": before.get(c.field), "to": (after or {}).get(c.field)}
                        for c in rule.conditions if c.is_transition
                    },
                    "record": self.render(rule.model, row, rule.fields, rule.includes),
                    "source": {
                        "table": self.registry.models[rule.model].table,
                        "xid": txn.xid,
                        "commit_lsn": format_lsn(txn.commit_lsn),
                    },
                }
                events.append(Event(eid, rule.name, rule.model, record_id, payload))
        return events

    def _previous_unknown(self, model: str, record_id: int, txn: Transaction, replay: bool) -> None:
        """Say why a record's rules were not checked."""
        table = self.registry.models[model].table
        if replay:
            # Versions are dropped once the slot is confirmed past them.
            log.debug("%s %s: already delivered in transaction %s, not sent again", model, record_id, txn.xid)
        elif txn.commit_lsn < self.store.loaded.get(table, 0):
            if table not in self._before_copy_logged:
                self._before_copy_logged.add(table)
                log.info("%s: changes committed before the state store copied the table are not checked"
                         " against rules", table)
        else:
            log.warning("%s %s: its previous row is neither in the state store nor sent by Postgres,"
                        " so its rules are not checked", model, record_id)

    def _with_related(self, rule: Rule, row: dict | None) -> dict | None:
        """The row plus the values of conditions on related fields or paths."""
        if row is None:
            return None
        columns = self.registry.models[rule.model].fields
        extra = {c.field: self.value(rule.model, row, c.field)
                 for c in rule.conditions if not (c.field in columns and columns[c.field].column)}
        return {**row, **extra} if extra else row

    def render(self, model: str, row: dict, fields, includes: dict[str, Include]) -> dict:
        m = self.registry.models[model]
        names = fields if fields is not None else [n for n, f in m.fields.items() if f.column]
        out = {"id": row.get("id")}
        for name in names:
            out[name] = self.value(model, row, name)
        for inc in includes.values():
            f = m.fields[inc.field]
            target = self.registry.models[f.relation]
            if f.type == "many2one":
                target_id = row.get(f.column)
                related = self.store.get(target.table, target_id, self._as_of) if target_id is not None else None
                if target_id is None:
                    out[inc.field] = None
                elif related is None:
                    out[inc.field] = {"id": target_id}  # not in the store (yet)
                else:
                    out[inc.field] = self.render(target.name, related, inc.fields, inc.children)
            else:
                inverse = target.fields[f.inverse].column
                rows = self.store.find(target.table, inverse, row["id"], self._as_of)
                out[inc.field] = [
                    self.render(target.name, r, inc.fields, inc.children)
                    for r in rows if all(c.matches({}, r) for c in inc.where)
                ]
        return out

    def value(self, model: str, row: dict, name: str) -> object:
        """A field's value: its column, or for a related field, the value at
        the end of its path of many2one fields, read from the store."""
        key = (model, name)
        if key not in self._hops:
            self._hops[key] = related_hops(self.registry, model, name)
        *hops, (_, f) = self._hops[key]
        current = row
        for _, hop in hops:
            target_id = current.get(hop.column)
            table = self.registry.models[hop.relation].table
            current = self.store.get(table, target_id, self._as_of) if target_id else None
            if current is None:
                return None
        value = current.get(f.column)
        if f.translate and isinstance(value, dict):
            value = value.get(self.lang) or value.get("en_US") or next(iter(value.values()), None)
        return value
