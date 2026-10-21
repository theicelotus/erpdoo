"""Checks the smoke test runs on every event (see scripts/smoke_test.py).

- SchemaCheck: an event is valid against the schema it was written with,
  and that schema is registered under its event type's subject.
- same_event: an Avro-decoded event says the same as the JSON one.
- OdooCheck: every field of an event, related records and many2many lines
  included, equals what Odoo itself returns for it through JSON-RPC, read
  with Odoo's own field types (fields_get), not with erpdoo's code.
"""

from __future__ import annotations

import json
import math
import urllib.request
from collections import Counter
from decimal import Decimal, InvalidOperation

import fastavro
from jsonschema import Draft7Validator

from erpdoo.rules import Include, load_rules


class SchemaCheck:
    def __init__(self, client, registry_url: str) -> None:
        self.client = client
        self.url = registry_url.rstrip("/")
        self._schemas: dict[int, tuple[str, object]] = {}
        self._subjects: dict[int, list[str]] = {}

    def problems(self, event: dict, schema_id: int | None, namespace: str) -> list[str]:
        if schema_id is None:
            return ["written without a registered schema"]
        if schema_id not in self._schemas:
            schema = self.client.get_schema(schema_id)
            definition = json.loads(schema.schema_str)
            checker = (fastavro.parse_schema(definition) if schema.schema_type == "AVRO"
                       else Draft7Validator(definition, format_checker=Draft7Validator.FORMAT_CHECKER))
            self._schemas[schema_id] = (schema.schema_type, checker)
            with urllib.request.urlopen(f"{self.url}/schemas/ids/{schema_id}/versions", timeout=10) as r:
                self._subjects[schema_id] = [v["subject"] for v in json.load(r)]
        kind, checker = self._schemas[schema_id]
        problems = []
        subject = f"{namespace}.{event['event_type']}"
        if subject not in self._subjects[schema_id]:
            problems.append(f"schema {schema_id} is registered as {self._subjects[schema_id]}, not {subject}")
        if kind == "AVRO":
            if not fastavro.validation.validate(event, checker, raise_errors=False):
                problems.append(f"not valid against Avro schema {schema_id}")
        else:
            problems += [f"schema {schema_id}: {'.'.join(map(str, e.absolute_path)) or 'event'}: {e.message}"
                         for e in checker.iter_errors(event)]
        return problems


def same_event(avro: dict, plain: dict, path: str = "event") -> list[str]:
    """Differences between an Avro-decoded event and the JSON one. Avro has
    every field of a record, so a key the JSON leaves out is null there;
    a JSON value (jsonb column) is its JSON text."""
    if isinstance(avro, dict) and isinstance(plain, dict):
        problems = [f"{path}.{k}: missing in Avro" for k in plain.keys() - avro.keys()]
        for k, v in avro.items():
            if k in plain:
                problems += same_event(v, plain[k], f"{path}.{k}")
            elif v is not None:
                problems.append(f"{path}.{k}: {v!r} in Avro only")
        return problems
    if isinstance(avro, list) and isinstance(plain, list):
        if len(avro) != len(plain):
            return [f"{path}: {len(avro)} items in Avro, {len(plain)} in JSON"]
        return [p for i, (a, b) in enumerate(zip(avro, plain)) for p in same_event(a, b, f"{path}[{i}]")]
    if isinstance(avro, str) and isinstance(plain, (dict, list)):
        avro = json.loads(avro)
    return [] if avro == plain else [f"{path}: {avro!r} in Avro, {plain!r} in JSON"]


class OdooCheck:
    """Compares events with Odoo's records."""

    def __init__(self, odoo, rules_path: str = "rules", lang: str = "en_US") -> None:
        self.odoo = odoo
        self.rules = {r.name: r for r in load_rules(rules_path)}
        self.context = {"lang": lang}
        self._fields: dict[str, dict] = {}
        self.compared: Counter = Counter()  # kinds of values compared

    def fields(self, model: str) -> dict:
        if model not in self._fields:
            self._fields[model] = self.odoo(model, "fields_get",
                                            attributes=["type", "relation", "translate", "store"])
        return self._fields[model]

    def problems(self, event: dict) -> list[str]:
        rule = self.rules[event["event_type"]]
        problems = []
        if event["model"] != rule.model or event["record_id"] != event["record"]["id"]:
            problems.append(f"model {event['model']} record_id {event['record_id']} for {rule.model}"
                            f" record {event['record']['id']}")
        self._record(rule.model, event["record"], rule.fields, rule.includes, "record", problems)
        # A transition's "to" is the value at the commit: Odoo still has it.
        if event["changes"]:
            now = self._read(rule.model, event["record_id"], list(event["changes"]))
            for name, change in event["changes"].items():
                self._compare(rule.model, name, change["to"], now.get(name), f"changes.{name}.to", problems)
        return problems

    def _read(self, model: str, record_id: int, names: list[str]) -> dict:
        plain = [n for n in names if "." not in n]
        found = self.odoo(model, "read", [record_id], plain, context=self.context)
        if not found:
            return {}
        row = found[0]
        for name in names:
            if "." in name:  # a path through many2one fields
                first, rest = name.split(".", 1)
                target = self.odoo(model, "read", [record_id], [first], context=self.context)[0][first]
                relation = self.fields(model)[first]["relation"]
                row[name] = self._read(relation, target[0], [rest]).get(rest) if target else False
        return row

    def _record(self, model: str, record: dict, names, includes: dict[str, Include], path: str,
                problems: list[str]) -> None:
        fields = self.fields(model)
        if names is None:
            names = [n for n in record if n not in includes and n != "id"]
        expected = {"id", *names, *includes}
        if record.keys() != expected:
            problems.append(f"{path}: missing {sorted(expected - record.keys())}, unexpected"
                            f" {sorted(record.keys() - expected)} (keys the rule gives)")
        row = self._read(model, record["id"], [*names, *includes])
        if not row:
            problems.append(f"{path}: {model} {record['id']} not found in Odoo")
            return
        for name in names:
            if name in record:
                self._compare(model, name, record[name], row.get(name), f"{path}.{name}", problems)
        for name, inc in includes.items():
            if name not in record:
                continue
            f = fields[name]
            value = record[name]
            if f["type"] == "many2one":
                odoo_id = row[name][0] if row[name] else None
                got = value["id"] if value else None
                if got != odoo_id:
                    problems.append(f"{path}.{name}: {f['relation']} {got}, Odoo says {odoo_id}")
                elif value:
                    self._record(f["relation"], value, inc.fields, inc.children, f"{path}.{name}", problems)
                continue
            # one2many, many2many: Odoo's ids, kept by the include's "where", in id order.
            ids = sorted(row[name])
            if inc.where:
                linked = self.odoo(f["relation"], "read", ids, [c.field for c in inc.where], context=self.context)
                ids = [r["id"] for r in linked
                       if all(_plain(r[c.field]) in {_plain(v) for v in c.equals} for c in inc.where)]
            got = [item["id"] for item in value]
            if got != sorted(ids):
                problems.append(f"{path}.{name}: {f['relation']} {got}, Odoo says {sorted(ids)}")
                continue
            self.compared[f["type"]] += 1
            for i, item in enumerate(value):
                self._record(f["relation"], item, inc.fields, inc.children, f"{path}.{name}[{i}]", problems)

    def _compare(self, model: str, name: str, got, odoo_value, path: str, problems: list[str]) -> None:
        f = self._field(model, name)
        kind = f["type"]
        if f.get("translate"):
            kind = "translated"
        ok = _equal(f["type"], got, odoo_value)
        self.compared[kind] += 1
        if not ok:
            problems.append(f"{path}: {got!r}, Odoo says {odoo_value!r} ({f['type']})")

    def _field(self, model: str, name: str) -> dict:
        first, _, rest = name.partition(".")
        f = self.fields(model)[first]
        return self._field(f["relation"], rest) if rest else f


def _plain(value):
    """Odoo's False for an empty value, as None."""
    return None if value is False else value


def _equal(kind: str, got, odoo_value) -> bool:
    """Whether an event's value is Odoo's, by Odoo field type. Odoo reads an
    empty value as False (0 for numbers); events have null."""
    if kind == "boolean":
        return isinstance(got, (bool, type(None))) and bool(got) == odoo_value
    if kind == "integer":
        return (got or 0) == odoo_value
    if kind in ("float", "monetary"):
        if got is None:
            return odoo_value in (0, False)
        try:
            number = float(Decimal(got)) if isinstance(got, str) else float(got)
        except (InvalidOperation, ValueError, TypeError):
            return False
        return math.isclose(number, odoo_value, rel_tol=1e-9, abs_tol=1e-9)
    if kind == "many2one":
        return got == (odoo_value[0] if odoo_value else None)
    if kind in ("one2many", "many2many"):
        return sorted(got or []) == sorted(odoo_value or [])
    if kind == "datetime" and isinstance(got, str):
        # Odoo reads datetimes to the second; write_date and create_date
        # have microseconds in the database.
        return got[:19] == odoo_value
    return _plain(got) == _plain(odoo_value)
