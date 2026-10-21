"""Event schemas, generated from each rule and the Odoo field types.

Every event type gets one schema, as JSON Schema (draft-07) or Avro, that
describes exactly what erpdoo.events renders: the envelope (event_id,
event_type, model, record_id, occurred_at, changes, record, source), the
rule's fields with a type taken from the Odoo field and its Postgres column,
and each include as a nested record or a list of records.

Values keep the JSON representation erpdoo has always used, in both formats:

    boolean                 boolean
    integer, many2one ids   integer (Avro long)
    float without digits    number (double): a float8 column
    float with digits,      string: a numeric column, kept as text so no
    monetary                precision is lost ("1500.00")
    date                    string, "YYYY-MM-DD"
    datetime                string, "YYYY-MM-DD HH:MM:SS[.ffffff]", UTC
    translated fields       string, in the language erpdoo renders (ERPDOO_LANG)
    json, properties        any JSON value (Avro: the JSON text as a string)
    char, text, selection   string

Every value may be null. The schemas are written so that the usual rule
edits stay compatible under the registry's BACKWARD rule: only the envelope
is required, so adding a field or an include to a rule adds an optional
property. Objects are closed (no other properties), so a JSON Schema
consumer can tell a field was removed: removing one, or changing its type,
is a breaking change. In Avro every field defaults to null, so removing a
field is compatible there; changing its type is not.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from erpdoo.models import Field, ModelRegistry
from erpdoo.rules import Include, Rule, related_hops

DEFAULT_NAMESPACE = "erpdoo"
FORMATS = ("json", "avro")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NAMESPACE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

# Postgres column type -> kind of value in the event.
_KINDS = {
    "int2": "integer", "int4": "integer", "int8": "integer", "oid": "integer",
    "float4": "number", "float8": "number",
    "bool": "boolean",
    "numeric": "decimal",
    "date": "date",
    "timestamp": "datetime", "timestamptz": "datetime",
    "json": "json", "jsonb": "json",
}
# Column types Odoo uses for each field type, when the column type is not known.
_ODOO_COLUMNS = {
    "boolean": "bool", "integer": "int4", "many2one": "int4", "many2one_reference": "int4",
    "float": "numeric", "monetary": "numeric", "date": "date", "datetime": "timestamp",
    "json": "jsonb", "properties": "jsonb", "properties_definition": "jsonb",
}
_PATTERNS = {
    "decimal": (r"^(-?[0-9]+(\.[0-9]+)?|NaN|-?Infinity)$", "decimal number as text, e.g. \"1500.00\""),
    "date": (r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", "date, YYYY-MM-DD"),
    "datetime": (r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?([+-][0-9]{2}(:?[0-9]{2})?)?$",
                 "date and time in UTC, YYYY-MM-DD HH:MM:SS[.ffffff]"),
}


class SchemaError(Exception):
    pass


@dataclass
class Shape:
    """What one value of an event looks like.

    kind is a scalar kind (string, integer, number, boolean, decimal, date,
    datetime, json), "object" (``fields``, of which ``required`` are always
    present) or "array" (of ``items``). ``const`` fixes a string's value.
    """

    kind: str
    nullable: bool = True
    fields: dict[str, Shape] = field(default_factory=dict)
    required: tuple[str, ...] = ()
    items: Shape | None = None
    const: str | None = None
    doc: str | None = None


def value_kind(f: Field, resolved: bool = True) -> str:
    """The kind of value an event holds for field ``f``. With ``resolved``,
    translated fields are the text in one language, as records render them;
    without, the stored {"en_US": ...} mapping, as "changes" shows it."""
    if f.translate and resolved:
        return "string"
    column_type = f.column_type or ("jsonb" if f.translate else _ODOO_COLUMNS.get(f.type, "varchar"))
    return _KINDS.get(column_type, "string")


def _scalar(registry: ModelRegistry, model: str, name: str) -> Shape:
    *_, (owner, f) = related_hops(registry, model, name)
    return Shape(value_kind(f), doc=f"{owner}.{f.name} ({f.type})")


def _record(registry: ModelRegistry, model: str, names, includes: dict[str, Include], nullable: bool,
            doc: str) -> Shape:
    """The shape EventBuilder.render gives a row of ``model``."""
    m = registry.models[model]
    if names is None:
        names = [n for n, f in m.fields.items() if f.column]
    fields = {"id": Shape("integer", nullable=False, doc=f"{model}.id")}
    for name in names:
        if name != "id":
            fields[name] = _scalar(registry, model, name)
    for inc in includes.values():
        f = m.fields[inc.field]
        target = f.relation
        inner_doc = f"{target} via {model}.{inc.field} ({f.type})"
        if f.type == "many2one":
            # Null if not set; only the id if the related row is not known yet.
            fields[inc.field] = _record(registry, target, inc.fields, inc.children, True, inner_doc)
        else:
            row = _record(registry, target, inc.fields, inc.children, False, inner_doc)
            fields[inc.field] = Shape("array", nullable=False, items=row, doc=inner_doc)
    return Shape("object", nullable=nullable, fields=fields, required=("id",), doc=doc)


def event_shape(rule: Rule, registry: ModelRegistry) -> Shape:
    """The shape of the events of ``rule``, as EventBuilder.event renders them."""
    columns = registry.models[rule.model].fields
    changes = {}
    for c in rule.conditions:
        if c.is_transition:
            kind = value_kind(columns[c.field], resolved=False)
            # Nullable, so that a new transition condition is compatible in Avro.
            changes[c.field] = Shape("object", required=("from", "to"), fields={
                "from": Shape(kind, doc="value at the start of the transaction"),
                "to": Shape(kind, doc="value at its commit (null: deleted)"),
            }, doc=f"{rule.model}.{c.field}")
    string, integer = (lambda doc=None: Shape("string", nullable=False, doc=doc)), Shape("integer", nullable=False)
    return Shape("object", nullable=False, required=(
        "event_id", "event_type", "model", "record_id", "occurred_at", "changes", "record", "source"), fields={
        "event_id": string("same transaction, rule and record: same id"),
        "event_type": Shape("string", nullable=False, const=rule.name),
        "model": Shape("string", nullable=False, const=rule.model),
        "record_id": integer,
        "occurred_at": string("commit time, ISO 8601 with offset"),
        "changes": Shape("object", nullable=False, fields=changes, doc="fields of transition conditions"),
        "record": _record(registry, rule.model, rule.fields, rule.includes, False, rule.model),
        "source": Shape("object", nullable=False, required=("table",), fields={
            "table": string(),
            "xid": Shape("integer", doc="Postgres transaction id (streamed events)"),
            "commit_lsn": Shape("string", doc="WAL position of the commit (streamed events)"),
            "backfill": Shape("boolean", doc="rebuilt by erpdoo backfill"),
            "approximate": Shape("boolean", doc="backfilled from current values"),
            "mail_message_id": Shape("integer", doc="backfill: Odoo's tracking message"),
            "write_date": Shape("string", doc="backfill: the record's write_date"),
            "create_date": Shape("string", doc="backfill: the record's create_date"),
        }),
    }, doc=f"erpdoo event {rule.name} ({rule.model})")


def full_name(namespace: str, rule: Rule) -> str:
    """The record name of an event type's schema, which is also its subject
    (record name strategy)."""
    return f"{namespace}.{rule.name}"


def json_schema(rule: Rule, registry: ModelRegistry, namespace: str = DEFAULT_NAMESPACE) -> dict:
    schema = _json(event_shape(rule, registry))
    return {"$schema": "http://json-schema.org/draft-07/schema#", "title": full_name(namespace, rule), **schema}


def _json(shape: Shape) -> dict:
    out: dict = {}
    if shape.doc:
        out["description"] = shape.doc
    if shape.kind == "object":
        out["type"] = "object"
        out["properties"] = {name: _json(s) for name, s in shape.fields.items()}
        if shape.required:
            out["required"] = list(shape.required)
        out["additionalProperties"] = False
    elif shape.kind == "array":
        out["type"] = "array"
        out["items"] = _json(shape.items)
    elif shape.kind == "json":
        return out  # any JSON value
    else:
        out["type"] = {"decimal": "string", "date": "string", "datetime": "string"}.get(shape.kind, shape.kind)
        if shape.kind in _PATTERNS:
            out["pattern"], out["description"] = _PATTERNS[shape.kind][0], (
                f"{shape.doc}: {_PATTERNS[shape.kind][1]}" if shape.doc else _PATTERNS[shape.kind][1])
        if shape.const is not None:
            out["const"] = shape.const
    if shape.nullable:
        out["type"] = [out["type"], "null"]
    return out


def avro_schema(rule: Rule, registry: ModelRegistry, namespace: str = DEFAULT_NAMESPACE) -> dict:
    """The Avro schema of the events of ``rule``. Nested records are named
    after the event type and their path (SaleOrderConfirmed_record_partner_id),
    so the types of different events never clash."""
    if not _NAME.match(rule.name):
        raise SchemaError(f"rule {rule.name}: not a valid Avro record name (letters, digits and _ only)")
    schema = _avro(event_shape(rule, registry), rule.name, rule.name)
    return {**schema, "namespace": namespace}


def _avro(shape: Shape, name: str, path: str) -> dict | str | list:
    if shape.kind == "object":
        fields = []
        for key, s in shape.fields.items():
            if not _NAME.match(key):
                raise SchemaError(f"field {key} in {path}: not a valid Avro field name; use a related field"
                                  " (or an include) instead of a dotted path")
            entry = {"name": key, "type": _avro(s, f"{name}_{key}", f"{path}.{key}")}
            if s.doc:
                entry["doc"] = s.doc
            if s.nullable:
                entry["default"] = None
            elif s.kind == "array":
                entry["default"] = []  # so that adding an include is compatible
            fields.append(entry)
        schema: dict | str = {"type": "record", "name": name, "fields": fields}
        if shape.doc:
            schema["doc"] = shape.doc
    elif shape.kind == "array":
        schema = {"type": "array", "items": _avro(shape.items, name, f"{path}[]")}
    else:
        schema = {"integer": "long", "number": "double", "boolean": "boolean"}.get(shape.kind, "string")
    return ["null", schema] if shape.nullable else schema


def avro_datum(shape: Shape, value, path: str = "event"):
    """An event payload (or part of it) as Avro writes it: every field of a
    record present (null if the payload leaves it out, as for a related
    record not in the state store yet), JSON values as their JSON text.
    Raises SchemaError for a key the schema does not have."""
    if value is None:
        return None
    if shape.kind == "object":
        extra = value.keys() - shape.fields.keys()
        if extra:
            raise SchemaError(f"{path}: {sorted(extra)} not in the schema")
        return {key: avro_datum(s, value.get(key), f"{path}.{key}") for key, s in shape.fields.items()}
    if shape.kind == "array":
        return [avro_datum(shape.items, v, f"{path}[]") for v in value]
    if shape.kind == "json":
        return json.dumps(value)
    return value


def check_namespace(namespace: str) -> str:
    if not isinstance(namespace, str) or not _NAMESPACE.match(namespace):
        raise SchemaError(f"schema namespace {namespace!r}: expected dotted names of letters, digits and _,"
                          " e.g. erpdoo or acme.erpdoo")
    return namespace
