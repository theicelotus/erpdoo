"""Odoo model reader: maps Postgres tables to Odoo models and describes their
fields and relations, using ir_model, ir_model_fields and the Postgres catalog.

    python -m erpdoo.models sale.order
"""

from __future__ import annotations

import os
import sys
from contextlib import closing
from dataclasses import dataclass

import psycopg2

# Odoo core models whose table is not the model name with dots replaced by
# underscores (they set _table). ir_model does not record table names.
TABLE_OVERRIDES = {
    "ir.actions.actions": "ir_actions",
    "ir.actions.act_window": "ir_act_window",
    "ir.actions.act_window.view": "ir_act_window_view",
    "ir.actions.act_window_close": "ir_actions",
    "ir.actions.act_url": "ir_act_url",
    "ir.actions.server": "ir_act_server",
    "ir.actions.client": "ir_act_client",
    "ir.actions.report": "ir_act_report_xml",
}

RELATIONAL_TYPES = {"many2one", "one2many", "many2many"}
# Columns Odoo gives every model's table; they say nothing about which model it is.
MAGIC_COLUMNS = {"id", "create_uid", "create_date", "write_uid", "write_date"}


@dataclass(frozen=True)
class Field:
    name: str
    type: str  # Odoo field type (ttype): char, many2one, one2many, ...
    relation: str | None  # target model of a relational field
    column: str | None  # column in the model's table, if the field is stored there
    inverse: str | None  # one2many: the field on the target model that points back
    relation_table: str | None  # many2many: join table
    column1: str | None  # many2many: join table column holding this model's id
    column2: str | None  # many2many: join table column holding the target's id
    related: str | None = None  # path of a related field, e.g. "product_tmpl_id.name"
    translate: bool = False  # stored as jsonb {"en_US": ..., "fr_FR": ...}


@dataclass(frozen=True)
class Model:
    name: str
    table: str
    fields: dict[str, Field]

    def relational_fields(self) -> list[Field]:
        return [f for f in self.fields.values() if f.type in RELATIONAL_TYPES]


@dataclass(frozen=True)
class Reference:
    model: str
    id: int


@dataclass(frozen=True)
class Link:
    """One row of a many2many join table, seen from one side of the relation."""

    model: str
    id: int
    field: str
    target_model: str
    target_id: int


class ModelRegistry:
    def __init__(self, models: dict[str, Model], join_tables: dict[str, list[tuple[str, Field]]],
                 detected: dict[str, str] | None = None, problems: list[str] | None = None) -> None:
        self.models = models
        self.join_tables = join_tables
        # Models whose table was found by its columns, and tables that were
        # not found or not clear.
        self.detected = detected or {}
        self.problems = problems or []
        self._by_table: dict[str, str] = {}
        for model in models.values():
            current = self._by_table.get(model.table)
            # Several models can share a table (ir.actions.act_window_close
            # uses ir_actions); prefer the one the table is named after.
            if current is None or _default_table(model.name) == model.table:
                self._by_table[model.table] = model.name

    @classmethod
    def load(cls, dsn: str, overrides: dict[str, str] | None = None) -> ModelRegistry:
        with closing(psycopg2.connect(dsn, application_name="erpdoo-models")) as conn, conn.cursor() as cur:
            cur.execute("SELECT model FROM ir_model")
            model_names = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT model, name, ttype, relation, relation_field, relation_table, column1, column2, store,"
                " related, translate FROM ir_model_fields"
            )
            field_rows = cur.fetchall()
            cur.execute(
                "SELECT c.relname, a.attname FROM pg_class c"
                " JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'"
                " JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped"
                " WHERE c.relkind IN ('r', 'p')"
            )
            table_columns: dict[str, set[str]] = {}
            for table, column in cur.fetchall():
                table_columns.setdefault(table, set()).add(column)
        return cls.from_rows(model_names, field_rows, table_columns, overrides)

    @classmethod
    def from_rows(cls, model_names, field_rows, table_columns: dict[str, set[str]],
                  overrides: dict[str, str] | None = None) -> ModelRegistry:
        """Build the registry from ir_model names, ir_model_fields rows and the
        columns of each table. A model's table is, in order: ``overrides``
        (``tables:`` in erpdoo.yml), Odoo's naming rule with the core models
        known to differ, or else the one table no other model uses whose
        columns include every stored field of the model. Models without a
        table (abstract models, SQL views) are left out."""
        overrides = overrides or {}
        stored: dict[str, set[str]] = {}
        for model, name, ttype, *_, store, _related, _translate in field_rows:
            if store and ttype not in ("one2many", "many2many"):
                stored.setdefault(model, set()).add(name)
        tables, problems = {}, []
        for name in model_names:
            table = overrides.get(name) or TABLE_OVERRIDES.get(name, _default_table(name))
            if table in table_columns:
                tables[name] = table
            elif name in overrides:
                problems.append(f"{name}: table {table} from erpdoo.yml does not exist")
        detected = _detect_tables([m for m in model_names if m not in tables], stored, table_columns,
                                  set(tables.values()), problems)
        tables.update(detected)
        fields_by_model: dict[str, dict[str, Field]] = {name: {} for name in tables}
        for model, name, ttype, relation, inverse, rel_table, column1, column2, store, related, translate in field_rows:
            if model not in tables:
                continue
            is_column = store and ttype not in ("one2many", "many2many") and name in table_columns[tables[model]]
            fields_by_model[model][name] = Field(
                name, ttype, relation or None, name if is_column else None, inverse or None,
                rel_table if store and ttype == "many2many" else None,
                column1 or None, column2 or None, related or None, bool(translate),
            )
        models = {name: Model(name, tables[name], fields_by_model[name]) for name in tables}
        join_tables: dict[str, list[tuple[str, Field]]] = {}
        for model in models.values():
            for f in model.fields.values():
                if f.relation_table:
                    join_tables.setdefault(f.relation_table, []).append((model.name, f))
        return cls(models, join_tables, detected, problems)

    def model_for_table(self, table: str) -> str | None:
        return self._by_table.get(table)

    def references(self, model: str, row: dict[str, object]) -> dict[str, Reference]:
        """The records a row points to through its many2one columns."""
        refs = {}
        for f in self.models[model].fields.values():
            if f.type == "many2one" and f.column and row.get(f.column) is not None:
                refs[f.name] = Reference(f.relation, row[f.column])
        return refs

    def links(self, table: str, row: dict[str, object]) -> list[Link]:
        """What a many2many join table row links, from each side that declares
        the relation. A few tables are also a model's own table (Odoo uses
        mail_notification as the join table of mail.message.notified_partner_ids)."""
        return [
            Link(model, row[f.column1], f.name, f.relation, row[f.column2])
            for model, f in self.join_tables.get(table, [])
            if row.get(f.column1) is not None and row.get(f.column2) is not None
        ]


def _default_table(model: str) -> str:
    return model.replace(".", "_")


def _detect_tables(models, stored, table_columns, claimed: set[str], problems: list[str]) -> dict[str, str]:
    """Tables of models that set their own _table, found by their columns:
    a table with an id that no other model uses, whose columns include every stored field of
    the model besides the ones all tables have. Of several such tables, the
    one with the fewest other columns (columns of removed fields stay in
    Odoo's tables); if two are equally close, none."""
    found = {}
    for model in models:
        fields = stored.get(model, set())
        if not fields - MAGIC_COLUMNS:
            continue  # abstract models, or nothing to tell tables apart by
        # A model's table has an id; many2many join tables do not.
        candidates = sorted((len(columns - fields), table) for table, columns in table_columns.items()
                            if table not in claimed and "id" in columns and fields <= columns)
        if not candidates:
            continue  # no table: an abstract model or an SQL view
        if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
            problems.append(f"{model}: tables {candidates[0][1]} and {candidates[1][1]} both fit; set the table"
                            " under tables: in erpdoo.yml")
            continue
        found[model] = candidates[0][1]
        claimed.add(candidates[0][1])
    return found


def main() -> None:
    from erpdoo.config import load_config

    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    registry = ModelRegistry.load(dsn, load_config(os.environ.get("ERPDOO_CONFIG"))["tables"])
    print(f"{len(registry.models)} models with tables, {len(registry.join_tables)} many2many join tables")
    for model, table in sorted(registry.detected.items()):
        print(f"{model} -> table {table}, found by its columns")
    for problem in registry.problems:
        print(f"table of model {problem}")
    for name in sys.argv[1:]:
        model = registry.models[name]
        how = " (found by its columns)" if name in registry.detected else ""
        print(f"\n{model.name} -> table {model.table}{how}, {len(model.fields)} fields")
        for f in sorted(model.relational_fields(), key=lambda f: (f.type, f.name)):
            if f.type == "many2one":
                where = f"column {f.column}" if f.column else "not stored"
            elif f.type == "one2many":
                where = f"via {f.relation}.{f.inverse}"
            else:
                where = f"join {f.relation_table}({f.column1}, {f.column2})" if f.relation_table else "not stored"
            print(f"  {f.type:9} {f.name:32} -> {f.relation:28} {where}")


if __name__ == "__main__":
    main()
