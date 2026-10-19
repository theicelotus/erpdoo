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
    def __init__(self, models: dict[str, Model], join_tables: dict[str, list[tuple[str, Field]]]) -> None:
        self.models = models
        self.join_tables = join_tables
        self._by_table: dict[str, str] = {}
        for model in models.values():
            current = self._by_table.get(model.table)
            # Several models can share a table (ir.actions.act_window_close
            # uses ir_actions); prefer the one the table is named after.
            if current is None or _default_table(model.name) == model.table:
                self._by_table[model.table] = model.name

    @classmethod
    def load(cls, dsn: str) -> ModelRegistry:
        with closing(psycopg2.connect(dsn, application_name="erpdoo-models")) as conn, conn.cursor() as cur:
            cur.execute("SELECT model FROM ir_model")
            model_names = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT model, name, ttype, relation, relation_field, relation_table, column1, column2, store"
                " FROM ir_model_fields"
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
        return cls.from_rows(model_names, field_rows, table_columns)

    @classmethod
    def from_rows(cls, model_names, field_rows, table_columns: dict[str, set[str]]) -> ModelRegistry:
        """Build the registry from ir_model names, ir_model_fields rows and the
        columns of each table. Models without a table (abstract models, SQL
        views) are left out."""
        fields_by_model: dict[str, dict[str, Field]] = {}
        tables = {}
        for name in model_names:
            table = TABLE_OVERRIDES.get(name, _default_table(name))
            if table in table_columns:
                tables[name] = table
                fields_by_model[name] = {}
        for model, name, ttype, relation, inverse, rel_table, column1, column2, store in field_rows:
            if model not in tables:
                continue
            is_column = store and ttype not in ("one2many", "many2many") and name in table_columns[tables[model]]
            fields_by_model[model][name] = Field(
                name, ttype, relation or None, name if is_column else None, inverse or None,
                rel_table if store and ttype == "many2many" else None,
                column1 or None, column2 or None,
            )
        models = {name: Model(name, tables[name], fields_by_model[name]) for name in tables}
        join_tables: dict[str, list[tuple[str, Field]]] = {}
        for model in models.values():
            for f in model.fields.values():
                if f.relation_table:
                    join_tables.setdefault(f.relation_table, []).append((model.name, f))
        return cls(models, join_tables)

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


def main() -> None:
    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    registry = ModelRegistry.load(dsn)
    print(f"{len(registry.models)} models with tables, {len(registry.join_tables)} many2many join tables")
    for name in sys.argv[1:]:
        model = registry.models[name]
        print(f"\n{model.name} -> table {model.table}, {len(model.fields)} fields")
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
