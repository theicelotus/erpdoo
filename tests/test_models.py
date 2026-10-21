from erpdoo.models import Link, ModelRegistry, Reference

MODELS = ["sale.order", "sale.order.line", "res.partner", "crm.tag", "mail.thread",
          "ir.actions.actions", "ir.actions.act_window", "ir.actions.act_window_close"]
# model, name, ttype, relation, relation_field, relation_table, column1, column2, store, related, translate
FIELDS = [
    ("sale.order", "id", "integer", None, None, None, None, None, True, None, False),
    ("sale.order", "state", "selection", None, None, None, None, None, True, None, False),
    ("sale.order", "partner_id", "many2one", "res.partner", None, None, None, None, True, None, False),
    ("sale.order", "tax_country_id", "many2one", "res.country", None, None, None, None, False, None, False),
    ("sale.order", "order_line", "one2many", "sale.order.line", "order_id", None, None, None, True, None, False),
    ("sale.order", "tag_ids", "many2many", "crm.tag", None, "sale_order_tag_rel", "order_id", "tag_id", True, None, False),
    ("sale.order", "invoice_ids", "many2many", "account.move", None, None, None, None, False, None, False),
    ("sale.order.line", "order_id", "many2one", "sale.order", None, None, None, None, True, None, False),
    ("mail.thread", "message_ids", "one2many", "mail.message", "res_id", None, None, None, True, None, False),
]
TABLES = {
    "sale_order": {"id", "state", "partner_id"},
    "sale_order_line": {"id", "order_id"},
    "res_partner": {"id"},
    "crm_tag": {"id"},
    "sale_order_tag_rel": {"order_id", "tag_id"},
    "ir_actions": {"id"},
    "ir_act_window": {"id"},
}


def registry():
    return ModelRegistry.from_rows(MODELS, FIELDS, TABLES)


def test_tables_map_to_models():
    r = registry()
    assert r.model_for_table("sale_order") == "sale.order"
    assert r.model_for_table("sale_order_line") == "sale.order.line"
    assert r.model_for_table("sale_order_tag_rel") is None
    # Abstract models have no table.
    assert "mail.thread" not in r.models


def test_custom_table_names_and_shared_tables():
    r = registry()
    assert r.models["ir.actions.act_window"].table == "ir_act_window"
    assert r.model_for_table("ir_act_window") == "ir.actions.act_window"
    # ir.actions.act_window_close also uses ir_actions; the table keeps its namesake.
    assert r.models["ir.actions.act_window_close"].table == "ir_actions"
    assert r.model_for_table("ir_actions") == "ir.actions.actions"


def test_fields_know_their_columns_and_relations():
    fields = registry().models["sale.order"].fields
    assert fields["partner_id"].column == "partner_id" and fields["partner_id"].relation == "res.partner"
    assert fields["tax_country_id"].column is None  # computed, not stored
    assert fields["order_line"].column is None and fields["order_line"].inverse == "order_id"
    assert fields["tag_ids"].relation_table == "sale_order_tag_rel"
    assert fields["invoice_ids"].relation_table is None  # computed many2many


def test_references_and_links():
    r = registry()
    row = {"id": 7, "state": "sale", "partner_id": 10}
    assert r.references("sale.order", row) == {"partner_id": Reference("res.partner", 10)}
    assert r.references("sale.order", {"id": 8, "partner_id": None}) == {}
    assert r.links("sale_order_tag_rel", {"order_id": 7, "tag_id": 3}) == [
        Link("sale.order", 7, "tag_ids", "crm.tag", 3)]


def gadget_rows():
    # A model that sets _table = "x_custom_gadgets", and an abstract mixin.
    fields = [(m, n, t, r, None, None, None, None, True, None, False) for m, n, t, r in [
        ("x.gadget", "id", "integer", None), ("x.gadget", "name", "char", None),
        ("x.gadget", "state", "selection", None), ("x.gadget", "partner_id", "many2one", "res.partner"),
        ("x.gadget", "line_ids", "one2many", "x.gadget.line"),
        ("x.mixin", "id", "integer", None), ("x.mixin", "create_date", "datetime", None)]]
    tables = {"x_custom_gadgets": {"id", "name", "state", "partner_id", "create_date", "old_note"},
              "res_partner": {"id", "name", "state"}, "x_unrelated": {"id", "name"}}
    return ["x.gadget", "x.mixin", "res.partner"], fields, tables


def test_table_found_by_its_columns():
    r = ModelRegistry.from_rows(*gadget_rows())
    assert r.models["x.gadget"].table == "x_custom_gadgets"
    assert r.model_for_table("x_custom_gadgets") == "x.gadget"
    assert r.detected == {"x.gadget": "x_custom_gadgets"}
    assert r.models["x.gadget"].fields["state"].column == "state"
    # Only magic fields: nothing to tell tables apart by.
    assert "x.mixin" not in r.models and r.problems == []


def test_table_from_the_config_wins():
    models, fields, tables = gadget_rows()
    tables["x_gadgets_v2"] = {"id", "name", "state", "partner_id"}
    r = ModelRegistry.from_rows(models, fields, tables, {"x.gadget": "x_custom_gadgets"})
    assert r.models["x.gadget"].table == "x_custom_gadgets" and r.detected == {}
    r = ModelRegistry.from_rows(models, fields, tables, {"x.gadget": "missing"})
    assert r.problems == ["x.gadget: table missing from erpdoo.yml does not exist"]


def test_two_equally_close_tables_are_not_guessed():
    models, fields, tables = gadget_rows()
    tables["x_gadgets_copy"] = set(tables["x_custom_gadgets"])
    r = ModelRegistry.from_rows(models, fields, tables)
    assert "x.gadget" not in r.models
    assert "both fit" in r.problems[0]
