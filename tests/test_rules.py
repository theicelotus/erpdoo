import pytest
import yaml

import fake_odoo
from erpdoo.rules import (
    Condition, RuleError, YamlLoader, missing_model_message, parse_rule, split_by_model, tracked_tables, validate,
)


def test_yaml_keeps_on_and_no_as_strings():
    doc = yaml.load("on: [create]\nwhen: {invoice_status: no, active: true}", Loader=YamlLoader)
    assert doc == {"on": ["create"], "when": {"invoice_status": "no", "active": True}}


@pytest.mark.parametrize("spec, before, after, expected", [
    ("sale", {"state": "draft"}, {"state": "sale"}, True),
    ("sale", {"state": "sale"}, {"state": "sale"}, True),  # plain value: no change needed
    ({"to": "sale"}, {"state": "sale"}, {"state": "sale"}, False),  # transition needs a change
    ({"to": "sale"}, {"state": "sent"}, {"state": "sale"}, True),
    ({"to": "sale"}, {}, {"state": "sale"}, True),  # created as sale
    ({"from": "draft", "to": "sale"}, {"state": "sent"}, {"state": "sale"}, False),
    ({"from": ["draft", "sent"], "to": "sale"}, {"state": "sent"}, {"state": "sale"}, True),
    ({"changed": True}, {"state": "draft"}, {"state": "sent"}, True),
    ({"changed": True}, {"state": "draft"}, {"state": "draft"}, False),
])
def test_conditions(spec, before, after, expected):
    assert Condition.parse("state", spec).matches(before, after) is expected


def test_triggers():
    rule = parse_rule("R", {"model": "sale.order"})
    assert rule.matches({}, {"id": 1}) and rule.matches({"id": 1}, {"id": 1})
    assert not rule.matches({"id": 1}, None)  # delete is not a default trigger
    deleted = parse_rule("R", {"model": "sale.order", "on": "delete", "when": {"state": {"from": "cancel"}}})
    assert deleted.matches({"id": 1, "state": "cancel"}, None)
    assert not deleted.matches({"id": 1, "state": "cancel"}, {"id": 1, "state": "draft"})


def test_includes_accept_list_and_mapping_forms():
    rule = parse_rule("R", {"model": "sale.order", "include": ["partner_id", "order_line.product_id"]})
    assert rule.includes["partner_id"].fields is None
    assert list(rule.includes["order_line"].children) == ["product_id"]
    rule = parse_rule("R", {"model": "sale.order", "include": {
        "order_line": {"fields": ["name"], "where": {"display_type": None}},
        "order_line.product_id": ["name"]}})
    line = rule.includes["order_line"]
    assert line.fields == ("name",) and line.where[0].equals == frozenset([None])
    assert line.children["product_id"].fields == ("name",)


@pytest.mark.parametrize("spec, message", [
    ({"model": "no.such"}, "unknown model no.such .* Is its Odoo app installed"),
    ({"model": "sale.order", "when": {"tax_country_id": 1}}, "tax_country_id is not stored"),
    ({"model": "sale.order", "fields": ["nope"]}, "has no field nope"),
    ({"model": "sale.order", "include": ["tag_preview_ids"]}, "many2many is not stored"),
    ({"model": "sale.order", "include": ["name"]}, "not a relational field"),
    ({"model": "sale.order", "include": {"order_line": {"where": {"nope": 1}}}}, "where field nope"),
])
def test_validation_errors(spec, message):
    with pytest.raises(RuleError, match=message):
        validate([parse_rule("R", spec)], fake_odoo.registry())


def test_unknown_keys_are_rejected():
    with pytest.raises(RuleError, match="unknown keys"):
        parse_rule("R", {"model": "sale.order", "includes": []})


def test_tracked_tables_follow_includes_and_related_fields():
    rule = parse_rule("R", {"model": "sale.order", "include": {
        "partner_id": ["name"], "order_line": ["name"], "order_line.product_id": ["name"]}})
    assert tracked_tables([rule], fake_odoo.registry()) == {
        "sale_order": set(),  # the rule's own table, for each change's previous row
        "res_partner": set(), "sale_order_line": {"order_id"},
        "product_product": set(), "product_template": set(),  # product name is product_tmpl_id.name
    }


def test_conditions_on_related_paths():
    registry = fake_odoo.registry()
    rule = parse_rule("R", {"model": "sale.order.line", "when": {"product_id.name": "Office Chair"}})
    validate([rule], registry)
    # product_id.name follows product_id, then the related product_tmpl_id.name.
    assert tracked_tables([rule], registry) == {
        "sale_order_line": set(), "product_product": set(), "product_template": set()}
    with pytest.raises(RuleError, match="supports only a plain value"):
        validate([parse_rule("R", {"model": "sale.order.line", "when": {"product_id.name": {"to": "x"}}})], registry)
    with pytest.raises(RuleError, match="not a stored many2one"):
        validate([parse_rule("R", {"model": "sale.order.line", "when": {"name.x": "y"}})], registry)


def test_many2many_include_tracks_the_join_table():
    rule = parse_rule("R", {"model": "sale.order", "include": {"tag_ids": ["name"]}})
    validate([rule], fake_odoo.registry())
    assert tracked_tables([rule], fake_odoo.registry()) == {
        "sale_order": set(), "sale_order_tag_rel": {"order_id"}, "crm_tag": set()}


def test_rule_table_without_id_is_not_tracked():
    from erpdoo.models import ModelRegistry

    registry = ModelRegistry.from_rows(
        ["x.log"], [("x.log", "name", "char", None, None, None, None, None, True, None, False)], {"x_log": {"name"}})
    assert tracked_tables([parse_rule("R", {"model": "x.log"})], registry) == {}


def test_split_by_model_sets_aside_rules_for_missing_apps():
    rules = [parse_rule("A", {"model": "sale.order"}), parse_rule("B", {"model": "crm.lead"})]
    active, skipped = split_by_model(rules, fake_odoo.registry())
    assert [r.name for r in active] == ["A"] and [r.name for r in skipped] == ["B"]


def test_missing_model_message_suggests_close_names():
    registry = fake_odoo.registry()
    assert missing_model_message(parse_rule("R", {"model": "sale.ordr"}), registry) == \
        "model sale.ordr not found (did you mean sale.order or sale.order.line?)"
    assert missing_model_message(parse_rule("R", {"model": "crm.lead"}), registry) == \
        "model crm.lead not found (is its Odoo app installed?)"
