"""A small Odoo schema and store for rule and event tests."""

from erpdoo.models import ModelRegistry
from erpdoo.store import StateStore

MODELS = ["sale.order", "sale.order.line", "res.partner", "product.product", "product.template", "crm.tag"]


def f(model, name, ttype="char", relation=None, inverse=None, rel_table=None, c1=None, c2=None,
      store=True, related=None, translate=False):
    return (model, name, ttype, relation, inverse, rel_table, c1, c2, store, related, translate)


FIELDS = [
    f("sale.order", "id", "integer"),
    f("sale.order", "name"),
    f("sale.order", "state", "selection"),
    f("sale.order", "invoice_status", "selection"),
    f("sale.order", "amount_total", "monetary"),
    f("sale.order", "partner_id", "many2one", "res.partner"),
    f("sale.order", "order_line", "one2many", "sale.order.line", inverse="order_id"),
    f("sale.order", "tag_ids", "many2many", "crm.tag", rel_table="sale_order_tag_rel", c1="order_id", c2="tag_id"),
    f("sale.order", "tax_country_id", "many2one", "res.country", store=False),
    f("sale.order", "tag_preview_ids", "many2many", "crm.tag", store=False),
    f("sale.order.line", "id", "integer"),
    f("sale.order.line", "order_id", "many2one", "sale.order"),
    f("sale.order.line", "name", "text"),
    f("sale.order.line", "display_type", "selection"),
    f("sale.order.line", "product_id", "many2one", "product.product"),
    f("sale.order.line", "price_subtotal", "monetary"),
    f("res.partner", "id", "integer"),
    f("res.partner", "name"),
    f("res.partner", "email"),
    f("product.product", "id", "integer"),
    f("product.product", "default_code"),
    f("product.product", "product_tmpl_id", "many2one", "product.template"),
    f("product.product", "name", store=False, related="product_tmpl_id.name"),
    f("product.template", "id", "integer"),
    f("product.template", "name", translate=True),
    f("crm.tag", "id", "integer"),
    f("crm.tag", "name"),
]

TABLES = {
    "sale_order": {"id", "name", "state", "invoice_status", "amount_total", "partner_id"},
    "sale_order_line": {"id", "order_id", "name", "display_type", "product_id", "price_subtotal"},
    "res_partner": {"id", "name", "email"},
    "product_product": {"id", "default_code", "product_tmpl_id"},
    "product_template": {"id", "name"},
    "crm_tag": {"id", "name"},
    "sale_order_tag_rel": {"order_id", "tag_id"},
}


def registry() -> ModelRegistry:
    return ModelRegistry.from_rows(MODELS, FIELDS, TABLES)


def store() -> StateStore:
    s = StateStore(":memory:")
    s.load("res_partner", [{"id": 10, "name": "Azure Interior", "email": "azure@example.com"}])
    s.load("product_template", [{"id": 70, "name": {"en_US": "Office Chair", "fr_FR": "Chaise de bureau"}}])
    s.load("product_product", [{"id": 7, "default_code": "CHAIR", "product_tmpl_id": 70}])
    s.load("sale_order_line", [
        {"id": 100, "order_id": 5, "name": "Office Chair", "display_type": None, "product_id": 7, "price_subtotal": "70.00"},
        {"id": 101, "order_id": 5, "name": "Furniture", "display_type": "line_section", "product_id": None, "price_subtotal": "0.00"},
    ])
    s.index("order_id")
    return s
