from odoo import fields, models


class Gadget(models.Model):
    _name = "erpdoo.test.gadget"
    _description = "erpdoo test gadget"
    # Not the table Odoo's naming rule gives (erpdoo_test_gadget).
    _table = "erpdoo_custom_gadgets"

    name = fields.Char(required=True)
    state = fields.Selection([("draft", "Draft"), ("active", "Active")], default="draft", required=True)
    partner_id = fields.Many2one("res.partner")
