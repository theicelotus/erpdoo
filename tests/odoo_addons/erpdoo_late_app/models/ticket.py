from odoo import fields, models


class Ticket(models.Model):
    _name = "erpdoo.test.ticket"
    _description = "erpdoo test ticket"
    _inherit = ["mail.thread"]

    name = fields.Char(required=True)
    # Tracked: its changes are in the chatter, so they are backfilled exactly.
    state = fields.Selection([("new", "New"), ("open", "Open"), ("done", "Done")], default="new", required=True,
                             tracking=True)
    # Not tracked: backfilled from the record's current value (approximate).
    priority = fields.Selection([("normal", "Normal"), ("high", "High")], default="normal", required=True)
    partner_id = fields.Many2one("res.partner")
