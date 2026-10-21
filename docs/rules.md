# Writing rules

Events are defined by YAML rules: which model, which change, and which
related records to include. No code is needed per event. This page covers
the rule format; the events erpdoo ships are listed in [Events](events.md),
and rules for messages that act on Odoo are in [Inbound](inbound.md).

- [Where rules live](#where-rules-live)
- [An example](#an-example)
- [The rule format](#the-rule-format)
- [Checks at startup](#checks-at-startup)

## Where rules live

Rules live in [rules/](../rules/), one or more YAML files (`ERPDOO_RULES`,
a file or directory, default `rules`), mounted into the dev stack's
container: edit them and run `docker compose restart erpdoo`, no rebuild
needed.

## An example

A shortened version of the `InvoicePosted` rule
([rules/invoicing.yml](../rules/invoicing.yml)):

```yaml
InvoicePosted:
  model: account.move
  when:
    state: { to: posted }      # changed to posted in this transaction
    move_type: out_invoice     # and is a customer invoice
  include:
    partner_id: [name, email, vat]
    invoice_line_ids:
      fields: [name, quantity, price_unit, price_subtotal]
      where: { display_type: product }
    invoice_line_ids.product_id: [name, default_code]
    invoice_line_ids.tax_ids: [name, amount]   # many2many
```

The rule's name is the event type.

## The rule format

- `when` compares each record at the start of a transaction with the same
  record at commit: `field: value` (value at commit), `{to: v}`,
  `{from: v, to: w}` or `{changed: true}`. Values can be lists. A plain
  value may also test a related record through a dotted path, as
  `DeliveryDone` does with `picking_type_id.code: outgoing`.
- `on` picks the triggers: `create`, `update`, `delete` (default: create and update).
- `fields` lists the record's fields to keep (default: every stored column).
- `include` expands many2one, one2many and many2many fields, nested with
  dots. Related fields such as a product's `name` (stored on the product
  template) are followed automatically. `where` filters the related rows.
  one2many and many2many rows come in order of their id, not in the order
  Odoo shows them.

The full reference is at the top of [src/erpdoo/rules.py](../src/erpdoo/rules.py).

Changing a rule changes its event type's schema. Which changes are
compatible for consumers is in [Compatibility](schemas.md#compatibility).

## Checks at startup

erpdoo checks rules against Odoo's models at startup and lists every
problem, such as an unknown field.

A rule for an app that isn't installed is skipped with a warning, and
starts working once the app is installed, without a restart (see
[Shipped events](events.md#shipped-events)). The tables a new rule needs
must be in erpdoo's publication: erpdoo works out which and prints the SQL
(see [Your own rules](postgres-setup.md#your-own-rules-and-apps-installed-later)).
