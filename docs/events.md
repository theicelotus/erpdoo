# Events

The events erpdoo ships rules for, and what an event looks like. To write
your own, see [Writing rules](rules.md); for each event type's schema, see
[Schemas](schemas.md).

- [Shipped events](#shipped-events)
- [An event](#an-event)
- [Keys, ids and values](#keys-ids-and-values)
- [The source field](#the-source-field)

## Shipped events

The packs in [rules/](../rules/) cover:

| Event | Model | Fires when |
|---|---|---|
| `SaleOrderConfirmed` | `sale.order` | an order is confirmed (from draft or sent) |
| `SaleOrderCancelled` | `sale.order` | a quotation or order is cancelled |
| `InvoicePosted` | `account.move` | a customer invoice is posted |
| `InvoicePaid` | `account.move` | a customer invoice becomes fully paid (`in_payment` or `paid`) |
| `DeliveryDone` | `stock.picking` | an outgoing delivery is validated |
| `LeadWon` | `crm.lead` | an opportunity is marked won |

Each pack is plain YAML. A rule for an app that isn't installed is skipped
with a warning, and starts working once the app is installed, without a
restart: right away if erpdoo's role may publish the app's tables, otherwise
once an admin has run the SQL erpdoo logs (see
[Setting up Postgres](postgres-setup.md#your-own-rules-and-apps-installed-later)).
Changes to the app's records from its install until then are backfilled,
each exactly once (see [Apps installed later](backfill.md#apps-installed-later)).

## An event

This is a real event from the dev stack, after confirming a quotation in
Odoo:

```json
{
  "event_id": "cd443309-b495-5e44-af7e-736cd95bad1b",
  "event_type": "SaleOrderConfirmed",
  "model": "sale.order",
  "record_id": 3740,
  "occurred_at": "2026-10-21T00:04:04.388605+00:00",
  "changes": {
    "state": {
      "from": "draft",
      "to": "sale"
    }
  },
  "record": {
    "id": 3740,
    "name": "S03740",
    "state": "sale",
    "date_order": "2026-10-21 00:04:04",
    "client_order_ref": null,
    "currency_id": 1,
    "amount_untaxed": "1500.00",
    "amount_tax": "225.00",
    "amount_total": "1725.00",
    "user_id": 2,
    "team_id": 1,
    "company_id": 1,
    "partner_id": {
      "id": 10,
      "name": "Acme Corporation",
      "email": "info@agrolait.com",
      "phone": "(603)-996-3829",
      "vat": "US12345673",
      "is_company": true
    },
    "order_line": [
      {
        "id": 3766,
        "name": "[DESK0005] Customizable Desk (Custom, White)\n160x80cm, with large legs.",
        "product_uom_qty": "2.00",
        "price_unit": "750.0",
        "discount": "0.00",
        "price_subtotal": "1500.00",
        "price_total": "1725.00",
        "product_id": {
          "id": 45,
          "name": "Customizable Desk",
          "default_code": "DESK0005"
        },
        "tax_id": [
          {
            "id": 1,
            "name": "15%",
            "amount": "15.0000",
            "amount_type": "percent"
          }
        ]
      }
    ]
  },
  "source": {
    "table": "sale_order",
    "xid": 10145,
    "commit_lsn": "0/E5EBAC8"
  }
}
```

## Keys, ids and values

- **Keys.** Events are keyed `sale.order:<id>` (`model:id`), so all events
  for one record stay in order on one partition.
- **Ids.** `event_id` is derived from the Postgres transaction, the rule
  and the record, so redelivered events carry the same id and consumers can
  dedupe.
- **Values.** Numeric values are strings (`"1500.00"`) so no precision is
  lost. How each Odoo field type appears is in
  [How Odoo fields appear](schemas.md#how-odoo-fields-appear).
- **Schemas.** Each event type has a schema generated from its rule (see
  [Schemas](schemas.md)).

## The source field

`source` says where an event came from. Streamed events have the table,
the transaction id (`xid`) and its commit position (`commit_lsn`), as above.
Other events add to it:

- backfilled events: `"backfill": true`, `"approximate"` and the tracking
  message (see [Backfill](backfill.md));
- polled events: `"polling": true` (see [Polling mode](polling.md#the-two-modes));
- events caused by an inbound message: `"inbound"` with the message (see
  [Loop safety](inbound.md#loop-safety)).
