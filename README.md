# erpdoo

Turn Odoo's raw database changes into clean business events (`SaleOrderConfirmed`,
`InvoicePaid`, ...) and stream them to Kafka and other sinks, with nothing
installed inside Odoo.

erpdoo reads PostgreSQL logical replication (pgoutput) directly. No Debezium,
no Kafka Connect: Postgres, one erpdoo process and a sink.

```
Odoo ──▶ PostgreSQL ──(logical replication)──▶ erpdoo ──▶ Kafka / Redpanda / webhook / stdout
```

> **Status: early development.** Events are defined by YAML rules, and six
> ship with erpdoo (below). Related records (partner, order lines, product
> names) come from a local SQLite copy kept current from the stream. Events
> go to Kafka, a webhook or stdout.

## Try it

Requires Docker with Compose v2. The first start downloads about 2 GB of
images, then installs Odoo's Sales, Inventory and CRM apps with demo data
(about a minute).

```sh
docker compose up -d --build
docker compose logs -f erpdoo
```

Open Odoo at http://localhost:8069 (login `admin`, password `admin`), go to
**Sales**, open a quotation and click **Confirm**. The event appears in the
erpdoo log and in Redpanda Console at http://localhost:8080 (topic `erpdoo.events`).
Posting a customer invoice gives an `InvoicePosted` event the same way.
This is a real event from the dev stack:

```json
{
  "event_id": "cdf720b0-763d-5c84-b402-ce66b89b9e47",
  "event_type": "SaleOrderConfirmed",
  "model": "sale.order",
  "record_id": 35,
  "occurred_at": "2026-10-19T23:47:45.626831+00:00",
  "changes": {
    "state": {
      "from": "draft",
      "to": "sale"
    }
  },
  "record": {
    "id": 35,
    "name": "S00035",
    "state": "sale",
    "date_order": "2026-10-19 23:47:45",
    "client_order_ref": null,
    "currency_id": 1,
    "amount_untaxed": "1500.00",
    "amount_tax": "0.00",
    "amount_total": "1500.00",
    "user_id": 2,
    "team_id": 1,
    "company_id": 1,
    "partner_id": {
      "id": 10,
      "name": "Acme Corporation",
      "email": "info@agrolait.com",
      "phone": "(603)-996-3829",
      "vat": "US12345673"
    },
    "order_line": [
      {
        "id": 59,
        "name": "[DESK0005] Customizable Desk (Custom, White)\n160x80cm, with large legs.",
        "product_uom_qty": "2.00",
        "price_unit": "750.0",
        "discount": "0.00",
        "price_subtotal": "1500.00",
        "price_total": "1500.00",
        "product_id": {
          "id": 44,
          "name": "Customizable Desk",
          "default_code": "DESK0005"
        }
      }
    ]
  },
  "source": {
    "table": "sale_order",
    "xid": 1813,
    "commit_lsn": "0/61C27B0"
  }
}
```

Events are keyed `sale.order:<id>`, so all events for one record stay in order
on one partition. `event_id` is derived from the Postgres transaction, the rule
and the record, so redelivered events carry the same id and consumers can dedupe.
Numeric values are strings (`"1500.00"`) so no precision is lost.

To check the whole path from a script (drives every shipped event through
Odoo's API and checks each against Odoo's data, plus actions that must not
produce an event):

```sh
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python scripts/smoke_test.py
```

## Events

The packs in [rules/](rules/) cover:

| Event | Model | Fires when |
|---|---|---|
| `SaleOrderConfirmed` | `sale.order` | an order is confirmed (from draft or sent) |
| `SaleOrderCancelled` | `sale.order` | a quotation or order is cancelled |
| `InvoicePosted` | `account.move` | a customer invoice is posted |
| `InvoicePaid` | `account.move` | a customer invoice becomes fully paid (`in_payment` or `paid`) |
| `DeliveryDone` | `stock.picking` | an outgoing delivery is validated |
| `LeadWon` | `crm.lead` | an opportunity is marked won |

Each pack is plain YAML. A rule for an app you don't have installed stops
erpdoo at startup with a message saying so: delete that file (for example
`rules/crm.yml` without CRM).

## Sinks

Sinks are set in [erpdoo.yml](erpdoo.yml), also mounted into the container.
Events go to every sink listed, and the replication slot is confirmed only
after all of them have delivered. Strings may read environment variables
with `${VAR}` or `${VAR:-default}`.

```yaml
sinks:
  - type: kafka
    bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
    topic: erpdoo.events
  - type: webhook
    url: https://example.com/hooks/erpdoo
    headers: { Authorization: "Bearer ${WEBHOOK_TOKEN}" }
    secret: ${WEBHOOK_SECRET}   # optional HMAC signature
  - type: stdout
```

- **kafka**: one topic, keyed `model:id`, written with an idempotent producer (`acks=all`).
- **webhook**: one `POST` per event, in order, with the event as the JSON
  body and `X-Erpdoo-Event-Id` / `X-Erpdoo-Event-Type` headers. With
  `secret`, `X-Erpdoo-Signature: sha256=<hex>` is the HMAC-SHA256 of the raw
  body. Network errors, timeouts, 5xx, 408 and 429 are retried with
  exponential backoff (`retries`, default 5; `backoff`, default 1 s, doubling
  up to 60 s; `Retry-After` is honoured); any other 4xx counts as a rejection.
- **stdout**: one JSON event per line, for debugging (`docker compose logs erpdoo`).

Without a config file, events go to Kafka as set by `ERPDOO_KAFKA_BOOTSTRAP`
and `ERPDOO_TOPIC`.

## Writing rules

Rules live in [rules/](rules/), one or more YAML files, mounted into the
container: edit them and run `docker compose restart erpdoo`. A shortened
version of the `InvoicePosted` rule ([rules/invoicing.yml](rules/invoicing.yml)):

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
```

- `when` compares each record at the start of a transaction with the same
  record at commit: `field: value` (value at commit), `{to: v}`,
  `{from: v, to: w}` or `{changed: true}`. Values can be lists. A plain
  value may also test a related record through a dotted path, as
  `DeliveryDone` does with `picking_type_id.code: outgoing`.
- `on` picks the triggers: `create`, `update`, `delete` (default: create and update).
- `fields` lists the record's fields to keep (default: every stored column).
- `include` expands many2one and one2many fields, nested with dots. Related
  fields such as a product's `name` (stored on the product template) are
  followed automatically. `where` filters the related rows.

erpdoo checks rules against Odoo's models at startup and lists every
problem, such as an unknown field. The full reference is at the top of
[src/erpdoo/rules.py](src/erpdoo/rules.py).

## Trade-offs, honestly

- **Needs replication access to Odoo's Postgres** (`wal_level=logical` and a
  role with `REPLICATION`). Hosted Odoo platforms (Odoo Online, Odoo.sh)
  usually don't give you this. A polling mode over Odoo's API is planned for
  that case, with its own limits.
- **`REPLICA IDENTITY FULL` on each rule's table** (`sale_order`,
  `account_move`, `stock_picking` and `crm_lead` with the shipped rules). erpdoo sets this so Postgres
  includes the previous row in each update, which is how a rule sees the
  value a field is leaving. It makes each update to those tables write more WAL.
- **erpdoo adds tables to Odoo's publication** for its rules and their
  related records, but first checks that each has a primary key or another
  replica identity. Without one, Postgres would reject Odoo's updates and
  deletes on that table, so erpdoo refuses to start and names the table.
- **The state store copies whole tables.** Each table a rule includes
  (partners, products, order and invoice lines, ...) is copied into SQLite
  when tracking starts, which takes a while on a large database, then kept
  current from the stream. If events are sent again after a crash, their
  related records reflect the current state, not the original moment.
  many2many includes are not supported yet.
- **Delivery is at-least-once.** erpdoo streams changes over Postgres'
  replication protocol and confirms its position in the slot right after
  every sink has delivered the events. If it crashes in between, or one sink
  fails after another succeeded, the events are sent again after a restart,
  with the same `event_id`. Consumers should dedupe on it.
- **A sink that keeps failing stops the stream.** erpdoo never skips an
  event: when a webhook rejects one (a 4xx) or stays unreachable past its
  retries, erpdoo exits without confirming it and tries again after a
  restart. Nothing is lost, but nothing newer gets through until the
  receiver accepts it.
- **Table names are inferred.** Odoo does not store a model's table name in
  the database, so erpdoo uses Odoo's naming rule (`sale.order` →
  `sale_order`) plus the core models known to differ (`ir.actions.*`). A
  model from another module that sets its own `_table` is not recognised
  yet. `python -m erpdoo.models <model>` shows how a model is mapped.
- **An unused replication slot keeps WAL.** If erpdoo is stopped for a long
  time, Postgres keeps the WAL it hasn't read yet and disk use grows. Drop the
  slot (`SELECT pg_drop_replication_slot('erpdoo')`) if you remove erpdoo.
- **The dev stack's database role is broad.** Its `erpdoo` role inherits the
  privileges of the `odoo` role so it can create its publication. A tighter
  production setup is still to be documented.

## License

MIT
