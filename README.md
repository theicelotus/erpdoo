# erpdoo

Turn Odoo's raw database changes into clean business events (`SaleOrderConfirmed`,
`InvoicePaid`, ...) and stream them to Kafka and other sinks, with nothing
installed inside Odoo.

erpdoo reads PostgreSQL logical replication (pgoutput) directly. No Debezium,
no Kafka Connect: Postgres, one erpdoo process and a sink.

```
Odoo ──▶ PostgreSQL ──(logical replication)──▶ erpdoo ──▶ Kafka / Redpanda
```

> **Status: early development.** Today erpdoo emits one hard-coded event,
> `SaleOrderConfirmed`. YAML rules, a state store for related records, more
> sinks and built-in event packs are next.

## Try it

Requires Docker with Compose v2. The first start downloads about 2 GB of
images, then installs Odoo's Sales app with demo data (about a minute).

```sh
docker compose up -d --build
docker compose logs -f erpdoo
```

Open Odoo at http://localhost:8069 (login `admin`, password `admin`), go to
**Sales**, open a quotation and click **Confirm**. The event appears in the
erpdoo log and in Redpanda Console at http://localhost:8080 (topic `erpdoo.events`).
This is a real event from the dev stack:

```json
{
  "event_id": "54b11ee0-d688-5e06-ae49-205e7a0cb5cd",
  "event_type": "SaleOrderConfirmed",
  "model": "sale.order",
  "record_id": 21,
  "occurred_at": "2026-10-19T22:07:27.481175+00:00",
  "changes": {
    "state": {
      "from": "draft",
      "to": "sale"
    }
  },
  "record": {
    "id": 21,
    "name": "S00021",
    "state": "sale",
    "partner_id": 10,
    "partner_invoice_id": 10,
    "partner_shipping_id": 10,
    "user_id": 2,
    "team_id": 1,
    "company_id": 1,
    "currency_id": 1,
    "pricelist_id": null,
    "date_order": "2026-10-19 22:07:27",
    "client_order_ref": null,
    "amount_untaxed": "1500.00",
    "amount_tax": "0.00",
    "amount_total": "1500.00"
  },
  "source": {
    "table": "sale_order",
    "xid": 1375,
    "commit_lsn": "0/46A8C40"
  }
}
```

Events are keyed `sale.order:<id>`, so all events for one record stay in order
on one partition. `event_id` is derived from the Postgres transaction, the rule
and the record, so redelivered events carry the same id and consumers can dedupe.

To check the whole path from a script (creates and confirms an order through
Odoo's API, then waits for the event):

```sh
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python scripts/smoke_sale_order.py
```

## Trade-offs, honestly

- **Needs replication access to Odoo's Postgres** (`wal_level=logical` and a
  role with `REPLICATION`). Hosted Odoo platforms (Odoo Online, Odoo.sh)
  usually don't give you this. A polling mode over Odoo's API is planned for
  that case, with its own limits.
- **`REPLICA IDENTITY FULL` on `sale_order`.** erpdoo sets this so Postgres
  includes the previous row in each update, which is how it sees which state
  an order is leaving. It makes each update to that table write more WAL.
- **Delivery is at-least-once.** erpdoo streams changes over Postgres'
  replication protocol and confirms its position in the slot only after
  Kafka has acknowledged the events. Confirmations reach Postgres about every
  10 seconds, so after a crash up to roughly the last 10 seconds of events
  are sent again, with the same `event_id`.
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
