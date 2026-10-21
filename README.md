# erpdoo

[![CI](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml/badge.svg)](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml)

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
> go to Kafka, a webhook or stdout, and a live page shows them next to the
> database changes they came from.

## Try it

Requires Docker with Compose v2. The first start downloads about 2 GB of
images, then installs Odoo's Sales, Inventory and CRM apps with demo data
(about a minute). A one-shot `db-setup` container then prepares Postgres
the way a database admin would (see [Setting up Postgres](#setting-up-postgres)),
so erpdoo runs with a role that can stream and read but not write.

```sh
docker compose up -d --build
docker compose logs -f erpdoo
```

Open Odoo at http://localhost:8069 (login `admin`, password `admin`), go to
**Sales**, open a quotation and click **Confirm**. The event appears in the
erpdoo log and in Redpanda Console at http://localhost:8080 (topic `erpdoo.events`).
Posting a customer invoice gives an `InvoicePosted` event the same way.
The live viewer at http://localhost:8000 shows each transaction's raw changes
next to the events built from it. This is a real event from the dev stack:

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
      "vat": "US12345673"
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

Events are keyed `sale.order:<id>`, so all events for one record stay in order
on one partition. `event_id` is derived from the Postgres transaction, the rule
and the record, so redelivered events carry the same id and consumers can dedupe.
Numeric values are strings (`"1500.00"`) so no precision is lost.

To check the whole path from a script (drives every shipped event through
Odoo's API and checks each against Odoo's data, plus actions that must not
produce an event, and checks that erpdoo's role cannot write to Odoo's
tables; CI runs it on every push):

```sh
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python scripts/smoke_test.py
```

`scripts/replay_test.py` checks the crash case: erpdoo delivers an event,
the customer is renamed, and erpdoo crashes before confirming the slot. The
event sent again after the restart must be identical to the first,
customer name included. It uses a test hook (`ERPDOO_TEST_HOLD_ACK` in
[docker-compose.yml](docker-compose.yml)) and also runs in CI.

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

Each pack is plain YAML. A rule for an app that isn't installed is skipped
with a warning, and starts working once the app is installed, without a
restart: right away if erpdoo's role may publish the app's tables, otherwise
once an admin has run the SQL erpdoo logs (see
[Setting up Postgres](#setting-up-postgres)).

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

By default an event a sink cannot deliver stops erpdoo, so nothing is lost
(see the trade-offs). To keep going instead, give the sink a dead-letter topic:

```yaml
  - type: webhook
    url: https://example.com/hooks/erpdoo
    dead_letter:
      topic: erpdoo.dead-letter
      bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
```

An event the sink rejects, or that still fails after the retries, is then
written to that topic as `{"event": ..., "error": ..., "sink": ...,
"failed_at": ...}` and the stream continues. A sink failure that is not
about one event, such as an unreachable Kafka broker, still stops erpdoo,
and so does a failure to write the dead-letter topic.

Without a config file, events go to Kafka as set by `ERPDOO_KAFKA_BOOTSTRAP`
and `ERPDOO_TOPIC`.

## Live viewer

With `viewer:` in [erpdoo.yml](erpdoo.yml) (on in the dev stack), erpdoo
serves a page at http://localhost:8000 that shows each transaction as it is
delivered: its inserts, updates (with the fields that changed, old and new)
and deletes on one side, and the events built from it on the other. It
needs the `viewer` extra (`pip install 'erpdoo[viewer]'`, included in the
Docker image). It has no login and shows business data, so keep it on
localhost or a private network.

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
    invoice_line_ids.tax_ids: [name, amount]   # many2many
```

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

erpdoo checks rules against Odoo's models at startup and lists every
problem, such as an unknown field. The full reference is at the top of
[src/erpdoo/rules.py](src/erpdoo/rules.py).

## Setting up Postgres

erpdoo only needs a role with `REPLICATION`, `CONNECT` on Odoo's database
and `SELECT` on the tables it streams. It never writes to Odoo's tables.
An admin (a superuser, or the role that owns Odoo's tables) prepares the
rest once. Postgres must run with `wal_level=logical` (a restart), with a
replication slot and a WAL sender to spare (`max_replication_slots`,
`max_wal_senders`). If erpdoo connects from another host, `pg_hba.conf`
needs an ordinary entry for Odoo's database; logical replication does not
use the `replication` entries.

For the shipped rules, run this in Odoo's database (named `odoo` here):

```sql
CREATE ROLE erpdoo WITH LOGIN REPLICATION PASSWORD 'change-me';
GRANT CONNECT ON DATABASE odoo TO erpdoo;

-- The rules' tables, the related records they include (with the join
-- tables of many2many fields such as taxes), and ir_model /
-- ir_model_fields, which tell erpdoo when an Odoo app is installed.
CREATE PUBLICATION erpdoo FOR TABLE
  account_move, account_move_line, account_move_line_account_tax_rel, account_tax,
  account_tax_sale_order_line_rel, crm_lead, crm_stage, ir_model, ir_model_fields,
  product_product, product_template, res_partner, sale_order, sale_order_line,
  stock_move, stock_picking, stock_picking_type;

GRANT SELECT ON
  account_move, account_move_line, account_move_line_account_tax_rel, account_tax,
  account_tax_sale_order_line_rel, crm_lead, crm_stage, ir_model, ir_model_fields,
  product_product, product_template, res_partner, sale_order, sale_order_line,
  stock_move, stock_picking, stock_picking_type
  TO erpdoo;
```

Tables keep their default replica identity (the primary key): erpdoo takes
the previous row of each update and delete from its state store, so
`REPLICA IDENTITY FULL` is not needed. erpdoo creates its replication slot
itself, which `REPLICATION` allows. The
publication and slot names are set with `ERPDOO_PUBLICATION` and
`ERPDOO_SLOT` (both `erpdoo` by default). The dev stack runs the same steps
from [docker/postgres/setup-erpdoo.sql](docker/postgres/setup-erpdoo.sql).

With your own rules the list of tables differs, and you don't have to work
it out: erpdoo checks the publication at startup and, if anything is
missing, stops with a list and the SQL that fixes it:

```
ERROR erpdoo: erpdoo needs these changes in Postgres, which its role erpdoo is not allowed to make:
  not in publication erpdoo: purchase_order, purchase_order_line
  not readable by erpdoo: purchase_order, purchase_order_line
Run this SQL as an admin (a superuser, or the owner of Odoo's tables and of the publication):

  ALTER PUBLICATION erpdoo ADD TABLE public.purchase_order, public.purchase_order_line;
  GRANT SELECT ON public.purchase_order, public.purchase_order_line TO erpdoo;
```

When an Odoo app installed later activates a rule (Purchase, for the rule
above), erpdoo logs the same message instead of stopping, keeps the other
rules running, and checks again every 30 seconds; the new rule starts once
an admin has run the SQL. erpdoo only adds to a publication it did not
create, so tables an admin published for other uses stay.

If erpdoo's role is allowed to make these changes, for example because it
owns Odoo's tables, erpdoo makes them itself and no admin step is needed.

One thing the grants above do not cover: when Odoo creates a database it
runs `GRANT CREATE ON SCHEMA public TO PUBLIC`, so every role, erpdoo's
included, can create new tables of its own in `public` (not change Odoo's).
erpdoo never does. On Postgres 15 and later the database's owner also owns
`public`, so if Odoo's role owns the database you can take this away from
every other role with `REVOKE CREATE ON SCHEMA public FROM PUBLIC`.

## Performance

Measured with [scripts/benchmark.py](scripts/benchmark.py) on the dev stack,
everything on one laptop: Intel Core i9-11900H (8 cores, 16 threads),
15.4 GiB RAM, Samsung NVMe SSD, Fedora 42 (kernel 6.19), Docker 29.5.3.
Odoo, Postgres, Redpanda, Redpanda Console and erpdoo all share that machine.
erpdoo runs the six shipped rules with the dev config (Kafka and stdout
sinks, viewer on), and each event includes the partner and order lines from
the state store. Latency is from the Postgres commit to the moment a Kafka
consumer receives the event. Results from two consecutive runs:

| | Run 1 | Run 2 |
|---|---|---|
| Latency, one order at a time (200 orders), p50 / p95 / p99 | 13.2 / 16.4 / 28.5 ms | 13.2 / 15.7 / 22.3 ms |
| Burst while erpdoo runs (3,000 orders, 25 per transaction, 8 concurrent clients) | 166 events/s | 169 events/s |
| Latency during that burst, p50 / p95 / p99 | 24 / 52 / 65 ms | 32 / 71 / 87 ms |
| Last event after the last commit | 0.04 s | 0.05 s |
| Backlog of 3,000 confirmed orders, erpdoo started afterwards | 3,188 events/s (0.94 s) | 3,219 events/s (0.93 s) |
| erpdoo startup (container start to streaming) | 0.72 s | 0.69 s |

How to read it:

- In the burst, Odoo is the limit: it committed about 170 confirmations a
  second, and erpdoo delivered each within about 90 ms, finishing 50 ms
  after the last commit.
- The backlog run takes Odoo out of the picture: erpdoo works through 3,000
  waiting confirmations at about 3,200 events a second, including the
  state store lookups for each event's partner and order lines. The orders
  were created while erpdoo ran, so their lines were already in the store;
  orders from before erpdoo started cost one query each for their lines.
- No events were missing or duplicated in any phase.
- This is one machine with no network between the services, and small
  demo tables. Expect higher latency across real networks, and a longer
  first start on a large database while the state store copies the rule
  tables.
  The orders use a service product, so no deliveries are created.

To reproduce, start the stack and run `.venv/bin/python scripts/benchmark.py`.
It creates about 6,200 orders in the dev database and stops and starts the
erpdoo container for the backlog phase.

### State store size and startup

The state store keeps the rule tables with only the columns the rules use,
and fetches related records when an event first needs them. Before, it
copied every column of the rule tables and of every related table at
startup. Measured on the dev database after the runs above (about 16,000
sale orders and order lines, 3,700 deliveries and stock moves), each
with a new store and its own slot, two runs each:

| | Copy whole tables | Columns rules use, related rows on demand |
|---|---|---|
| Startup, process start to streaming | 2.15 s / 1.90 s | 0.94 s / 0.92 s |
| Store size after startup (SQLite file after a clean stop) | 59.2 MB | 8.5 MB |
| The same after the smoke test ran meanwhile | | 8.5 MB |

Startup is dominated by copying `sale_order` (16,000 rows), now with 13 of its
51 columns; `sale_order_line` and the other related
tables are no longer copied.

### WAL per order confirmation

Earlier versions of erpdoo needed `REPLICA IDENTITY FULL` on the rule tables,
so that Postgres wrote each updated row's previous version into the WAL.
erpdoo now takes that row from its state store instead. Measured with
[scripts/wal_cost.py](scripts/wal_cost.py) on the same machine: 300 sale
orders, each with one stocked product (so confirming also creates a
delivery), confirmed one at a time through Odoo's API, three runs each.

| | `REPLICA IDENTITY FULL` | Default (primary key) |
|---|---|---|
| WAL on the rule tables (`sale_order`, `stock_picking`, ...) | 1,356 / 1,357 / 1,403 bytes | 963 / 966 / 986 bytes |
| All WAL, without full-page images | 15,956 / 15,966 / 17,455 bytes | 15,657 / 15,734 / 17,212 bytes |
| All WAL, with full-page images | 16,166 / 16,151 / 18,268 bytes | 22,477 / 16,041 / 19,603 bytes |

Dropping `REPLICA IDENTITY FULL` saves about 390 bytes per confirmation,
29% of the WAL written to the rule tables. That is only about 2% of all the
WAL a confirmation writes: most of it goes to Odoo's chatter
(`mail_message`, `mail_followers`) and to indexes. The totals vary from run
to run by more than the saving, mostly through full-page images, which
depend on when checkpoints happen; the rule-table figure is the steady one.

## Trade-offs, honestly

- **Needs replication access to Odoo's Postgres** (`wal_level=logical` and a
  role with `REPLICATION`), and an admin to run a few statements once (see
  [Setting up Postgres](#setting-up-postgres)). Hosted Odoo platforms
  (Odoo Online, Odoo.sh) usually don't give you this. A polling mode over
  Odoo's API is planned for that case, with its own limits.
- **The rules' own tables are copied too.** A rule sees the value a field
  is leaving by comparing with the row before the change, and Postgres only
  sends that row with `REPLICA IDENTITY FULL`. erpdoo instead keeps the
  rule tables (`sale_order`, `account_move`, `stock_picking` and `crm_lead`
  with the shipped rules) in its state store, copied when tracking starts
  but only with the columns the rules use, with the previous version of
  each changed row until the slot is confirmed past it. That costs disk
  space and a longer first copy on a large database (`account_move` holds
  every invoice and journal entry), to save little WAL (see
  [Performance](#wal-per-order-confirmation)). Changes committed before a
  table was copied are not checked against rules. A rule table without an
  `id` column would still need `REPLICA IDENTITY FULL`; erpdoo says so in
  its log and asks for it in its SQL, but Odoo's models always have one.
- **A new app's rules wait for an admin.** With a read-only role, a rule for
  an app installed later starts only after an admin runs the SQL erpdoo
  logs (erpdoo checks every 30 seconds). Changes to the app's records before
  that produce no events.
- **Published tables need a replica identity.** Before publishing a table,
  or asking an admin to, erpdoo checks that it has a primary key or another
  replica identity. Without one, Postgres would reject Odoo's updates and
  deletes on that table, so erpdoo refuses to start and names the table.
- **Related records are fetched on first use.** Tables a rule includes
  (partners, products, order and invoice lines, ...) are not copied up
  front. The first time an event needs a row, erpdoo reads it from
  Postgres, with the columns the rules use, and from then on keeps it
  current from the stream; the lines of an order are read together the
  first time. A row read this way is as Postgres has it at that moment,
  which can be newer than the transaction the event is about when erpdoo
  is behind, for example while it works through a backlog. Records created
  while erpdoo runs need no query: their lines arrive in the stream. A
  many2many include works the same way through its join table (for taxes,
  `account_move_line_account_tax_rel`), which must be published like the
  other tables.
- **Delivery is at-least-once.** erpdoo streams changes over Postgres'
  replication protocol and confirms its position in the slot right after
  every sink has delivered the events. If it crashes in between, or one sink
  fails after another succeeded, the events are sent again after a restart,
  with the same `event_id`. Consumers should dedupe on it. A resent event
  is the same as the first, related records included: the state store keeps
  the previous versions of changed rows until the slot is confirmed, and
  reads related records as of the original commit (a related row it reads
  from Postgres for the first time during the resend is current). If Postgres itself
  crashes, the slot can restart from further back than erpdoo confirmed
  (Postgres saves its position at checkpoints); erpdoo no longer has
  versions for those transactions, recognises them as delivered and does
  not send them again.
- **A sink that keeps failing stops the stream,** unless it has a
  dead-letter topic. By default erpdoo never skips an event: when a webhook
  rejects one (a 4xx) or stays unreachable past its retries, erpdoo exits
  without confirming it and tries again after a restart. Nothing is lost,
  but nothing newer gets through until the receiver accepts it. With a
  dead-letter topic, failed events wait there for you instead.
- **Needs something to restart it.** When Postgres or a sink goes away while
  erpdoo runs, it logs the cause and exits without confirming the
  undelivered events, and relies on a supervisor (Docker's restart policy,
  systemd) to start it again. At startup it waits for Postgres and Kafka,
  retrying after 1 s and then twice as long each time, up to 30 s, so after
  a reboot it starts cleanly in whatever order the services come up.
- **erpdoo does not undo `REPLICA IDENTITY FULL`** set for earlier versions,
  or remove tables from a publication it did not create. Reset a table with
  `ALTER TABLE <table> REPLICA IDENTITY DEFAULT` for the smaller WAL (the
  dev stack's setup script does this).
- **Table names are inferred.** Odoo does not store a model's table name in
  the database, so erpdoo uses Odoo's naming rule (`sale.order` →
  `sale_order`) plus the core models known to differ (`ir.actions.*`). A
  model from another module that sets its own `_table` is not recognised
  yet. `python -m erpdoo.models <model>` shows how a model is mapped.
- **An unused replication slot keeps WAL.** If erpdoo is stopped for a long
  time, Postgres keeps the WAL it hasn't read yet and disk use grows. Drop the
  slot (`SELECT pg_drop_replication_slot('erpdoo')`) if you remove erpdoo.

## License

MIT
