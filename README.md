# erpdoo

[![CI](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml/badge.svg)](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml)

Turn Odoo's raw database changes into clean business events (`SaleOrderConfirmed`,
`InvoicePaid`, ...) and stream them to Kafka and other sinks, with nothing
installed inside Odoo.

erpdoo reads PostgreSQL logical replication (pgoutput) directly. No Debezium,
no Kafka Connect: Postgres, one erpdoo process and a sink.

```
Odoo ──▶ PostgreSQL ──(logical replication)──▶ erpdoo ──▶ Kafka / Redpanda / webhook / stdout
  ▲                                              │
  └───────────(Odoo's JSON-RPC API)──────────────┴◀── Kafka (inbound messages)
```

> **Status: early development.** Events are defined by YAML rules, and six
> ship with erpdoo (below). Related records (partner, order lines, product
> names) come from a local SQLite copy kept current from the stream. Events
> go to Kafka (as JSON Schema or Avro, with each event type's schema in the
> schema registry), a webhook or stdout, and a live page shows them next to
> the database changes they came from. Inbound rules go the other way:
> messages from other systems act on Odoo through its API, exactly once.

## Try it

Requires Docker with Compose v2. The first start downloads about 2 GB of
images, then installs Odoo's Sales, Inventory, CRM and shipping apps with
demo data (about a minute), and an integration user for inbound messages
(see [Inbound](#inbound)). A one-shot `db-setup` container then prepares Postgres
the way a database admin would (see [Setting up Postgres](#setting-up-postgres)),
so erpdoo runs with a role that can stream and read but not write.

```sh
docker compose up -d --build
docker compose logs -f erpdoo
```

Open Odoo at http://localhost:8069 (login `admin`, password `admin`), go to
**Sales**, open a quotation and click **Confirm**. The event appears in the
erpdoo log and in Redpanda Console at http://localhost:8080: topic
`erpdoo.events` has it as JSON Schema, `erpdoo.events.avro` the same in
Avro, and **Schema Registry** lists each event type's schema (see [Schemas](#schemas)).
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

Events are keyed `sale.order:<id>`, so all events for one record stay in order
on one partition. `event_id` is derived from the Postgres transaction, the rule
and the record, so redelivered events carry the same id and consumers can dedupe.
Numeric values are strings (`"1500.00"`) so no precision is lost. Each event
type has a schema generated from its rule (see [Schemas](#schemas)).

To check the whole path from a script (CI runs it on every push):

- it drives every shipped event through Odoo's API, and checks actions that
  must not produce an event;
- it reads every event from both topics with confluent-kafka's own
  deserializers; each must be valid against the schema registered for its
  event type, and the Avro event must say the same as the JSON one;
- it compares every field of every event, related records and many2many
  lines included, with the value Odoo returns for it through JSON-RPC, by
  Odoo's own field types (see [scripts/event_checks.py](scripts/event_checks.py)).
  The comparison must cover booleans, dates, datetimes, numbers,
  selections and translated fields (the product gets a French name, and
  the event must show the English one). With the bug that once read
  booleans from snapshots as false put back, it fails with
  `record.partner_id.is_company: False, Odoo says True (boolean)`; the
  schema check alone passes, since `false` is a valid boolean;
- it checks that erpdoo's role cannot write to Odoo's tables.

```sh
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python scripts/smoke_test.py
```

`scripts/replay_test.py` checks the crash case: erpdoo delivers an event,
the customer is renamed, and erpdoo crashes before confirming the slot. The
event sent again after the restart must be identical to the first,
customer name included. It uses a test hook (`ERPDOO_TEST_HOLD_ACK` in
[docker-compose.yml](docker-compose.yml)) and also runs in CI.

`scripts/check_all.sh` rebuilds erpdoo and runs every check (unit tests and
the test scripts) against the dev stack, with a summary at the end.

`scripts/delivery_test.py`, also in CI, checks outages: it confirms orders
while erpdoo is stopped, then while Redpanda is down. Each event must
arrive exactly once on both topics once the service is back, and while
Redpanda is down the replication slot must not be confirmed past any of
the undelivered transactions. With erpdoo changed to confirm the slot
before delivering, it fails, naming the order whose event was lost.

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

- **kafka**: one topic, keyed `model:id`, written with an idempotent producer
  (`acks=all`). With `schema_registry`, messages are JSON Schema (`format: json`,
  the default) or Avro (`format: avro`), see [Schemas](#schemas); without it,
  plain JSON.
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
and so does a failure to write the dead-letter topic. Dead-letter messages
are plain JSON, without a schema.

Without a config file, events go to Kafka as set by `ERPDOO_KAFKA_BOOTSTRAP`
and `ERPDOO_TOPIC`.

## Schemas

With `schema_registry` on a Kafka sink, as in the dev stack, erpdoo
generates a schema for each rule from the rule and Odoo's field types,
registers it when it starts, and writes each message in the registry's
wire format: a zero byte, the schema's id (4 bytes), then the event.

```yaml
sinks:
  - type: kafka
    bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
    topic: erpdoo.events
    format: json                # JSON Schema (the default), or avro
    schema_registry:
      url: http://127.0.0.1:18081
      namespace: erpdoo         # the default
      # username: ...           # HTTP basic auth, with password
```

- **One subject per event type.** Several event types share a topic, so
  subjects follow the record name strategy: `<namespace>.<event type>`,
  e.g. `erpdoo.SaleOrderConfirmed`, which is also the schema's record name
  (Avro) or `title` (JSON Schema). Two sinks in different formats need
  different namespaces (the dev stack uses `erpdoo` and `erpdoo.avro`), and
  so do two erpdoo installations with different rules sharing a registry.
- **`format: json`**: JSON Schema (draft-07). After the 5-byte header, the
  message is the same JSON as without a registry, so a consumer that does
  not use the registry can skip those 5 bytes.
- **`format: avro`**: values are the same as in JSON. Amounts, dates and
  datetimes stay strings: Odoo's numeric columns have no fixed scale, which
  Avro's decimal type needs. A `json` or `properties` field is its JSON text.
  A key the JSON event leaves out is null in Avro (the fields of a related
  record that is not in the state store yet, which then has only its id).
- **Switching a topic to the registry.** Messages written before have no
  header, and a registry deserializer reading from the start of the topic
  fails on them ("Invalid magic byte"). Use a new topic, or start consumers
  after the switch.

How Odoo fields appear, in both formats:

| Odoo field | Postgres column | In the event (JSON Schema / Avro) |
|---|---|---|
| boolean | `bool` | boolean |
| integer, many2one (the id) | `int4` | integer / long |
| float without digits (`probability`) | `float8` | number / double |
| float with digits, monetary | `numeric` | string, `"1500.00"` |
| date | `date` | string, `YYYY-MM-DD` |
| datetime | `timestamp` | string, `YYYY-MM-DD HH:MM:SS[.ffffff]`, UTC |
| char, text, html, selection | `varchar`, `text` | string |
| translated char, text | `jsonb` | string, in `ERPDOO_LANG` (default `en_US`) |
| json, properties | `jsonb` | any JSON / its JSON text |

Every value may be null. The type comes from the Postgres column when the
field type does not decide it: a float is a number or a decimal string
depending on its `digits`. JSON Schema checks decimals, dates and datetimes
with a pattern; datetimes have no `T` or timezone, so they are not
`format: date-time`. A selection is a plain string rather than an enum,
because modules add values to selections, and each install would then be a
schema change. In `changes`, a translated field holds every language, as
Odoo stores it.

**Compatibility.** erpdoo sets each subject it creates to `BACKWARD`
(consumers using the newest schema can read every earlier event). When it
starts, it checks every rule's schema against the latest version of its
subject, and registers them only if all pass; otherwise it registers none
and stops with the registry's reasons:

```
ERROR erpdoo: the rules changed in a way that breaks the registered event schemas, so erpdoo registered none of the new ones (schema registry http://redpanda:8081, compatibility BACKWARD: ...):
  SaleOrderCancelled (subject erpdoo.SaleOrderCancelled): The new has a closed content model and is missing a property or item present at path record.amount_total in the old schema
Adding fields and includes is compatible; removing a field (JSON Schema) or changing its type is not. Undo the change, or publish it as a new event type (rename the rule) or under a new namespace (schema_registry: namespace: in erpdoo.yml). To replace the schema anyway, ...
  curl -X PUT -H 'Content-Type: application/json' -d '{"compatibility": "NONE"}' http://redpanda:8081/config/erpdoo.SaleOrderCancelled
```

| Rule change | JSON Schema | Avro |
|---|---|---|
| Add a field, an include, a transition condition | compatible | compatible |
| Remove a field or an include | breaking: objects are closed, so a consumer can count on every field listed | compatible: fields default to null |
| Change a field's type (in Odoo, or its column) | breaking | breaking |
| Rename the rule | a new subject | a new subject |

Only the envelope (`event_id`, `event_type`, `model`, `record_id`,
`occurred_at`, `changes`, `record`, `source`) and each record's `id` are
required, which is what keeps added fields compatible.

- Going back to an earlier version of a rule works: erpdoo finds that
  schema registered and writes with its id. The next change is still
  checked against the latest version, so a field the rollback removed
  counts as removed.
- To make a breaking change on purpose, rename the event or change the
  namespace, or allow it once for the subject as the message shows, restart
  erpdoo, and set the subject back to `BACKWARD`. erpdoo keeps a level set
  by hand, and warns at each start while it is not `BACKWARD`.
- A rule that activates while erpdoo runs (its app was just installed) has
  its schema registered then. If the registry refuses it, the rule stays
  inactive and erpdoo logs why. At startup erpdoo waits for an unreachable
  registry, as it does for Postgres and Kafka.

**In Redpanda Console** (http://localhost:8080), **Schema Registry** lists
the dev stack's subjects with their versions and compatibility, and the
topics show messages decoded in both formats. The same `SaleOrderConfirmed`
is about 970 bytes as JSON and 300 as Avro.

**Reading the events** with confluent-kafka (`pip install 'confluent-kafka[avro,json]'`):

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

registry = SchemaRegistryClient({"url": "http://localhost:18081"})
# Fetches each message's schema by its id. For erpdoo.events (JSON Schema):
# JSONDeserializer(None, schema_registry_client=registry), which also validates.
decode = AvroDeserializer(registry)
consumer = Consumer({"bootstrap.servers": "127.0.0.1:19092", "group.id": "erpdoo-demo",
                     "auto.offset.reset": "earliest"})
consumer.subscribe(["erpdoo.events.avro"])
while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    event = decode(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    print(event["event_type"], event["record_id"], event["record"]["partner_id"])
```

## Live viewer

With `viewer:` in [erpdoo.yml](erpdoo.yml) (on in the dev stack), erpdoo
serves a page at http://localhost:8000 that shows each transaction as it is
delivered: its inserts, updates (with the fields that changed, old and new)
and deletes on one side, and the events built from it on the other. It
needs the `viewer` extra (`pip install 'erpdoo[viewer]'`, included in the
Docker image). It has no login and shows business data, so keep it on
localhost or a private network. Its header shows how far the replication
slot is behind (see below).

## Slot health

Postgres keeps all WAL that erpdoo's replication slot has not confirmed.
While erpdoo keeps up, that is a few seconds' worth; if erpdoo is stopped,
or a sink blocks the stream, it grows until erpdoo catches up. Every
minute erpdoo logs how far the slot is behind, in bytes of WAL and in
seconds (Postgres' `flush_lag` for erpdoo's connection: how long WAL waits
before erpdoo confirms it; erpdoo confirms at least every 10 seconds):

```
INFO erpdoo: slot erpdoo: 469.7 kB of WAL not confirmed yet, 0.4 s behind; 12 rows fetched on demand so far
```

It logs a warning instead when the slot is more than 1 GB or 300 seconds
behind, or when Postgres is about to invalidate it. The thresholds are set
in [erpdoo.yml](erpdoo.yml):

```yaml
slot:
  lag_warning: 1GB          # bytes of WAL, or 512MB, 2GB, ...
  lag_warning_seconds: 300
  report_seconds: 60
```

**Limit what a slot can hold.** By default Postgres keeps WAL for a slot
without limit, so a slot left behind (erpdoo stopped for days) can fill
the disk and stop the database, Odoo included. Set
`max_slot_wal_keep_size` in `postgresql.conf` (or with
`ALTER SYSTEM SET max_slot_wal_keep_size = '10GB'` and
`SELECT pg_reload_conf()`, no restart needed) to cap it. Pick a size that
covers the longest erpdoo outage you want to survive: when a slot needs
more, Postgres invalidates it at the next checkpoint and removes the WAL.
With the cap set, erpdoo's lag line also says how much more the slot can
fall behind (`invalidated after 31.1 MB more`) and warns when Postgres
marks it as about to be invalidated.

If Postgres has invalidated the slot, erpdoo creates it again when it
starts, copies its state store again and logs the time window it could
not stream, from the last transaction it processed to the new slot:

```
ERROR erpdoo: replication slot erpdoo had been invalidated by Postgres, which removed WAL erpdoo had not read yet (it held more than max_slot_wal_keep_size); created it again. Changes committed between 2026-10-21T01:28:55.063437+00:00 and 2026-10-21T01:29:05.901836+00:00 were not streamed, so events from that time may be missing. They are backfilled from Odoo's records next. The state store is copied again.
INFO erpdoo: backfilled 2026-10-21T01:28:55.063437+00:00 to 2026-10-21T01:29:05.901836+00:00: 3 events (2 from Odoo's field tracking, 1 approximate)
```

It then rebuilds the events of that window from Odoo's own records (see
[Backfill](#backfill)) before it streams again; `backfill: {after_lost_slot:
false}` in erpdoo.yml turns that off. `scripts/backfill_test.py`, also run
in CI, checks this on the dev stack: with `max_slot_wal_keep_size = '64MB'`
and erpdoo stopped, it confirms an order, cancels another and wins a lead,
writes WAL elsewhere until Postgres invalidates the slot, and expects each
change exactly once as a backfilled event that fits its registered schema.

## Backfill

```sh
erpdoo backfill --since 2026-10-21T01:28:55Z --until 2026-10-21T01:29:06Z [--dry-run]
```

rebuilds the events the active rules would have produced in that window
and sends them to the configured sinks (`--dry-run` prints them instead).
In the dev stack: `docker compose exec erpdoo erpdoo backfill ...`. erpdoo
runs it by itself for the window of an invalidated slot. It reads what
Odoo recorded, not the WAL, so it also covers time erpdoo was not
running at all.

- **Rules on fields Odoo tracks** (those whose changes appear in a
  record's chatter: a sale order's or invoice's `state`, an invoice's
  `payment_state`, a delivery's `state`): each tracked change in the
  window is checked, with its real old and new values from
  `mail_tracking_value` and its time from `mail_message`. The event's
  `occurred_at` is that time and `changes` has the real `from` and `to`.
- **Other rules** (a field Odoo does not track, such as `crm.lead`'s
  `probability`, which `LeadWon` uses; create rules): erpdoo takes the
  records written (created) in the window whose current values match the
  rule. The previous values are unknown, so `from` conditions are not
  checked and `changes` has `"from": null`. These events are marked
  `"approximate": true`.

Each backfilled event says so in its source, and has an id built from the
rule, the record and the tracking message (or the record's
`write_date`), so running the same backfill again sends the same ids:

```json
"source": {"table": "sale_order", "backfill": true, "approximate": false, "mail_message_id": 65293}
```

Limits:

- **Other fields are as they are now.** The tracked change's own fields
  have the values of that moment, but the rest of the record and its
  related records (customer, lines) are read as they are when the
  backfill runs.
- **Ids differ from streamed events.** A change that was streamed and is
  also in a backfill window is sent twice, with two ids. erpdoo widens the
  window to whole seconds (Odoo stores times without fractions), so a
  change in the first second of an automatic backfill can be one.
- **Approximate events can be wrong.** A record written in the window for
  another reason, one that already matched the rule before, gives an
  event too: a lead won last month and edited today counts as won today,
  and an Odoo module update that rewrites records makes them all count.
  A record that matched in the window but no longer does gives none, and
  since its id comes from the `write_date`, writing the record again
  changes the id.
- **Only one event per tracked change, and only what Odoo tracked.** A
  change made without tracking (SQL, some imports, a record created
  directly in its final state) is not found. Delete rules cannot be
  backfilled, and deleted records are skipped.
- **It needs read access to Odoo's tracking:** `mail_tracking_value`,
  `ir_model_fields_selection`, and four columns of `mail_message` (the
  record and the time, not the message text); see the SQL below.

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

## Inbound

erpdoo can also act on Odoo when other systems send messages: a warehouse
reports a shipment, a payment provider reports a payment. Inbound rules
(YAML, in [rules/inbound/](rules/inbound/)) map a topic and message type to
an Odoo action. erpdoo finds the record the message is about, then writes
fields, calls a method or runs a wizard, through Odoo's JSON-RPC API as a
dedicated integration user. Odoo's own access rights, rules and checks
apply, as if that user clicked the button. erpdoo's Postgres role stays
read-only: no inbound write goes to the database directly.

The two shipped rules:

```yaml
DeliveryShipped:                       # {"id": "wh-81723", "type": "DeliveryShipped",
  topic: warehouse.events              #  "reference": "WH/OUT/00042", "tracking_number": "1Z999..."}
  model: stock.picking
  find: { name: "{reference}", picking_type_code: outgoing }
  already_done: { state: done }
  steps:
    - write: { carrier_tracking_ref: "{tracking_number}" }
    - call: button_validate
      context: { skip_sms: true, skip_backorder: true }
  expect: { state: done }

PaymentReceived:                       # {"id": "evt_3Q7x", "type": "PaymentReceived", "invoice": "INV/2026/00042",
  topic: payments.events               #  "amount": 1725.0, "date": "2026-10-21", "payment_id": "pi_3Q7xY2"}
  model: account.move
  find: { name: "{invoice}", move_type: out_invoice, state: posted }
  already_done:
    model: account.payment
    find: { memo: "{payment_id}" }
  steps:
    - wizard: account.payment.register   # Odoo's Register Payment dialog
      values: { amount: "{amount}", payment_date: "{date}", communication: "{payment_id}" }
      call: action_create_payments
  expect: { payment_state: [partial, in_payment, paid] }
```

- **Messages** are JSON objects, plain or in the schema registry's JSON
  wire format. A rule matches by `topic` and by `match` (default: the
  field `type` equals the rule's name); `id` names the field with the
  message's unique id (default `id`). Messages of a type no rule matches
  are ignored.
- **`find`** must give exactly one record; a list value means "is one of".
  `"{path}"` takes a value from the message (dotted for nested fields; alone
  it keeps its type, inside a longer string it is formatted into it), and
  `"{record.id}"` is the record found.
- **Steps**: `write: {field: value}`, `call: method` (with optional
  `args` and `context`), or `wizard: model` with `values` and `call`
  (created with `active_model`, `active_id` and `active_ids` of the record,
  as Odoo's buttons do). Only public methods: Odoo refuses names starting
  with `_` over its API.
- **`expect`** is checked after the steps. A method that needs a person's
  answer (a backorder question, say) returns a wizard instead of acting;
  `expect` turns that into a failure that names the wizard, rather than a
  message that silently did nothing.
- At startup erpdoo checks the rules against Odoo's models: an unknown
  field stops it, a rule for a model that is not installed is skipped with a
  warning (and needs a restart once its app is installed).

Turn it on with `inbound:` in [erpdoo.yml](erpdoo.yml):

```yaml
inbound:
  bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
  odoo:
    url: http://odoo:8069
    db: odoo
    login: erpdoo-integration
    api_key: ${ERPDOO_ODOO_API_KEY}
  # group_id: erpdoo-inbound
  # dead_letter_topic: erpdoo.inbound.dead-letter
  # backoff: 1         # seconds, doubling up to max_backoff
  # max_backoff: 60
  # keep_days: 30      # how long processed message ids are kept
```

To try it on the dev stack, find a delivery waiting in Inventory and send:

```sh
echo '{"id": "wh-1", "type": "DeliveryShipped", "reference": "WH/OUT/00042", "tracking_number": "1Z999"}' \
  | docker compose exec -T redpanda rpk topic produce warehouse.events
docker compose logs erpdoo | grep inbound
```

### The integration user

The dev stack creates `erpdoo-integration` with a fixed API key (in
[docker-compose.yml](docker-compose.yml), dev only). For your own Odoo:

1. **Settings → Users & Companies → Users → New**: name it, login
   `erpdoo-integration`, type **Internal User**. Give it only the groups the
   rules need. For the shipped rules: **Inventory: User** (validate
   deliveries) and **Invoicing: Invoicing** (register payments); leave
   Sales, Settings and everything else empty. Odoo's groups are the
   smallest unit of rights, so the user can do whatever those groups allow
   (Invoicing, for one, also lets it edit sale orders), and Odoo refuses
   anything else: with these two groups it cannot change a contact.
2. Give it a password, log in as it, open **My Profile → Account Security
   → New API Key**, and keep the key for `api_key` (an environment
   variable, as above). Or as an admin, in `odoo shell`:
   ```python
   user = env["res.users"].search([("login", "=", "erpdoo-integration")])
   print(env["res.users.apikeys"].with_user(user).sudo()._generate(None, "erpdoo", None)); env.cr.commit()
   ```
3. Use this user for erpdoo only, never for people: erpdoo recognises
   the changes it caused by this user (see the loop marker below).

### Exactly once

Messages are carried out one at a time, in order per partition:

1. A message whose id erpdoo noted as processed is a repeat, and is
   skipped (ids are kept `keep_days`, in the state store, which survives a
   recreated replication slot).
2. erpdoo carries it out in Odoo, notes its id, and only then commits the
   Kafka offset. If erpdoo stops before the commit, the message comes again
   and is a repeat. If it stops in the moment between Odoo's commit and the
   note, the message is carried out again unless the rule's `already_done`
   recognises it (the delivery is done; a payment with that memo exists),
   so give every rule one.
3. A message that cannot be carried out (no matching record, Odoo refused
   it with a validation or user error, not JSON, no id) goes to the
   dead-letter topic as `{"message": ..., "error": ..., "rule": ...,
   "message_id": ..., "topic": ..., "partition": ..., "offset": ...,
   "failed_at": ...}`, and the stream moves on. Only processed ids are
   noted, so a dead-lettered message can be sent again once fixed.
4. When Odoo or Postgres is away, or Odoo hits a passing database
   conflict, the message is retried with backoff (1 s, doubling up to
   60 s) for as long as it takes, and the messages after it wait. A refused
   API key or a missing access right is retried the same way and logged as
   an error, since the messages should go through once an admin fixes it.

Each step is its own Odoo call and transaction: a message whose second
step fails has had its first step done. A retry runs every step again, so
steps must be safe to repeat (writes are; `button_validate` skips done
deliveries).

### Loop safety

An event caused by an inbound message says so:

```json
"source": {"xid": 58123, "commit_lsn": "0/8E21A0C8",
           "inbound": {"message_id": "wh-81723", "topic": "warehouse.events", "rule": "DeliveryShipped"}}
```

so the warehouse can tell the `DeliveryDone` it caused from one a person
caused. A sink can leave them out, so a system does not get its own changes
back:

```yaml
sinks:
  - type: webhook
    url: https://warehouse.example.com/hooks/odoo
    skip_inbound: [warehouse.events]   # or true: events caused by any inbound topic
```

erpdoo notes Postgres' WAL position right before and right after each
message's Odoo calls. A transaction that commits in between, and in which
the integration user wrote a row (`write_uid`), was caused by that message,
since erpdoo carries out one message at a time and nothing else uses that
user. Resent events are marked the same way. Changes made as the
integration user outside erpdoo, and backfilled events, are not marked.

`scripts/inbound_test.py`, also in CI, sends both kinds of messages on the
dev stack and checks the outcome in Odoo, the marker on the events and the
stdout sink leaving out the warehouse's, a repeated message, the same
payment in a new message, a message for a missing invoice (dead-lettered),
and a payment sent while Odoo is stopped (carried out once, after it is
back).

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

-- For backfill: Odoo's field tracking. Of mail_message only which record
-- changed when, not the messages themselves.
GRANT SELECT (id, model, res_id, date) ON mail_message TO erpdoo;
GRANT SELECT ON mail_tracking_value, ir_model_fields_selection TO erpdoo;
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

Inbound messages need nothing more: they act through Odoo's API as the
integration user, and erpdoo only reads Postgres' current WAL position for
them (`pg_current_wal_insert_lsn()`, open to every role).

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
erpdoo runs the six shipped rules with the dev config (two Kafka sinks, JSON
Schema and Avro with the schema registry, and stdout; viewer on), and each
event includes the partner and order lines from the state store. Latency is
from the Postgres commit to the moment a Kafka consumer of the JSON Schema
topic receives the event. Results from two consecutive runs:

| | Run 1 | Run 2 |
|---|---|---|
| Latency, one order at a time (200 orders), p50 / p95 / p99 | 18.4 / 20.2 / 30.1 ms | 18.5 / 21.9 / 50.2 ms |
| Burst while erpdoo runs (3,000 orders, 25 per transaction, 8 concurrent clients) | 159 events/s | 157 events/s |
| Latency during that burst, p50 / p95 / p99 | 30 / 89 / 95 ms | 36 / 90 / 101 ms |
| Last event after the last commit | 0.05 s | 0.05 s |
| Backlog of 3,000 confirmed orders, erpdoo started afterwards | 2,993 events/s (1.00 s) | 2,520 events/s (1.19 s) |
| erpdoo startup (container start to streaming) | 0.89 s | 0.93 s |

How to read it:

- In the burst, Odoo is the limit: it committed about 160 confirmations a
  second, and erpdoo delivered each within about 100 ms, finishing 50 ms
  after the last commit.
- The backlog run takes Odoo out of the picture: erpdoo works through 3,000
  waiting confirmations at 2,500 to 3,000 events a second, each written to
  both topics, including the state store lookups for each event's partner
  and order lines. The orders
  were created while erpdoo ran, so their lines were already in the store;
  orders from before erpdoo started cost one query each for their lines.
- No events were missing or duplicated in any phase.
- Schemas cost no measurable time. An earlier measurement, with one Kafka
  sink writing plain JSON, gave 13.2 ms at p50; that same configuration
  gives 18.8 ms on this machine today (and one JSON Schema sink alone
  18.6 ms), with a dev database grown to about 47,000 orders by the test
  runs. The difference is the machine and its data, not the formats.
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
  `sale_order`) plus the core models known to differ (`ir.actions.*`). For
  a model that sets its own `_table`, erpdoo looks for the table by its
  columns: a table no other model uses, with an `id`, whose columns include
  every stored field of the model. It logs which table it found, and does
  not guess when two tables fit equally well. To be sure, or for a model it
  cannot find, set the table in [erpdoo.yml](erpdoo.yml):
  ```yaml
  tables:
    x_custom.gadget: x_custom_gadgets
  ```
  `python -m erpdoo.models <model>` shows how a model is mapped, and
  `scripts/custom_table_test.py` checks both with a small test module
  ([tests/odoo_addons](tests/odoo_addons)) that sets its own `_table`.
- **Schemas hold rules to their word.** With a schema registry, removing a
  field from a rule (JSON Schema) or changing a field's type stops erpdoo
  until you choose how to publish the change (see [Schemas](#schemas)),
  because consumers may depend on it. Schemas say what erpdoo sends, not
  every value Odoo allows: selections are plain strings, and every value
  may be null.
- **Inbound is exactly once only with `already_done`.** erpdoo notes a
  message as processed right after Odoo commits its action; if erpdoo stops
  in that moment, the message is carried out again unless its rule can tell
  it was done. A rule's steps are separate Odoo transactions, so a failure
  can leave the first steps done (they run again on retry). Messages are
  handled one at a time, so one waiting for Odoo holds back the rest, and
  only one erpdoo should consume the inbound topics: the processed ids live
  in its state store.
- **An unused replication slot keeps WAL.** If erpdoo is stopped for a long
  time, Postgres keeps the WAL it hasn't read yet and disk use grows, without
  limit unless `max_slot_wal_keep_size` is set (see
  [Slot health](#slot-health)). With the limit, a slot that needs more is
  invalidated: erpdoo then starts over with a new slot, logs the window it
  missed and backfills it from Odoo's records, which is exact only for
  fields Odoo tracks (see [Backfill](#backfill)). Drop the slot
  (`SELECT pg_drop_replication_slot('erpdoo')`) if you remove erpdoo.

## License

MIT
