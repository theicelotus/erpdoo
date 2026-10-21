# Inbound

erpdoo can also act on Odoo when other systems send messages: a warehouse
reports a shipment, a payment provider reports a payment. This page covers
inbound rules, the Odoo user they act as, how each message is carried out
exactly once, and how erpdoo keeps a system from getting its own changes back.

- [How it works](#how-it-works)
- [The shipped rules](#the-shipped-rules)
- [The rule format](#the-rule-format)
- [Turning it on](#turning-it-on)
- [The integration user](#the-integration-user)
- [Exactly once](#exactly-once)
- [Loop safety](#loop-safety)

## How it works

Inbound rules (YAML, in [rules/inbound/](../rules/inbound/), or
`ERPDOO_INBOUND_RULES`) map a topic and message type to an Odoo action.
erpdoo finds the record the message is about, then writes fields, calls a
method or runs a wizard, through Odoo's JSON-RPC API as a dedicated
integration user. Odoo's own access rights, rules and checks apply, as if
that user clicked the button. erpdoo's Postgres role stays read-only: no
inbound write goes to the database directly, and inbound messages need
nothing more in Postgres (erpdoo only reads Postgres' current WAL position
for them, with `pg_current_wal_insert_lsn()`, open to every role).

Inbound rules work in [polling mode](polling.md) too, with the limits of
its loop marker. On Odoo Online they need the Custom plan (see
[API access](polling.md#api-access)).

## The shipped rules

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

## The rule format

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
- **`already_done`** recognises a message that was already carried out
  (see [Exactly once](#exactly-once)).
- **`expect`** is checked after the steps. A method that needs a person's
  answer (a backorder question, say) returns a wizard instead of acting;
  `expect` turns that into a failure that names the wizard, rather than a
  message that silently did nothing.
- At startup erpdoo checks the rules against Odoo's models: an unknown
  field stops it, a rule for a model that is not installed is skipped with a
  warning (and needs a restart once its app is installed).

The full reference is in [src/erpdoo/inbound.py](../src/erpdoo/inbound.py).

## Turning it on

With `inbound:` in [erpdoo.yml](../erpdoo.yml):

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

## The integration user

The dev stack creates `erpdoo-integration` on its first start, when it
installs Odoo ([docker/odoo/init.py](../docker/odoo/init.py)), with a fixed
API key (in [docker-compose.yml](../docker-compose.yml), dev only). For your
own Odoo:

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
   the changes it caused by this user (see [Loop safety](#loop-safety)).

## Exactly once

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
   noted, so a dead-lettered message can be sent again once fixed. Unless
   `delivery: {transactions: false}`, the dead-letter record and the
   consumer offset are committed in one Kafka transaction, so a crash
   cannot dead-letter a message twice. The events a message causes are not
   in that transaction: they come from Odoo's changes, through the stream,
   like any other.
4. When Odoo or Postgres is away, or Odoo hits a passing database
   conflict, the message is retried with backoff (1 s, doubling up to
   60 s) for as long as it takes, and the messages after it wait. A refused
   API key or a missing access right is retried the same way and logged as
   an error, since the messages should go through once an admin fixes it.

Limits:

- **Steps are not one transaction.** Each step is its own Odoo call and
  transaction: a message whose second step fails has had its first step
  done. A retry runs every step again, so steps must be safe to repeat
  (writes are; `button_validate` skips done deliveries).
- **Only one erpdoo** should consume the inbound topics, since the
  processed ids live in its state store.

## Loop safety

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
user. Resent events are marked the same way.

Limits:

- Changes made as the integration user outside erpdoo, and backfilled
  events, are not marked.
- In polling mode the marker is matched by time instead (see
  [Polling mode limits](polling.md#limits)).
