# Architecture

How erpdoo turns Odoo's database changes into events: the pieces it is made
of, the path a change takes, and the live viewer that shows that path. For
the format of the events themselves, see [Events](events.md).

- [Overview](#overview)
- [The pieces](#the-pieces)
- [The state store](#the-state-store)
- [Live viewer](#live-viewer)

## Overview

erpdoo reads PostgreSQL logical replication (pgoutput) directly. No Debezium,
no Kafka Connect: Postgres, one erpdoo process and a sink. Nothing is
installed inside Odoo.

```
Odoo ──▶ PostgreSQL ──(logical replication)──▶ erpdoo ──▶ Kafka / Redpanda / webhook / stdout
  ▲                                              │
  └───────────(Odoo's JSON-RPC API)──────────────┴◀── Kafka (inbound messages)
```

The lower path is optional: [inbound rules](inbound.md) let messages from
other systems act on Odoo through its API. Where erpdoo cannot reach
Postgres (hosted Odoo), it can read changes through Odoo's API instead, in
[polling mode](polling.md).

## The pieces

- **Replication reader.** erpdoo creates its own replication slot (and, if
  its role may, the publication; see [Setting up Postgres](postgres-setup.md)),
  streams and decodes pgoutput, and confirms a transaction to Postgres only
  after every sink has delivered its events (see [Delivery](sinks.md#delivery)).
  After a restart it resumes from the confirmed position.
- **Odoo model reader.** erpdoo reads Odoo's model list (`ir_model`,
  `ir_model_fields`) to map tables to models and follow many2one, one2many
  and many2many fields. Both tables are in the publication, so erpdoo
  notices when an Odoo app is installed. Odoo does not store table names;
  see [Tables of custom models](postgres-setup.md#tables-of-custom-models).
- **Transaction grouping.** All changes of one Postgres transaction are
  buffered and the rules are evaluated at its commit, so an event is never
  half-built (confirming an order touches several tables at once).
- **State store.** A local SQLite copy of the records events need (below).
- **Rule engine.** YAML rules decide which changes become which events
  (see [Writing rules](rules.md)).
- **Sinks.** Kafka, webhook and stdout (see [Sinks](sinks.md)), with
  schemas in a schema registry (see [Schemas](schemas.md)).

## The state store

Related records (partner, order lines, product names) come from a local
SQLite copy kept current from the stream, so events have full details
without querying Odoo. It lives at `ERPDOO_STATE` (default
`erpdoo-state.db`; in the Docker image `/var/lib/erpdoo/state.db`, a volume
in the dev stack).

- **The rules' own tables** (`sale_order`, `account_move`, `stock_picking`
  and `crm_lead` for the shipped rules) are copied into the store when
  erpdoo starts tracking them, with only the columns the rules use. erpdoo
  takes the previous row of each update and delete from there, so Odoo's
  tables keep their default replica identity (see
  [Replica identity](postgres-setup.md#replica-identity)). That costs disk
  space and a longer first copy on a large database (`account_move` holds
  every journal entry), for little WAL saved (see
  [WAL per order confirmation](performance.md#wal-per-order-confirmation)).
  Changes committed before a table was copied are not checked against
  rules, except those of an app installed later, which are backfilled (see
  [Apps installed later](backfill.md#apps-installed-later)). A rule table
  without an `id` column would still need `REPLICA IDENTITY FULL`, which
  erpdoo then asks for; Odoo's models always have one.
- **Tables a rule includes** (partners, products, order and invoice lines)
  are not copied: erpdoo reads a row from Postgres the first time an event
  needs it (an order's lines together) and keeps it current from the stream
  after that. Such a row is as Postgres has it then, which can be newer than
  the event's transaction while erpdoo works through a backlog. The join
  table of a many2many include (`account_move_line_account_tax_rel` for
  taxes) must be published like the other tables.
- **Resent events equal the first**, related records included: the state
  store keeps the previous versions of changed rows until the slot is
  confirmed (a related row first read from Postgres during the resend is
  current).

The size and startup time this gives are in
[State store size and startup](performance.md#state-store-size-and-startup).
The state store also keeps the processed ids of [inbound](inbound.md)
messages and the default Kafka `transactional_id` (see
[Delivery](sinks.md#delivery)).

## Live viewer

With `viewer:` in [erpdoo.yml](../erpdoo.yml) (on in the dev stack), erpdoo
serves a page at http://localhost:8000 that shows each transaction as it is
delivered: its inserts, updates (with the fields that changed, old and new)
and deletes on one side, and the events built from it on the other.

```yaml
viewer:
  host: 0.0.0.0
  port: 8000
```

- It needs the `viewer` extra (`pip install 'erpdoo[viewer]'`, included in
  the Docker image).
- **It has no login and shows business data**, so keep it on localhost or
  a private network (the dev stack publishes it on 127.0.0.1 only).
- Its header shows how far the replication slot is behind (see
  [Slot health](slot-health.md)).
- In polling mode it shows each poll that found something like a
  transaction, with its API calls, and the load in its status line (see
  [Load on Odoo](polling.md#load-on-odoo)).
