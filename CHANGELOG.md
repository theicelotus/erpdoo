# Changelog

All notable changes to erpdoo are listed here. Versions follow
[semantic versioning](https://semver.org); before 1.0, a minor version may
change the configuration.

## 0.1.0 (2026-10-21)

The first release. It is developed and tested end to end against Odoo 18
Community on PostgreSQL 16.

### Reading Odoo's changes

- Reads PostgreSQL logical replication (pgoutput) directly, with no
  Debezium, no Kafka Connect and nothing installed inside Odoo. Works with
  a replication slot and publication it creates, or ones a database admin
  set up, with a read-only role ([Postgres setup](docs/postgres-setup.md)).
- Maps tables to Odoo models from `ir_model` and `ir_model_fields`,
  follows module installs, and finds the tables of models that set their
  own `_table`.
- Groups each transaction's changes and evaluates rules at commit, so an
  event is never half-built.
- Keeps related records (customers, order lines, products, taxes) in a
  local SQLite state store, read as of each transaction's commit, so events
  carry them without querying Odoo ([architecture](docs/architecture.md)).
- Waits for Postgres and Kafka at startup, and resumes from the last
  confirmed position after a restart.
- Reports replication slot lag, and recreates a slot Postgres invalidated
  ([slot health](docs/slot-health.md)).
- Polling mode for hosted Odoo without database access: reads changes
  through Odoo's JSON-RPC API as a read-only user, with documented limits
  ([polling mode](docs/polling.md)).

### Events

- YAML rules on state transitions, field changes, creates and deletes,
  with many2one, one2many and many2many records included
  ([writing rules](docs/rules.md)). Rules for apps that are not installed
  are skipped, and activate when the app is installed.
- Six shipped events ([events](docs/events.md)): `SaleOrderConfirmed`,
  `SaleOrderCancelled`, `InvoicePosted`, `InvoicePaid`, `DeliveryDone` and
  `LeadWon`.
- Events keyed `model:id`, with deterministic event ids for consumers to
  dedupe on.
- `erpdoo backfill` rebuilds the events of a past window from Odoo's field
  tracking; it also runs by itself after a lost replication slot and for
  apps installed later ([backfill](docs/backfill.md)).

### Sinks and delivery

- Kafka (or Redpanda), webhook and stdout sinks, each with an optional
  dead-letter topic ([sinks](docs/sinks.md)).
- Exactly once to Kafka, in Kafka transactions; at-least-once with other
  sinks. The replication slot is confirmed only after every sink has
  delivered.
- JSON Schema or Avro, with each event type's schema generated from its
  rule and registered in the schema registry; breaking rule changes are
  refused at startup ([schemas](docs/schemas.md)).
- Webhooks are retried with backoff and can be signed with an HMAC.

### Inbound

- Messages from other systems act on Odoo through its API, as a dedicated
  integration user, exactly once, with retries and a dead-letter topic.
  Events they cause are marked, so sinks can leave them out and no event
  loop forms ([inbound](docs/inbound.md)). Rules for a warehouse and a
  payment provider ship with erpdoo.

### Running it

- A live viewer (the `viewer` extra) that shows each transaction's raw
  changes next to the events built from it.
- A Docker image, and a `docker compose` dev stack with Odoo 18 and demo
  data, Postgres 16, Redpanda and its schema registry.
- Unit tests, and tests against the full stack in CI: every shipped event
  checked against its schema and Odoo's values, crashes and outages during
  delivery, inbound messages, backfill and polling mode. Measured latency
  and throughput are in [performance](docs/performance.md).
