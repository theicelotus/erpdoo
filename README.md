# erpdoo

An Odoo Kafka connector and webhooks: change data capture that turns Odoo's database changes into clean business events, with nothing installed inside Odoo.

[![CI](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml/badge.svg)](https://github.com/theicelotus/erpdoo/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](pyproject.toml)

<!-- ![erpdoo demo: confirming a sale order in Odoo and the event it produces](docs/demo.gif) -->

## What it does

erpdoo reads Odoo's PostgreSQL logical replication stream directly, with no Debezium and no Kafka Connect, and turns each committed transaction into business events such as `SaleOrderConfirmed` or `InvoicePaid`, with the customer, order lines and products filled in.

Nothing is installed inside Odoo, and no code is needed per event: each event is a short YAML rule, and six ship with erpdoo. Events go to Kafka (JSON Schema or Avro, with schemas registered automatically), a webhook or stdout, and messages from other systems can act on Odoo in the other direction.

Version 0.1: tested end to end; configuration may change before 1.0.

```
Odoo ──▶ PostgreSQL ──(logical replication)──▶ erpdoo ──▶ Kafka / Redpanda / webhook / stdout
  ▲                                              │
  └───────────(Odoo's JSON-RPC API)──────────────┴◀── Kafka (inbound messages)
```

## Features

- **No code per event.** YAML rules on state transitions, field changes, creates and deletes; packs for sales, invoicing, inventory and CRM ship with erpdoo ([events](docs/events.md), [rules](docs/rules.md)).
- **Complete events.** A transaction's changes are grouped and evaluated at commit, and related records come from a local state store, without querying Odoo ([architecture](docs/architecture.md)).
- **Exactly once to Kafka**, with Kafka transactions; at-least-once to webhooks, with deterministic event ids to dedupe on ([delivery](docs/sinks.md#delivery)).
- **Schemas registered automatically**, one subject per event type in JSON Schema or Avro, with breaking rule changes refused at startup ([schemas](docs/schemas.md)).
- **Backfill** of past events from Odoo's own field tracking, by itself after a lost replication slot or a late app install ([backfill](docs/backfill.md)).
- **Inbound rules**: messages from a warehouse or payment provider act on Odoo through its API, exactly once and without event loops ([inbound](docs/inbound.md)).
- **Live viewer** that shows each transaction's raw changes next to the events built from it ([viewer](docs/architecture.md#live-viewer)).
- **Fast**: 18.4 ms p50 from Postgres commit to Kafka consumer, and 2,500 to 3,000 events a second through a backlog, on one laptop ([performance](docs/performance.md)).

## Requirements

- Odoo 18 Community, on PostgreSQL 16: what erpdoo is developed and tested against.
- Python 3.11 or later, or Docker.
- A sink: Kafka or Redpanda (a schema registry is optional), or a webhook endpoint.
- **Replication access to Odoo's Postgres**: `wal_level=logical`, a role with `REPLICATION`, and a database admin who runs a few SQL statements once ([Postgres setup](docs/postgres-setup.md)). erpdoo's role stays read-only.
- **Or, for hosted Odoo** without replication access: [polling mode](docs/polling.md) reads Odoo's API instead, with real limits.

## Quick start

The dev stack runs Odoo 18 with demo data, Postgres 16, Redpanda with its schema registry, Redpanda Console and erpdoo. It needs Docker with Compose v2.

1. Start it. The first start downloads about 2 GB of images, then installs Odoo with demo data (about a minute).
   ```sh
   git clone https://github.com/theicelotus/erpdoo && cd erpdoo
   docker compose up -d --build
   ```
2. Follow erpdoo's log: `docker compose logs -f erpdoo`.
3. Open Odoo at http://localhost:8069 (login `admin`, password `admin`), go to **Sales**, open a quotation and click **Confirm**.
4. The `SaleOrderConfirmed` event appears in the log, and in Redpanda Console at http://localhost:8080: topic `erpdoo.events` has it as JSON Schema, `erpdoo.events.avro` in Avro, and **Schema Registry** lists each event type's schema. Posting a customer invoice gives an `InvoicePosted` event the same way.
5. Open the live viewer at http://localhost:8000 to see each transaction's raw changes next to its events.

A full example event is in [docs/events.md](docs/events.md#an-event).

## Installation

For your own self-hosted Odoo:

1. Have a database admin prepare Postgres once, as described in the [Postgres setup guide](docs/postgres-setup.md).
2. Install erpdoo in one of two ways:
   - **With pip**, from a clone, with the live viewer's extra:
     ```sh
     git clone https://github.com/theicelotus/erpdoo && pip install './erpdoo[viewer]'
     ```
   - **With Docker**, by building its image:
     ```sh
     git clone https://github.com/theicelotus/erpdoo && docker build -t erpdoo ./erpdoo
     ```
3. Copy the shipped rules from [rules/](rules/) and set `ERPDOO_PG_DSN` (for example `host=db dbname=odoo user=erpdoo password=...`), `ERPDOO_RULES` and `ERPDOO_STATE` (the state store's SQLite file, on persistent storage). All settings are in [Configuration](docs/configuration.md).
4. Run `erpdoo` under a supervisor (systemd, or Docker's restart policy). When Postgres or a sink goes away it exits without confirming what is undelivered, and continues from there when restarted.
5. Tell it where events go, in `erpdoo.yml` (or `ERPDOO_CONFIG`):
   ```yaml
   sinks:
     - type: kafka
       bootstrap_servers: ${KAFKA_BOOTSTRAP:-kafka:9092}
       topic: erpdoo.events
       schema_registry:
         url: http://schema-registry:8081
   ```

## Usage

A rule is a model, a condition and the related records to include. This is the shipped `SaleOrderConfirmed`, shortened ([rules/sales.yml](rules/sales.yml)):

```yaml
SaleOrderConfirmed:
  model: sale.order
  when:
    state: { to: sale }      # changed to sale in this transaction
  include:
    partner_id: [name, email, phone, vat, is_company]
    order_line: [name, product_uom_qty, price_unit, price_subtotal]
    order_line.product_id: [name, default_code]
```

A consumer on the dev stack, with confluent-kafka (`pip install 'confluent-kafka[json]'`), whose deserializer fetches each message's schema from the registry and checks the event against it:

```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

registry = SchemaRegistryClient({"url": "http://127.0.0.1:18081"})
deserialize = JSONDeserializer(None, schema_registry_client=registry)  # each message's own schema
consumer = Consumer({"bootstrap.servers": "127.0.0.1:19092", "group.id": "billing",
                     "auto.offset.reset": "earliest"})  # read_committed is librdkafka's default
consumer.subscribe(["erpdoo.events"])
while True:
    msg = consumer.poll(1.0)
    if msg is not None and not msg.error():
        event = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        print(event["event_id"], event["event_type"], event["record"]["name"])
```

Commands:

```sh
erpdoo                                    # stream events, by the rules and erpdoo.yml
erpdoo backfill --since 2026-10-01T00:00:00Z --until 2026-10-02T00:00:00Z [--dry-run]
python -m erpdoo.models sale.order        # how an Odoo model maps to its table
docker compose restart erpdoo             # dev stack: after editing rules/ or erpdoo.yml
```

## Documentation

- [Architecture](docs/architecture.md): how erpdoo works, the state store and the live viewer
- [Events](docs/events.md): the shipped events and the event format
- [Writing rules](docs/rules.md): the YAML rule format
- [Sinks and delivery](docs/sinks.md): Kafka, webhook, stdout, dead-letter topics, exactly once
- [Schemas](docs/schemas.md): JSON Schema and Avro, field types, compatibility, consumers
- [Inbound](docs/inbound.md): messages that act on Odoo, the integration user, loop safety
- [Polling mode](docs/polling.md): hosted Odoo through its API, and its limits
- [Backfill](docs/backfill.md): rebuilding past events, apps installed later
- [Setting up Postgres](docs/postgres-setup.md): the role, publication and grants
- [Slot health](docs/slot-health.md): replication lag, WAL limits, invalidated slots
- [Performance](docs/performance.md): latency, throughput, state store size, WAL cost
- [Configuration](docs/configuration.md): environment variables and erpdoo.yml

## Contributing

Bug reports and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) covers the dev stack and the checks every change must pass.

## License

[MIT](LICENSE)
