# Configuration

erpdoo is configured by environment variables (where Postgres is, where the
rules and the state store live) and by one YAML file, `erpdoo.yml` (sinks
and everything else). This page lists both, with links to the page that
explains each setting.

- [Environment variables](#environment-variables)
- [erpdoo.yml](#erpdooyml)

## Environment variables

| Variable | Default | What it sets |
|---|---|---|
| `ERPDOO_PG_DSN` | `host=localhost dbname=odoo user=erpdoo password=erpdoo` | Odoo's Postgres, as a libpq connection string (see [Setting up Postgres](postgres-setup.md)) |
| `ERPDOO_CONFIG` | `erpdoo.yml` in the working directory, if it exists | the config file |
| `ERPDOO_RULES` | `rules` | the event rules, a file or directory (see [Writing rules](rules.md)) |
| `ERPDOO_INBOUND_RULES` | `inbound` in the rules directory | the inbound rules (see [Inbound](inbound.md)) |
| `ERPDOO_STATE` | `erpdoo-state.db` | the SQLite state store; keep it on persistent storage (see [The state store](architecture.md#the-state-store)) |
| `ERPDOO_SLOT` | `erpdoo` | the replication slot |
| `ERPDOO_PUBLICATION` | `erpdoo` | the publication |
| `ERPDOO_LANG` | `en_US` | the language of translated fields (see [How Odoo fields appear](schemas.md#how-odoo-fields-appear)) |
| `ERPDOO_LOG_LEVEL` | `INFO` | the log level |
| `ERPDOO_KAFKA_BOOTSTRAP`, `ERPDOO_TOPIC`, `ERPDOO_SCHEMA_REGISTRY` | `127.0.0.1:19092`, `erpdoo.events`, none | the Kafka sink used when there is no config file (see [Sinks](sinks.md#configuring-sinks)) |

The Docker image sets `ERPDOO_RULES=/app/rules`,
`ERPDOO_CONFIG=/app/erpdoo.yml` and `ERPDOO_STATE=/var/lib/erpdoo/state.db`.

## erpdoo.yml

Strings may read environment variables with `${VAR}` or `${VAR:-default}`,
so secrets can stay out of the file. The dev stack's
[erpdoo.yml](../erpdoo.yml) has every section, with comments.

| Section | What it sets | Details |
|---|---|---|
| `sinks` | where events go: `kafka`, `webhook`, `stdout`, each with optional `dead_letter` and `skip_inbound` | [Sinks](sinks.md), [Schemas](schemas.md) |
| `delivery` | Kafka transactions (exactly once), the `transactional_id` and checkpoint topic | [Delivery](sinks.md#delivery) |
| `source` | replication (the default) or `type: polling` with Odoo's API | [Polling mode](polling.md) |
| `inbound` | messages that act on Odoo, and the integration user | [Inbound](inbound.md) |
| `tables` | table names of models that set their own `_table` | [Tables of custom models](postgres-setup.md#tables-of-custom-models) |
| `slot` | when to warn about replication slot lag | [Slot health](slot-health.md) |
| `backfill` | `after_lost_slot`: backfill the window of an invalidated slot | [Slot health](slot-health.md#when-postgres-invalidates-the-slot) |
| `viewer` | the live viewer's `host` and `port` | [Live viewer](architecture.md#live-viewer) |
