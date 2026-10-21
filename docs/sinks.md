# Sinks and delivery

Where events go, and what erpdoo guarantees about getting them there:
exactly once to Kafka, at-least-once to anything else. Message formats and
the schema registry are on their own page, [Schemas](schemas.md).

- [Configuring sinks](#configuring-sinks)
- [Kafka](#kafka)
- [Webhook](#webhook)
- [stdout](#stdout)
- [Dead-letter topics](#dead-letter-topics)
- [Delivery](#delivery)
  - [Exactly once to Kafka](#exactly-once-to-kafka)
  - [At-least-once otherwise](#at-least-once-otherwise)
  - [Resent events](#resent-events)
  - [Something must restart erpdoo](#something-must-restart-erpdoo)

## Configuring sinks

Sinks are set in [erpdoo.yml](../erpdoo.yml) (`ERPDOO_CONFIG`, or
`erpdoo.yml` in the working directory), also mounted into the dev stack's
container. Events go to every sink listed, and the replication slot is
confirmed only after all of them have delivered. Strings may read
environment variables with `${VAR}` or `${VAR:-default}`.

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

Without a config file, events go to Kafka as set by `ERPDOO_KAFKA_BOOTSTRAP`
and `ERPDOO_TOPIC` (and `ERPDOO_SCHEMA_REGISTRY`, optional).

Any sink can leave out the events caused by inbound messages, with
`skip_inbound` (see [Loop safety](inbound.md#loop-safety)).

## Kafka

One topic, keyed `model:id`, written with an idempotent producer
(`acks=all`), in Kafka transactions when every sink is Kafka (see
[Delivery](#delivery)). With `schema_registry`, messages are JSON Schema
(`format: json`, the default) or Avro (`format: avro`), see
[Schemas](schemas.md); without it, plain JSON.

## Webhook

One `POST` per event, in order, with the event as the JSON body and
`X-Erpdoo-Event-Id` / `X-Erpdoo-Event-Type` headers.

- With `secret`, `X-Erpdoo-Signature: sha256=<hex>` is the HMAC-SHA256 of
  the raw body.
- Network errors, timeouts, 5xx, 408 and 429 are retried with exponential
  backoff (`retries`, default 5; `backoff`, default 1 s, doubling up to
  60 s; `Retry-After` is honoured); any other 4xx counts as a rejection.
- `timeout` is the time per request, in seconds (default 10).

## stdout

One JSON event per line, for debugging (`docker compose logs erpdoo`).

## Dead-letter topics

By default an event a sink cannot deliver (a webhook rejects it, or stays
unreachable past its retries) stops erpdoo without confirming it, and it is
tried again after a restart: nothing is lost, but nothing newer gets
through until the receiver accepts it. To keep going instead, give the sink
a dead-letter topic:

```yaml
  - type: webhook
    url: https://example.com/hooks/erpdoo
    dead_letter:
      topic: erpdoo.dead-letter
      bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
```

An event the sink rejects, or that still fails after the retries, is then
written to that topic as `{"event": ..., "error": ..., "sink": ...,
"failed_at": ...}` and the stream continues.

- A sink failure that is not about one event, such as an unreachable Kafka
  broker, still stops erpdoo, and so does a failure to write the
  dead-letter topic.
- Dead-letter messages are plain JSON, without a schema.

## Delivery

Exactly once to Kafka, at-least-once otherwise.

### Exactly once to Kafka

**When every sink is Kafka** (on one cluster, dead-letter topics
included), delivery is exactly once, with Kafka transactions. erpdoo
writes each batch's events to every sink's topic (the JSON Schema and the
Avro topic alike) together with a checkpoint record of how far it got, in
one transaction: the end of the batch's last Postgres transaction (its
LSN) in replication mode, the cursors in polling mode. Checkpoints go to a
compacted topic, `erpdoo.checkpoints`, keyed by the producer's
`transactional.id`. Only then is the slot confirmed. At startup erpdoo
fences any earlier producer with its `transactional.id` (Kafka aborts that
one's open transaction), reads its last checkpoint, skips the transactions
Postgres streams again up to it, and confirms the slot there. So:

- a crash after the Kafka commit but before the slot is confirmed sends
  nothing twice;
- a crash before the commit leaves an aborted transaction, and the batch
  is sent again, once.

**Consumers must read with `isolation.level=read_committed`**, or they
also see the records of aborted transactions. That is librdkafka's default
(confluent-kafka, kcat, most non-Java clients); the Java client and Kafka
Connect default to `read_uncommitted`, so set it there:

```properties
isolation.level=read_committed
```

In polling mode a cursor alone cannot say what was delivered (the overlap
is read again, compared with what the state store noted), so each poll is
noted with its events, and the checkpoint of the transaction that delivers
them lists their ids; after a crash, the noted events it lists are
forgotten instead of sent again. Backfilled events go the same way in both
modes.

```yaml
delivery:
  transactions: true                    # the default; false: at-least-once
  transactional_id: erpdoo-warehouse-1  # default: erpdoo-<random id kept in the state store>
  checkpoint_topic: erpdoo.checkpoints
```

- The default `transactional_id` is unique per state store, so two erpdoos
  never fence each other. If you set one, give each erpdoo its own.
- A new state store (a deleted volume) starts without a checkpoint: that
  one restart is at-least-once.
- The `erpdoo backfill` command does not use transactions, so it cannot
  fence a running erpdoo.

### At-least-once otherwise

**With a webhook or stdout sink** (or `transactions: false`, or Kafka
sinks on different clusters), delivery is at-least-once, and erpdoo logs
why at startup. If it stops after a sink delivered but before the slot is
confirmed, or one sink fails after another succeeded, the events are sent
again after a restart with the same `event_id`, so consumers should dedupe
on it.

### Resent events

Either way, a resent event equals the first, related records included:
the state store keeps the previous versions of changed rows until the slot
is confirmed (a related row first read from Postgres during the resend is
current). After a Postgres crash the slot can restart from before what
erpdoo confirmed; erpdoo recognises those transactions as delivered and
does not send them again.

### Something must restart erpdoo

When Postgres or a sink goes away while it runs, erpdoo logs why and exits
without confirming the undelivered events, for a supervisor (Docker's
restart policy, systemd) to start it again. At startup it waits for
Postgres and Kafka (after 1 s, then twice as long each time, up to 30 s),
so the order services come up in after a reboot does not matter.

[scripts/delivery_test.py](../scripts/delivery_test.py) checks outages and
crashes on the dev stack, in both delivery modes (see
[CONTRIBUTING.md](../CONTRIBUTING.md#the-checks)).
