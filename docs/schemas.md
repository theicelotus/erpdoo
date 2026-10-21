# Schemas

erpdoo generates a schema for each event type from its rule and Odoo's
field types, registers it in a schema registry, and writes Kafka messages
as JSON Schema or Avro. This page covers the formats, how Odoo fields
appear, schema compatibility when rules change, and how to read the events.

- [Turning it on](#turning-it-on)
- [Subjects and formats](#subjects-and-formats)
- [How Odoo fields appear](#how-odoo-fields-appear)
- [Compatibility](#compatibility)
- [In Redpanda Console](#in-redpanda-console)
- [Reading the events](#reading-the-events)

## Turning it on

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

Without `schema_registry`, messages are plain JSON.

## Subjects and formats

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

## How Odoo fields appear

In both formats:

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

- Every value may be null.
- The type comes from the Postgres column when the field type does not
  decide it: a float is a number or a decimal string depending on its
  `digits`.
- JSON Schema checks decimals, dates and datetimes with a pattern;
  datetimes have no `T` or timezone, so they are not `format: date-time`.
- A selection is a plain string rather than an enum, because modules add
  values to selections, and each install would then be a schema change.
- In `changes`, a translated field holds every language, as Odoo stores it.

In [polling mode](polling.md#limits), Odoo's API returns `0` and `false`
for empty numbers and booleans, where replication mode has `null`.

## Compatibility

erpdoo sets each subject it creates to `BACKWARD` (consumers using the
newest schema can read every earlier event). When it starts, it checks
every rule's schema against the latest version of its subject, and
registers them only if all pass; otherwise it registers none and stops with
the registry's reasons:

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

## In Redpanda Console

In the dev stack's Redpanda Console (http://localhost:8080), **Schema
Registry** lists the subjects with their versions and compatibility, and
the topics show messages decoded in both formats. The same
`SaleOrderConfirmed` is about 970 bytes as JSON and 300 as Avro.

## Reading the events

With confluent-kafka (`pip install 'confluent-kafka[avro,json]'`):

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

Consumers of erpdoo's Kafka transactions must read with
`isolation.level=read_committed`, librdkafka's default (see
[Exactly once to Kafka](sinks.md#exactly-once-to-kafka)).
