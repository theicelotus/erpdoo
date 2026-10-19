"""Run erpdoo: read Odoo's changes from Postgres and publish events to Kafka.

Event rules are YAML files (ERPDOO_RULES, a file or directory); connection
settings come from environment variables for now.
"""

from __future__ import annotations

import json
import logging
import os
import signal

from erpdoo.events import EventBuilder
from erpdoo.kafka import KafkaSink
from erpdoo.models import ModelRegistry
from erpdoo.replication import ReplicationReader
from erpdoo.rules import load_rules, tracked_tables, validate
from erpdoo.store import StateStore

log = logging.getLogger("erpdoo")

MODEL_TABLES = ("ir_model", "ir_model_fields")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("ERPDOO_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    bootstrap = os.environ.get("ERPDOO_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    topic = os.environ.get("ERPDOO_TOPIC", "erpdoo.events")
    slot = os.environ.get("ERPDOO_SLOT", "erpdoo")
    publication = os.environ.get("ERPDOO_PUBLICATION", "erpdoo")
    rules_path = os.environ.get("ERPDOO_RULES", "rules")
    state_path = os.environ.get("ERPDOO_STATE", "erpdoo-state.db")
    lang = os.environ.get("ERPDOO_LANG", "en_US")

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    rules = load_rules(rules_path)
    registry = ModelRegistry.load(dsn)
    validate(rules, registry)
    tracked = tracked_tables(rules, registry)
    log.info("loaded %d rules (%s) against %d Odoo models",
             len(rules), ", ".join(r.name for r in rules), len(registry.models))

    sink = KafkaSink(bootstrap, topic)
    sink.ensure_topic()

    # Rule models get REPLICA IDENTITY FULL so Postgres sends the row before
    # each update, which is how a rule sees the value a field is leaving.
    # ir_model and ir_model_fields are streamed so the registry follows
    # module installs.
    rule_tables = sorted({registry.models[r.model].table for r in rules})
    reader = ReplicationReader(dsn, slot, publication, lambda table: registry.model_for_table(table))
    slot_created = reader.setup(
        tables=sorted(set(rule_tables) | tracked.keys() | set(MODEL_TABLES)),
        replica_identity_full=rule_tables,
    )
    store = StateStore(state_path)
    if slot_created:
        # A new slot starts now: changes since the store was last updated are lost.
        store.reset()
    store.sync(dsn, tracked)
    builder = EventBuilder(rules, registry, store, lang)

    reader.start()
    log.info("streaming to %s topic %s", bootstrap, topic)
    try:
        while not stopping:
            batch = reader.poll(timeout=1.0)
            events = []
            for txn in batch.transactions:
                for change in txn.changes:
                    store.apply(change)
                events.extend(builder.build(txn))
            store.commit()
            if events:
                sink.send(events)
                for event in events:
                    print(json.dumps(event.payload), flush=True)
            if batch.ack_lsn is not None:
                # After a delivery, confirm right away so a crash does not
                # resend it. Idle progress can wait for the status interval.
                reader.ack(batch.ack_lsn, now=bool(events))
            if any(c.table in MODEL_TABLES for txn in batch.transactions for c in txn.changes):
                registry = ModelRegistry.load(dsn)
                validate(rules, registry)
                builder.registry = registry
                log.info("Odoo models changed, reloaded %d models", len(registry.models))
    finally:
        reader.close()
        sink.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
