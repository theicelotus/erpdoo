"""Run erpdoo: read Odoo's changes from Postgres and publish events to Kafka.

Configured with environment variables for now; YAML config arrives with the
rule engine and sinks.
"""

from __future__ import annotations

import json
import logging
import os
import signal

from erpdoo.events import sale_order_confirmed
from erpdoo.kafka import KafkaSink
from erpdoo.models import ModelRegistry
from erpdoo.replication import ReplicationReader

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

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sink = KafkaSink(bootstrap, topic)
    sink.ensure_topic()

    registry = ModelRegistry.load(dsn)
    log.info("loaded %d Odoo models", len(registry.models))
    reader = ReplicationReader(dsn, slot, publication, lambda table: registry.model_for_table(table))
    # REPLICA IDENTITY FULL makes Postgres send the old row on update,
    # which is how we see the state the order is leaving. ir_model and
    # ir_model_fields are streamed so the registry follows module installs.
    reader.setup(tables=["sale_order", *MODEL_TABLES], replica_identity_full=["sale_order"])
    reader.start()
    log.info("streaming to %s topic %s", bootstrap, topic)
    try:
        while not stopping:
            batch = reader.poll(timeout=1.0)
            events = [e for txn in batch.transactions for e in sale_order_confirmed(txn)]
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
                log.info("Odoo models changed, reloaded %d models", len(registry.models))
    finally:
        reader.close()
        sink.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
