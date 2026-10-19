"""Run erpdoo: read Odoo's changes from Postgres and publish events to Kafka.

Configured with environment variables for now; YAML config arrives with the
rule engine and sinks.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time

import psycopg

from erpdoo.events import sale_order_confirmed
from erpdoo.kafka import KafkaSink
from erpdoo.replication import ReplicationReader

log = logging.getLogger("erpdoo")


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
    poll_interval = float(os.environ.get("ERPDOO_POLL_INTERVAL", "0.5"))

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sink = KafkaSink(bootstrap, topic)
    sink.ensure_topic()

    with psycopg.connect(dsn, autocommit=True, application_name="erpdoo") as conn:
        reader = ReplicationReader(conn, slot, publication)
        # REPLICA IDENTITY FULL makes Postgres send the old row on update,
        # which is how we see the state the order is leaving.
        reader.setup(tables=["sale_order"], replica_identity_full=["sale_order"])
        log.info("streaming to %s topic %s", bootstrap, topic)

        while not stopping:
            batch = reader.poll()
            events = [e for txn in batch.transactions for e in sale_order_confirmed(txn)]
            if events:
                sink.send(events)
                for event in events:
                    print(json.dumps(event.payload), flush=True)
            if batch.ack_lsn is not None:
                reader.ack(batch.ack_lsn)
            if not batch.transactions:
                time.sleep(poll_interval)

    sink.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
