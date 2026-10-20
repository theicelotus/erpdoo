"""Run erpdoo: read Odoo's changes from Postgres and publish events to Kafka.

Event rules are YAML files (ERPDOO_RULES, a file or directory). Sinks are
set in a YAML config file (ERPDOO_CONFIG, default erpdoo.yml); database and
replication settings come from environment variables.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time

import psycopg2
from confluent_kafka import KafkaException

from erpdoo.backoff import Stopped, Unreachable, reason, retry
from erpdoo.config import ConfigError, load_config
from erpdoo.events import EventBuilder
from erpdoo.models import ModelRegistry
from erpdoo.replication import PublicationError, ReplicationReader, StreamError
from erpdoo.rules import (
    Rule, RuleError, load_rules, missing_model_message, split_by_model, tracked_tables, validate,
)
from erpdoo.sinks import DeliveryError, make_sink
from erpdoo.store import StateStore

log = logging.getLogger("erpdoo")

MODEL_TABLES = ("ir_model", "ir_model_fields")
RECHECK_SECONDS = 30
CONFIRM_SECONDS = 10  # at most this long between confirmations sent to Postgres


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("ERPDOO_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run()
    except (ConfigError, RuleError, PublicationError) as e:
        # Mistakes in the setup: say what is wrong, without a traceback.
        log.error("%s", e)
        sys.exit(2)
    except Stopped:
        log.info("stopped")
    except (DeliveryError, StreamError, psycopg2.OperationalError) as e:
        # Nothing undelivered was confirmed: after a restart, which waits for
        # Postgres and Kafka to be back, the events are sent again.
        log.error("%s; stopping, unconfirmed events are sent again after a restart", reason(e))
        sys.exit(1)


def run() -> None:
    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    slot = os.environ.get("ERPDOO_SLOT", "erpdoo")
    publication = os.environ.get("ERPDOO_PUBLICATION", "erpdoo")
    rules_path = os.environ.get("ERPDOO_RULES", "rules")
    state_path = os.environ.get("ERPDOO_STATE", "erpdoo-state.db")
    lang = os.environ.get("ERPDOO_LANG", "en_US")
    config = load_config(os.environ.get("ERPDOO_CONFIG"))

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())

    rules = load_rules(rules_path)
    # After a reboot erpdoo may start before Postgres and Kafka: wait for them.
    registry = retry("Postgres", lambda: ModelRegistry.load(dsn), (psycopg2.OperationalError,), stop)
    reader = ReplicationReader(dsn, slot, publication, lambda table: registry.model_for_table(table))
    store = StateStore(state_path)

    def activate(registry: ModelRegistry) -> tuple[EventBuilder, list[Rule]]:
        """Publish and track what the rules of installed apps need. The
        rules' own tables are kept in the state store, which gives each
        update and delete the row it replaces, so a rule sees the value a
        field is leaving. Only a rule table whose rows the store cannot keep
        needs REPLICA IDENTITY FULL, so that Postgres sends that row instead.
        ir_model and ir_model_fields are streamed so the registry follows
        module installs."""
        active, skipped = split_by_model(rules, registry)
        validate(active, registry)
        tracked = tracked_tables(active, registry)
        rule_tables = {registry.models[r.model].table for r in active}
        full = sorted(rule_tables - tracked.keys())
        for table in full:
            log.info("%s has no id column, so the state store cannot keep its rows; its rules need"
                     " REPLICA IDENTITY FULL instead", table)
        slot_created = reader.setup(
            tables=sorted(rule_tables | tracked.keys() | set(MODEL_TABLES)),
            replica_identity_full=full,
        )
        if slot_created:
            # A new slot starts now: changes since the store was last updated are lost.
            store.reset()
        store.sync(dsn, tracked)
        return EventBuilder(active, registry, store, lang), skipped

    def report(skipped: list[Rule], previous: set[str], registry: ModelRegistry) -> None:
        for rule in skipped:
            if rule.name not in previous:
                log.warning("rule %s skipped: %s; it is activated when the model appears",
                            rule.name, missing_model_message(rule, registry))

    builder, skipped = activate(registry)
    report(skipped, set(), registry)
    log.info("rules: %d active (%s), %d inactive%s", len(builder.rules),
             ", ".join(r.name for r in builder.rules) or "none", len(skipped),
             f" ({', '.join(f'{r.name}: {r.model} not found' for r in skipped)})" if skipped else "")
    # Sinks start after the slot exists, so once the Kafka topic exists no
    # change is missed (the smoke test relies on this).
    sinks = [retry(f"{spec['type']} sink", lambda spec=spec: make_sink(spec),
                   (Unreachable, KafkaException, TimeoutError), stop)
             for spec in config["sinks"]]
    viewer = None
    if config.get("viewer") is not None:
        try:
            from erpdoo.viewer import Viewer, describe
        except ImportError:
            raise ConfigError("the viewer needs FastAPI and uvicorn: pip install 'erpdoo[viewer]'") from None
        viewer = Viewer(**config["viewer"])
        viewer.start()

    confirmed_at = time.monotonic()
    recheck_at: float | None = None  # when to retry activating rules that did not fit
    last_error: str | None = None
    reader.start()
    log.info("streaming to %s", ", ".join(spec["type"] for spec in config["sinks"]))
    try:
        while not stop.is_set():
            batch = reader.poll(timeout=1.0)
            events = []
            shown = []
            for txn in batch.transactions:
                # Also gives updates and deletes their previous row.
                replay = store.apply_transaction(txn)
                txn_events = builder.build(txn, replay)
                events.extend(txn_events)
                if viewer:
                    shown.append(describe(txn, txn_events))
            store.commit()
            # Every sink must deliver before the slot is confirmed. If one
            # fails, erpdoo stops; after a restart all sinks get the events again.
            if events:
                for sink in sinks:
                    sink.send(events)
            for item in shown:
                viewer.publish(item)
            if batch.ack_lsn is not None:
                # After a delivery, confirm right away so a crash does not
                # resend it. Other progress is confirmed every few seconds;
                # the store then drops the row versions kept for replays.
                now = bool(events) or time.monotonic() - confirmed_at >= CONFIRM_SECONDS
                reader.ack(batch.ack_lsn, now=now)
                if now:
                    confirmed_at = time.monotonic()
                    store.prune(batch.ack_lsn)
                    store.commit()
            models_changed = any(c.table in MODEL_TABLES for txn in batch.transactions for c in txn.changes)
            if models_changed:
                registry = ModelRegistry.load(dsn)
            elif recheck_at is None or time.monotonic() < recheck_at:
                continue
            before = {r.name for r in builder.rules}
            try:
                builder, new_skipped = activate(registry)
            except (RuleError, PublicationError) as e:
                # Keep streaming with the rules that worked; a module
                # install should not stop erpdoo. If Postgres needs an
                # admin's SQL first, check again until it has been run.
                builder.registry = registry
                recheck_at = time.monotonic() + RECHECK_SECONDS if isinstance(e, PublicationError) else None
                if str(e) != last_error:
                    log.error("Odoo models changed and the rules no longer fit, keeping the active ones%s: %s",
                              f" (checking again every {RECHECK_SECONDS} s)" if recheck_at else "", e)
                last_error = str(e)
                continue
            recheck_at = last_error = None
            report(new_skipped, {r.name for r in skipped}, registry)
            skipped = new_skipped
            for name in sorted({r.name for r in builder.rules} - before):
                log.info("rule %s activated", name)
            if models_changed:
                log.info("Odoo models changed, reloaded %d models", len(registry.models))
    finally:
        reader.close()
        for sink in sinks:
            sink.close()
        if viewer:
            viewer.stop()
    log.info("stopped")


if __name__ == "__main__":
    main()
