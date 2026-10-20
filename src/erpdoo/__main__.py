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

from erpdoo.config import ConfigError, load_config
from erpdoo.events import EventBuilder
from erpdoo.models import ModelRegistry
from erpdoo.replication import PublicationError, ReplicationReader
from erpdoo.rules import Rule, RuleError, load_rules, split_by_model, tracked_tables, validate
from erpdoo.sinks import make_sink
from erpdoo.store import StateStore

log = logging.getLogger("erpdoo")

MODEL_TABLES = ("ir_model", "ir_model_fields")


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


def run() -> None:
    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    slot = os.environ.get("ERPDOO_SLOT", "erpdoo")
    publication = os.environ.get("ERPDOO_PUBLICATION", "erpdoo")
    rules_path = os.environ.get("ERPDOO_RULES", "rules")
    state_path = os.environ.get("ERPDOO_STATE", "erpdoo-state.db")
    lang = os.environ.get("ERPDOO_LANG", "en_US")
    config = load_config(os.environ.get("ERPDOO_CONFIG"))

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    rules = load_rules(rules_path)
    registry = ModelRegistry.load(dsn)
    reader = ReplicationReader(dsn, slot, publication, lambda table: registry.model_for_table(table))
    store = StateStore(state_path)

    def activate(registry: ModelRegistry) -> tuple[EventBuilder, list[Rule]]:
        """Publish and track what the rules of installed apps need. Rule
        models get REPLICA IDENTITY FULL so Postgres sends the row before each
        update, which is how a rule sees the value a field is leaving.
        ir_model and ir_model_fields are streamed so the registry follows
        module installs."""
        active, skipped = split_by_model(rules, registry)
        validate(active, registry)
        tracked = tracked_tables(active, registry)
        rule_tables = sorted({registry.models[r.model].table for r in active})
        slot_created = reader.setup(
            tables=sorted(set(rule_tables) | tracked.keys() | set(MODEL_TABLES)),
            replica_identity_full=rule_tables,
        )
        if slot_created:
            # A new slot starts now: changes since the store was last updated are lost.
            store.reset()
        store.sync(dsn, tracked)
        return EventBuilder(active, registry, store, lang), skipped

    def report(skipped: list[Rule], previous: set[str]) -> None:
        for rule in skipped:
            if rule.name not in previous:
                log.warning("rule %s skipped: model %s not found (is its Odoo app installed?);"
                            " it is activated when the model appears", rule.name, rule.model)

    builder, skipped = activate(registry)
    report(skipped, set())
    log.info("active rules: %s", ", ".join(r.name for r in builder.rules) or "none")
    # Sinks start after the slot exists, so once the Kafka topic exists no
    # change is missed (the smoke test relies on this).
    sinks = [make_sink(spec) for spec in config["sinks"]]

    reader.start()
    log.info("streaming to %s", ", ".join(spec["type"] for spec in config["sinks"]))
    try:
        while not stopping:
            batch = reader.poll(timeout=1.0)
            events = []
            for txn in batch.transactions:
                for change in txn.changes:
                    store.apply(change)
                events.extend(builder.build(txn))
            store.commit()
            # Every sink must deliver before the slot is confirmed. If one
            # fails, erpdoo stops; after a restart all sinks get the events again.
            if events:
                for sink in sinks:
                    sink.send(events)
            if batch.ack_lsn is not None:
                # After a delivery, confirm right away so a crash does not
                # resend it. Idle progress can wait for the status interval.
                reader.ack(batch.ack_lsn, now=bool(events))
            if any(c.table in MODEL_TABLES for txn in batch.transactions for c in txn.changes):
                registry = ModelRegistry.load(dsn)
                before = {r.name for r in builder.rules}
                try:
                    builder, new_skipped = activate(registry)
                except (RuleError, PublicationError) as e:
                    # Keep streaming with the rules that worked; a module
                    # install should not stop erpdoo.
                    builder.registry = registry
                    log.error("Odoo models changed and the rules no longer fit, keeping the active ones: %s", e)
                    continue
                report(new_skipped, {r.name for r in skipped})
                skipped = new_skipped
                for name in sorted({r.name for r in builder.rules} - before):
                    log.info("rule %s activated", name)
                log.info("Odoo models changed, reloaded %d models", len(registry.models))
    finally:
        reader.close()
        for sink in sinks:
            sink.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
