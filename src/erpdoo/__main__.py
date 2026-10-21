"""Run erpdoo: read Odoo's changes from Postgres and publish events to Kafka.

Event rules are YAML files (ERPDOO_RULES, a file or directory). Sinks are
set in a YAML config file (ERPDOO_CONFIG, default erpdoo.yml); database and
replication settings come from environment variables.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import KafkaException

from erpdoo import backfill
from erpdoo.backfill import BackfillError
from erpdoo.backoff import Stopped, Unreachable, reason, retry
from erpdoo.config import ConfigError, load_config
from erpdoo.events import Event, EventBuilder
from erpdoo.inbound import load_inbound_rules, validate_inbound
from erpdoo.inbound_consumer import Inbound, InboundFailed
from erpdoo.models import ModelRegistry
from erpdoo.odoo_rpc import OdooClient, OdooError, OdooSetupError, OdooUnavailable
from erpdoo.replication import PublicationError, ReplicationReader, StreamError
from erpdoo.rules import (
    Rule, RuleError, load_rules, missing_model_message, split_by_model, tracked_tables, validate,
)
from erpdoo.schemas import SchemaError
from erpdoo.sinks import DeliveryError, make_sink, prepare
from erpdoo.store import StateStore

log = logging.getLogger("erpdoo")

MODEL_TABLES = ("ir_model", "ir_model_fields")
RECHECK_SECONDS = 30
CONFIRM_SECONDS = 10  # at most this long between confirmations sent to Postgres
MAX_POLL_DELAY = 60  # seconds between polls at most while Odoo does not answer
MODELS_CHECK_SECONDS = 60  # polling: how often to check whether Odoo's models changed (two API calls)
REPORT_SECONDS = 60  # polling: how often the API calls are summed up in the log


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("ERPDOO_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        if sys.argv[1:2] == ["backfill"]:
            backfill_command(sys.argv[2:])
        else:
            run()
    except (ConfigError, RuleError, PublicationError, BackfillError, SchemaError) as e:
        # Mistakes in the setup: say what is wrong, without a traceback.
        log.error("%s", e)
        sys.exit(2)
    except Stopped:
        log.info("stopped")
    except (DeliveryError, StreamError, InboundFailed, psycopg2.OperationalError, OdooError) as e:
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
    if config["source"]["type"] == "polling":
        return run_polling(config, rules_path, state_path, lang, stop)

    rules = load_rules(rules_path)
    # After a reboot erpdoo may start before Postgres and Kafka: wait for them.
    registry = retry("Postgres", lambda: ModelRegistry.load(dsn, config["tables"]), (psycopg2.OperationalError,), stop)
    report_tables(registry, None)
    reader = ReplicationReader(dsn, slot, publication, lambda table: registry.model_for_table(table))
    store = StateStore(state_path)
    store.use_source("replication")
    if store.streaming_since is None:  # a store from before this was kept
        store.set_streaming_since(datetime.now(timezone.utc))

    def activate(registry: ModelRegistry, check=None) -> tuple[EventBuilder, list[Rule]]:
        """Publish and track what the rules of installed apps need. The
        rules' own tables are kept in the state store, which gives each
        update and delete the row it replaces, so a rule sees the value a
        field is leaving. Only a rule table whose rows the store cannot keep
        needs REPLICA IDENTITY FULL, so that Postgres sends that row instead.
        ir_model and ir_model_fields are streamed so the registry follows
        module installs. ``check(active)`` runs before anything is copied.

        A rule table copied for a model created since erpdoo started
        streaming (its app was installed later) is backfilled from the
        model's creation, in the snapshot of the copy (see catch_up)."""
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
            lost = None
            if reader.slot_was_lost:
                reader.slot_was_lost = False
                lost = (store.applied_time, int(time.time() * 1_000_000))
                since = (datetime.fromtimestamp(store.applied_time / 1e6, timezone.utc).isoformat()
                         if store.applied_time else "the last transaction erpdoo processed (time unknown)")
                after = (" They are backfilled from Odoo's records next." if config["backfill"]["after_lost_slot"]
                         and store.applied_time else "")
                log.error(
                    "replication slot %s had been invalidated by Postgres, which removed WAL erpdoo had not"
                    " read yet (it held more than max_slot_wal_keep_size); created it again. Changes committed"
                    " between %s and %s were not streamed, so events from that time may be missing.%s"
                    " The state store is copied again.", slot, since,
                    datetime.fromtimestamp(lost[1] / 1e6, timezone.utc).isoformat(), after)
            # A new slot starts now: changes since the store was last updated are lost.
            store.reset()
            store.set_streaming_since(datetime.now(timezone.utc))
            if lost and lost[0] and config["backfill"]["after_lost_slot"]:
                store.set_pending_backfill(*lost)
        if check is not None:
            check(active)
        store.sync(dsn, tracked, lambda conn, tables, snapshot: catch_up(conn, active, registry, tables, snapshot))
        return EventBuilder(active, registry, store, lang), skipped

    def catch_up(conn, active: list[Rule], registry: ModelRegistry, tables: list[str], snapshot) -> list[Event]:
        """The backfilled events of rules that activate late, found in the
        snapshot their tables were just copied in."""
        until = snapshot.time.isoformat()
        try:
            result, late = backfill.catch_up(conn, active, registry, tables, store.streaming_since, lang)
        except BackfillError as e:
            names = ", ".join(r.name for r in active if registry.models[r.model].table in tables)
            log.error("could not backfill the changes from before erpdoo copied the tables of %s at %s: %s\nThen"
                      " run: erpdoo backfill --since <when the app was installed> --until %s (a change streamed"
                      " since may then be sent twice)", names, until, e, until)
            return []
        for reason_ in result.skipped:
            log.warning("backfill: %s", reason_)
        for model, since in sorted(late.items()):
            mine = [e for e in result.events if e.model == model]
            log.info("%s was created at %s (its app was installed since erpdoo started streaming); backfilled"
                     " %s, up to %s when erpdoo copied its table: %d events", model, since.isoformat(),
                     ", ".join(r.name for r in active if r.model == model), until, len(mine))
        return result.events

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
    # Each event type's schema, for sinks with a schema registry. A rule
    # change that would break compatibility stops erpdoo here.
    retry("schema registry", lambda: prepare(sinks, builder.rules, registry), (Unreachable,), stop)
    send_pending(store, sinks, builder.rules)
    pending = store.pending_backfill()
    if pending:
        since, until = (datetime.fromtimestamp(t / 1e6, timezone.utc) for t in pending)
        try:
            result = backfill.run(builder.rules, registry, dsn, since, until, lang)
        except BackfillError as e:
            log.error("could not backfill %s to %s: %s\nThen run: erpdoo backfill --since %s --until %s",
                      since.isoformat(), until.isoformat(), e, since.isoformat(), until.isoformat())
        else:
            send_backfill(result, sinks)
            log.info("backfilled %s to %s: %s", since.isoformat(), until.isoformat(), result.summary())
        store.clear_pending_backfill()
    viewer = None
    if config.get("viewer") is not None:
        try:
            from erpdoo.viewer import Viewer, describe
        except ImportError:
            raise ConfigError("the viewer needs FastAPI and uvicorn: pip install 'erpdoo[viewer]'") from None
        viewer = Viewer(**config["viewer"])
        viewer.start()

    inbound = start_inbound(config, rules_path, registry, state_path, dsn, stop)

    confirmed_at = time.monotonic()
    reported_at = 0.0  # last slot health report
    # For tests only (scripts/replay_test.py): while this file exists, events
    # are delivered but the slot is not confirmed; once it is removed, erpdoo
    # exits at once, as if it had crashed.
    hold_ack = os.environ.get("ERPDOO_TEST_HOLD_ACK")
    holding = False
    recheck_at: float | None = None  # when to retry activating rules that did not fit
    last_error: str | None = None
    reader.start()
    log.info("streaming to %s", ", ".join(spec["type"] for spec in config["sinks"]))
    try:
        while not stop.is_set():
            if inbound is not None and inbound.error is not None:
                raise InboundFailed(f"inbound: {reason(inbound.error)}")
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
            if hold_ack and os.path.exists(hold_ack):
                holding = True  # test hook: delivered, not confirmed
            elif holding:
                # Test hook: crash between delivery and confirmation.
                log.warning("%s removed: exiting without confirming delivered events (test hook)", hold_ack)
                os._exit(70)
            elif batch.ack_lsn is not None:
                # After a delivery, confirm right away so a crash does not
                # resend it. Other progress is confirmed every few seconds;
                # the store then drops the row versions kept for replays.
                now = bool(events) or time.monotonic() - confirmed_at >= CONFIRM_SECONDS
                reader.ack(batch.ack_lsn, now=now)
                if now:
                    confirmed_at = time.monotonic()
                    store.prune(batch.ack_lsn)
                    store.commit()
            if time.monotonic() - reported_at >= config["slot"]["report_seconds"]:
                reported_at = time.monotonic()
                report_health(reader, store, config["slot"], viewer)
            models_changed = any(c.table in MODEL_TABLES for txn in batch.transactions for c in txn.changes)
            if models_changed:
                registry, previous = ModelRegistry.load(dsn, config["tables"]), registry
                report_tables(registry, previous)
            elif recheck_at is None or time.monotonic() < recheck_at:
                continue
            before = {r.name for r in builder.rules}
            try:
                # Schemas first: a rule the registry refuses copies nothing.
                new_builder, new_skipped = activate(registry, lambda active: prepare(sinks, active, registry))
            except (RuleError, PublicationError, SchemaError, Unreachable) as e:
                # Keep streaming with the rules that worked; a module
                # install should not stop erpdoo. If Postgres needs an
                # admin's SQL first, or the schema registry does not
                # answer, check again until it does.
                builder.registry = registry
                recheck_at = (time.monotonic() + RECHECK_SECONDS if isinstance(e, (PublicationError, Unreachable))
                              else None)
                if str(e) != last_error:
                    log.error("Odoo models changed and the rules no longer fit, keeping the active ones%s: %s",
                              f" (checking again every {RECHECK_SECONDS} s)" if recheck_at else "", e)
                last_error = str(e)
                continue
            builder, recheck_at, last_error = new_builder, None, None
            # Before anything streamed after the copy.
            send_pending(store, sinks, builder.rules)
            report(new_skipped, {r.name for r in skipped}, registry)
            skipped = new_skipped
            for name in sorted({r.name for r in builder.rules} - before):
                log.info("rule %s activated", name)
            if models_changed:
                log.info("Odoo models changed, reloaded %d models", len(registry.models))
    finally:
        if inbound is not None:
            stop.set()
            inbound.join(timeout=30)
        reader.close()
        for sink in sinks:
            sink.close()
        if viewer:
            viewer.stop()
    log.info("stopped")


def run_polling(config: dict, rules_path: str, state_path: str, lang: str, stop: threading.Event) -> None:
    """Polling mode (see erpdoo.polling): no Postgres, only Odoo's API."""
    from erpdoo.polling import Poller

    source = config["source"]
    odoo = source["odoo"]
    client = OdooClient(odoo["url"], odoo["db"], odoo["login"], odoo["api_key"])
    store = StateStore(state_path)
    store.use_source("polling")
    poller = Poller(client, source, store, lang, config["tables"])
    rules = load_rules(rules_path)

    def activate(previous: set[str]) -> list[Rule]:
        skipped = retry("Odoo", lambda: poller.activate(rules), (OdooUnavailable,), stop)
        for rule in skipped:
            if rule.name not in previous:
                log.warning("rule %s skipped: %s; it is activated when the model appears", rule.name,
                            missing_model_message(rule, poller.registry))
        return skipped

    try:
        # After a reboot erpdoo may start before Odoo: wait for it.
        retry("Odoo", poller.load, (OdooUnavailable,), stop)
        skipped = activate(set())
        retry("Odoo", poller.check_read_only, (OdooUnavailable,), stop)
    except OdooSetupError as e:
        raise ConfigError(f"polling: {e}") from None
    log.info("rules: %d active (%s), %d inactive; transitions from Odoo's field tracking: %s",
             len(poller.rules), ", ".join(r.name for r in poller.rules) or "none", len(skipped),
             ", ".join(sorted(n for p in poller.plans.values() for n in p.by_tracking)) or "none")
    sinks = [retry(f"{spec['type']} sink", lambda spec=spec: make_sink(spec),
                   (Unreachable, KafkaException, TimeoutError), stop)
             for spec in config["sinks"]]
    retry("schema registry", lambda: prepare(sinks, poller.rules, poller.registry), (Unreachable,), stop)
    viewer = None
    if config.get("viewer") is not None:
        try:
            from erpdoo.viewer import Viewer, describe_poll
        except ImportError:
            raise ConfigError("the viewer needs FastAPI and uvicorn: pip install 'erpdoo[viewer]'") from None
        viewer = Viewer(**config["viewer"])
        viewer.start()
    # No Postgres: inbound attempts are timed on erpdoo's clock (see erpdoo.polling).
    inbound = start_inbound(config, rules_path, poller.registry, state_path, None, stop)
    interval = source["interval"]
    log.info("polling %s every %g s as %s, to %s", odoo["url"], interval, odoo["login"],
             ", ".join(spec["type"] for spec in config["sinks"]))
    delay = interval  # after a failed poll; doubles up to MAX_POLL_DELAY
    load = PollLoad(client.calls)
    checked_at = time.monotonic()  # when Odoo's models were last checked for changes
    try:
        while not stop.is_set():
            if inbound is not None and inbound.error is not None:
                raise InboundFailed(f"inbound: {reason(inbound.error)}")
            started = time.monotonic()
            try:
                if started - checked_at >= MODELS_CHECK_SECONDS:
                    checked_at = started
                    if poller.models_changed():
                        before = {r.name for r in poller.rules}
                        poller.load()
                        skipped = activate({r.name for r in skipped})
                        prepare(sinks, poller.rules, poller.registry)
                        for name in sorted({r.name for r in poller.rules} - before):
                            log.info("rule %s activated", name)
                        log.info("Odoo models changed, reloaded %d models", len(poller.registry.models))
                result = poller.poll()
            except (OdooUnavailable, OdooSetupError, Unreachable) as e:
                (log.error if isinstance(e, OdooSetupError) else log.warning)(
                    "polling: %s; trying again in %g s", e, delay)
                load.failed(str(e))
                stop.wait(delay)
                delay = min(delay * 2, MAX_POLL_DELAY)
                continue
            delay = interval
            # Every sink must deliver before the poll is noted. If one fails,
            # erpdoo stops; after a restart the same changes are read again.
            if result.events:
                for sink in sinks:
                    sink.send(result.events)
            poller.commit(result)
            load.add(result)
            (log.info if result.changes or result.events else log.debug)("poll %d: %s", result.number,
                                                                          result.summary())
            if viewer and (result.changes or result.events):
                viewer.publish(describe_poll(result.number, datetime.now(timezone.utc), result.changes,
                                             result.events, result.calls))
            if load.due():
                log.info("%s", load.report(client.calls, interval))
            if viewer:
                viewer.slot = load.status(interval)
            stop.wait(max(0.0, interval - (time.monotonic() - started)))
    finally:
        if inbound is not None:
            stop.set()
            inbound.join(timeout=30)
        for sink in sinks:
            sink.close()
        if viewer:
            viewer.stop()


class PollLoad:
    """What polling costs Odoo, for the log (every REPORT_SECONDS) and the viewer."""

    def __init__(self, calls: int) -> None:
        self.since, self.calls = time.monotonic(), calls
        self.polls = self.read = self.events = 0
        self.last: float | None = None
        self.last_calls = 0
        self.error: str | None = None
        self.per_minute = 0.0

    def add(self, result) -> None:
        self.polls += 1
        self.read += result.read
        self.events += len(result.events)
        self.last, self.last_calls, self.error = time.monotonic(), result.calls, None

    def failed(self, error: str) -> None:
        self.error = error

    def due(self) -> bool:
        return time.monotonic() - self.since >= REPORT_SECONDS

    def report(self, calls: int, interval: float) -> str:
        seconds = time.monotonic() - self.since
        used = calls - self.calls
        self.per_minute = used * 60 / seconds
        text = (f"polling every {interval:g} s: {self.polls} polls in the last {seconds:.0f} s, {used} API calls"
                f" ({used / max(self.polls, 1):.1f} per poll, {self.per_minute:.0f} per minute),"
                f" {self.read} records read, {self.events} events")
        self.since, self.calls = time.monotonic(), calls
        self.polls = self.read = self.events = 0
        return text

    def status(self, interval: float) -> dict:
        ago = f"{time.monotonic() - self.last:.0f} s ago" if self.last is not None else "not yet"
        rate = f", {self.per_minute:.0f} API calls per minute" if self.per_minute else ""
        return {"text": f"polling every {interval:g} s: last poll {ago}, {self.last_calls} API calls{rate}",
                "warnings": [self.error] if self.error else [],
                "title": "erpdoo polls Odoo's API; each poll costs the API calls shown"}


def start_inbound(config: dict, rules_path: str, registry: ModelRegistry, state_path: str, dsn: str | None,
                  stop: threading.Event) -> Inbound | None:
    """Start consuming inbound messages, if erpdoo.yml has inbound settings
    and there are inbound rules (ERPDOO_INBOUND_RULES, by default the
    inbound directory next to the rules)."""
    path = os.environ.get("ERPDOO_INBOUND_RULES") or str(Path(rules_path) / "inbound")
    rules = load_inbound_rules(path)
    if config.get("inbound") is None:
        if rules:
            log.info("%d inbound rules in %s are not used: erpdoo.yml has no inbound settings", len(rules), path)
        return None
    active, skipped = validate_inbound(rules, registry)
    for message in skipped:
        log.warning("%s", message)
    if not active:
        log.warning("inbound: no inbound rules to use in %s", path)
        return None
    inbound = Inbound(config["inbound"], active, state_path, dsn, stop)
    inbound.start()
    return inbound


def send_pending(store: StateStore, sinks: list, rules: list[Rule]) -> None:
    """Deliver the backfilled events the state store keeps (see catch_up),
    then forget them. Those of rules not active now wait."""
    names = {r.name for r in rules}
    ready = [(seq, Event(**fields)) for seq, fields in store.pending_events() if fields["event_type"] in names]
    if not ready:
        return
    for start in range(0, len(ready), 500):
        for sink in sinks:
            sink.send([event for _, event in ready[start:start + 500]])
    store.clear_pending_events(seq for seq, _ in ready)
    log.info("sent %d backfilled events", len(ready))


def send_backfill(result: backfill.Backfill, sinks: list) -> None:
    for reason_ in result.skipped:
        log.warning("backfill: %s", reason_)
    for start in range(0, len(result.events), 500):
        for sink in sinks:
            sink.send(result.events[start:start + 500])


def backfill_command(argv: list[str]) -> None:
    """erpdoo backfill --since T --until T: send the events the rules would
    have produced in that window (see erpdoo.backfill)."""
    p = argparse.ArgumentParser(prog="erpdoo backfill", description=backfill.__doc__.split("\n\n")[0])
    p.add_argument("--since", required=True, type=backfill.parse_time, help="start, ISO 8601 (UTC if no zone)")
    p.add_argument("--until", required=True, type=backfill.parse_time, help="end, ISO 8601 (UTC if no zone)")
    p.add_argument("--dry-run", action="store_true", help="print the events instead of sending them")
    args = p.parse_args(argv)
    dsn = os.environ.get("ERPDOO_PG_DSN", "host=localhost dbname=odoo user=erpdoo password=erpdoo")
    config = load_config(os.environ.get("ERPDOO_CONFIG"))
    if config["source"]["type"] == "polling":
        raise ConfigError("erpdoo backfill reads Postgres, which polling mode does not use. In polling mode erpdoo"
                          " catches up by itself: after a stop it reads everything written since its cursor")
    registry = ModelRegistry.load(dsn, config["tables"])
    active, skipped = split_by_model(load_rules(os.environ.get("ERPDOO_RULES", "rules")), registry)
    validate(active, registry)
    for rule in skipped:
        log.warning("rule %s skipped: %s", rule.name, missing_model_message(rule, registry))
    result = backfill.run(active, registry, dsn, args.since, args.until, os.environ.get("ERPDOO_LANG", "en_US"))
    if args.dry_run:
        for event in result.events:
            print(json.dumps(event.payload))
    else:
        sinks = [make_sink(spec) for spec in config["sinks"]]
        try:
            prepare(sinks, active, registry)
            send_backfill(result, sinks)
        finally:
            for sink in sinks:
                sink.close()
    log.info("backfill %s to %s: %s%s", args.since.isoformat(), args.until.isoformat(), result.summary(),
             "" if args.dry_run else f", sent to {', '.join(spec['type'] for spec in config['sinks'])}")


def report_tables(registry: ModelRegistry, previous: ModelRegistry | None) -> None:
    """Log tables found by their columns, and tables not found, once each."""
    for model, table in sorted(registry.detected.items()):
        if previous is None or previous.detected.get(model) != table:
            log.info("model %s uses table %s, found by its columns (Odoo's naming rule gives %s); set it under"
                     " tables: in erpdoo.yml to be sure", model, table, model.replace(".", "_"))
    for problem in registry.problems:
        if previous is None or problem not in previous.problems:
            log.warning("table of model %s", problem)


def report_health(reader: ReplicationReader, store: StateStore, settings: dict, viewer) -> None:
    """Log how far the slot trails Postgres' WAL, as a warning past the
    configured thresholds, and show it in the viewer."""
    health = reader.health()
    if health is None:
        return
    problems = health.warnings(settings)
    text = f"{health.describe()}; {store.fetches} rows fetched on demand so far"
    if problems:
        log.warning("%s: %s", text, "; ".join(problems))
    else:
        log.info("%s", text)
    if viewer:
        viewer.slot = health.as_dict(settings)


if __name__ == "__main__":
    main()
