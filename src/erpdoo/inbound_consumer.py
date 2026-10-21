"""The inbound side: consume messages from Kafka and carry them out in Odoo
with the inbound rules (erpdoo.inbound), one at a time, exactly once.

For each message:

1. A message whose id is noted as processed in the state store is a
   repeat: it is skipped. A message no rule matches is ignored.
2. The attempt is noted with the WAL position before it, then the rule
   acts through Odoo's API, as the integration user.
3. Odoo succeeded: the attempt's end position and the message id are
   noted, and only then is the Kafka offset committed. erpdoo stopping
   in between means the message comes again: it is then a repeat, or,
   in the gap between Odoo's commit and the note, the rule's already_done
   recognises it.
4. A permanent failure (no such record, Odoo refused) sends the message
   to the dead-letter topic with the reason, and commits the offset.
   Unless ``delivery: {transactions: false}``, the dead-letter record and
   the offset are committed in one Kafka transaction, so a crash between
   the two cannot dead-letter a message twice. (The events a message
   causes come from Odoo's changes, through the outbound side, and are not
   part of it.)
   A passing failure (Odoo or Postgres down, a conflict) is retried with
   exponential backoff, holding back the messages after it; so is a setup
   failure (the API key or an access right), logged as an error.

The attempt positions also tell the outbound side which message caused a
transaction (see EventBuilder): events get "source": {"inbound": ...}. In
polling mode, without Postgres, the positions are times on erpdoo's clock,
and the record each message acted on is noted too (see erpdoo.polling).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from erpdoo.backoff import Unreachable, reason, retry
from erpdoo.inbound import InboundRule, MessageError, apply, lookup
from erpdoo.odoo_rpc import OdooClient, OdooError, OdooSetupError, OdooUnavailable
from erpdoo.sinks import DeliveryError
from erpdoo.sinks.transactions import Rejected, Transactions
from erpdoo.store import InboundLog

log = logging.getLogger(__name__)

DEFAULTS = {"group_id": "erpdoo-inbound", "dead_letter_topic": "erpdoo.inbound.dead-letter", "backoff": 1.0,
            "max_backoff": 60.0, "keep_days": 30.0}
PRUNE_SECONDS = 3600

# librdkafka logs every failed connection while Kafka is away; erpdoo logs its own lines.
_kafka_log = logging.getLogger(f"{__name__}.kafka")
_kafka_log.addHandler(logging.NullHandler())
_kafka_log.propagate = False


class InboundFailed(Exception):
    """The inbound side stopped on an unexpected error; erpdoo stops too."""


@dataclass
class DeadLetter:
    """A message to dead-letter: its record, and what to note once written."""

    record: tuple[bytes, bytes, list[tuple[str, bytes]]]
    topic: str
    message_id: str | None
    rule: str | None  # None if no rule matched it (not JSON, or no id)
    reason: str
    where: str  # topic, partition and offset, for the log


class Temporary(Exception):
    """Try the message again later."""

    def __init__(self, reason: str, setup: bool = False) -> None:
        super().__init__(reason)
        self.setup = setup


class Inbound(threading.Thread):
    def __init__(self, settings: dict, rules: list[InboundRule], state_path: str, dsn: str | None,
                 stop: threading.Event, transactional_id: str | None = None) -> None:
        super().__init__(name="erpdoo-inbound", daemon=True)
        self.settings = {**DEFAULTS, **settings}
        self.rules = rules
        self.state_path = state_path
        self.dsn = dsn
        self.stop = stop
        self.error: BaseException | None = None  # set if the thread failed
        odoo = self.settings["odoo"]
        self.odoo = OdooClient(odoo["url"], odoo["db"], odoo["login"], odoo["api_key"])
        self.topics = sorted({r.topic for r in rules})
        self.transactional_id = transactional_id
        self.transactions = None
        self._pg = None
        self._ignored: set[tuple[str, str]] = set()

    def run(self) -> None:
        try:
            self._run()
        except Exception as e:  # noqa: BLE001 - reported to the main thread, which stops erpdoo
            log.exception("inbound: stopped by an unexpected error")
            self.error = e

    def _run(self) -> None:
        from erpdoo.sinks.kafka import KafkaSink

        self.log = InboundLog(self.state_path)
        bootstrap = self.settings["bootstrap_servers"]
        retried = (Unreachable, KafkaException, TimeoutError)
        if self.transactional_id:
            self.transactions = retry("Kafka (inbound)", lambda: Transactions(bootstrap, self.transactional_id),
                                      retried, self.stop)
        self.dead_letter = retry("Kafka (inbound)", lambda: KafkaSink(bootstrap, self.settings["dead_letter_topic"],
                                                                      transactions=self.transactions),
                                 retried, self.stop)
        for topic in self.topics:
            self.dead_letter.ensure_topic(topic=topic)  # so that subscribing need not wait for the first message
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap, "group.id": self.settings["group_id"], "enable.auto.commit": False,
            "auto.offset.reset": "earliest", "logger": _kafka_log,
        })
        self.consumer.subscribe(self.topics)
        log.info("inbound: %d rules (%s) on %s, acting in Odoo %s as %s", len(self.rules),
                 ", ".join(r.name for r in self.rules), ", ".join(self.topics), self.odoo.url, self.odoo.login)
        pruned_at = 0.0
        try:
            while not self.stop.is_set():
                if time.monotonic() - pruned_at > PRUNE_SECONDS:
                    pruned_at = time.monotonic()
                    self.log.prune(self.settings["keep_days"] * 86400)
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.debug("inbound: %s", msg.error().str())
                    continue
                self._handle(msg)
        finally:
            self.consumer.close()
            self.dead_letter.close()
            if self.transactions is not None:
                self.transactions.close()
            self.log.close()

    def _handle(self, msg) -> None:
        """Carry out one message, retrying passing failures until it is done
        or to be dead-lettered, then write its dead-letter record, if any,
        and commit its offset, retrying until that is done too."""
        for step in (lambda: self._process(msg), lambda: self._commit(msg, dead)):
            delay = self.settings["backoff"]
            while not self.stop.is_set():
                try:
                    dead = step()
                    break
                except Temporary as e:
                    (log.error if e.setup else log.warning)(
                        "inbound: message at %s[%d] offset %d: %s; trying again in %g s",
                        msg.topic(), msg.partition(), msg.offset(), e, delay)
                    self._wait(delay)
                    delay = min(delay * 2, self.settings["max_backoff"])
            else:
                return  # stopping: the offset is not committed, the message comes again

    def _commit(self, msg, dead: DeadLetter | None) -> None:
        """Write the message's dead-letter record, if any, and commit its
        offset: in one Kafka transaction, or one after the other. Raises
        Temporary if the dead-letter record could not be written."""
        if self.transactions is None:
            if dead is not None:
                self._write_dead_letter(dead)
            try:
                self.consumer.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                # The message comes again (a rebalance, Kafka away) and is then a repeat.
                log.warning("inbound: could not commit the offset of %s[%d] %d: %s", msg.topic(), msg.partition(),
                            msg.offset(), e)
            return
        t = self.transactions
        try:
            t.begin()
            if dead is not None:
                self._write_dead_letter(dead, note=False)
            t.producer.send_offsets_to_transaction(
                [TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                self.consumer.consumer_group_metadata(), t.timeout)
            t.commit()
        except (KafkaException, DeliveryError, Rejected, Temporary) as e:
            t.abort()
            if dead is not None:
                raise Temporary(f"could not write the dead-letter topic: {reason(e)}") from None
            log.warning("inbound: could not commit the offset of %s[%d] %d: %s", msg.topic(), msg.partition(),
                        msg.offset(), reason(e))
            return
        if dead is not None:
            self._noted(dead)

    def _process(self, msg) -> DeadLetter | None:
        """Carry out one message. Returns its dead-letter record if it
        cannot be carried out."""
        topic, raw = msg.topic(), msg.value() or b""
        try:
            message = json.loads(raw[5:] if raw[:1] == b"\0" and len(raw) > 5 else raw)
            if not isinstance(message, dict):
                raise ValueError("not a JSON object")
        except ValueError as e:
            return self._dead_letter(msg, raw.decode(errors="replace"), None, None, f"not a JSON message: {e}")
        rule = next((r for r in self.rules if r.topic == topic and r.matches(message)), None)
        if rule is None:
            kind = str(message.get("type"))
            if (topic, kind) not in self._ignored:
                self._ignored.add((topic, kind))
                log.info("inbound: no rule for messages of type %s on %s; ignoring them", kind, topic)
            return None
        try:
            message_id = str(lookup(message, rule.id_path))
        except MessageError as e:
            return self._dead_letter(msg, message, rule, None, str(e))
        done = self.log.processed(topic, message_id)
        if done:
            log.info("inbound: %s %s is a repeat (%s before, record %s); skipped", rule.name, message_id, *done)
            return None
        attempt = outcome = None
        try:
            uid = self.odoo.uid or self.odoo.authenticate()
            attempt = self.log.begin(topic, message_id, rule.name, uid, self._wal_position())
            outcome = apply(rule, message, self.odoo)
        except (OdooUnavailable, psycopg2.Error) as e:
            raise Temporary(str(e).strip().splitlines()[0]) from None
        except OdooSetupError as e:
            raise Temporary(str(e), setup=True) from None
        except (MessageError, OdooError) as e:
            return self._dead_letter(msg, message, rule, message_id, str(e))
        finally:
            if attempt is not None:
                self.log.end(attempt, self._wal_position_or_none(),
                             (rule.model, outcome.record_id) if outcome is not None else None)
        self.log.finish(topic, message_id, rule.name, outcome.status, outcome.record_id)
        if outcome.status == "already_done":
            log.info("inbound: %s %s: already carried out (%s), noted as processed", rule.name, message_id,
                     outcome.detail)
        else:
            log.info("inbound: %s %s: done on %s %s", rule.name, message_id, rule.model, outcome.record_id)
        return None

    def _dead_letter(self, msg, message, rule: InboundRule | None, message_id: str | None,
                     reason_: str) -> DeadLetter:
        """The dead-letter record of a message that cannot be carried out."""
        value = {"message": message, "error": reason_, "rule": rule.name if rule else None,
                 "message_id": message_id, "topic": msg.topic(), "partition": msg.partition(),
                 "offset": msg.offset(), "failed_at": datetime.now(timezone.utc).isoformat()}
        headers = [("error", reason_.encode()[:1000])] + ([("rule", rule.name.encode())] if rule else [])
        return DeadLetter((msg.key() or (message_id or "").encode(), json.dumps(value).encode(), headers),
                          msg.topic(), message_id, rule.name if rule else None, reason_,
                          f"{msg.topic()}[{msg.partition()}] offset {msg.offset()}")

    def _write_dead_letter(self, dead: DeadLetter, note: bool = True) -> None:
        """Write the record (in a transaction: into the open one), then, with
        ``note``, note the message id as dead-lettered."""
        try:
            errors = self.dead_letter.deliver([dead.record])
        except DeliveryError as e:
            raise Temporary(f"could not write the dead-letter topic: {e}") from None
        if errors[0] is not None:
            raise Temporary(f"could not write the dead-letter topic: {errors[0].str()}")
        if note:
            self._noted(dead)

    def _noted(self, dead: DeadLetter) -> None:
        what = " ".join(filter(None, [dead.rule or "message", dead.message_id]))
        log.warning("inbound: %s from %s sent to %s: %s", what, dead.where, self.settings["dead_letter_topic"],
                    dead.reason)
        if dead.rule is not None and dead.message_id is not None:
            self.log.finish(dead.topic, dead.message_id, dead.rule, "dead-lettered", None)

    def _wait(self, delay: float) -> None:
        """Wait before trying the message in hand again, staying in the
        consumer group: partitions are paused and polled, and rewound to any
        message polled meanwhile."""
        assigned = self.consumer.assignment()
        self.consumer.pause(assigned)
        rewind: dict[tuple[str, int], int] = {}
        deadline = time.monotonic() + delay
        while not self.stop.is_set() and (left := deadline - time.monotonic()) > 0:
            msg = self.consumer.poll(min(left, 1.0))
            if msg is not None and not msg.error():
                key = (msg.topic(), msg.partition())
                rewind[key] = min(rewind.get(key, msg.offset()), msg.offset())
        self.consumer.resume(assigned)
        for (topic, partition), offset in rewind.items():
            try:
                self.consumer.seek(TopicPartition(topic, partition, offset))
            except KafkaException as e:  # no longer assigned: its new owner starts at the committed offset
                log.debug("inbound: seek %s[%d]: %s", topic, partition, e)

    def _wal_position(self) -> int:
        """Postgres' current WAL insert position. Any transaction Odoo
        commits after this call has a commit position past it. In polling
        mode (no Postgres), the time in microseconds instead."""
        if self.dsn is None:
            return int(time.time() * 1_000_000)
        for attempt in (1, 2):
            try:
                if self._pg is None or self._pg.closed:
                    self._pg = psycopg2.connect(self.dsn, application_name="erpdoo-inbound")
                    self._pg.autocommit = True
                with self._pg.cursor() as cur:
                    cur.execute("SELECT pg_current_wal_insert_lsn() - '0/0'")
                    return int(cur.fetchone()[0])
            except psycopg2.OperationalError:
                self._pg = None
                if attempt == 2:
                    raise
        raise AssertionError("unreachable")

    def _wal_position_or_none(self) -> int | None:
        try:
            return self._wal_position()
        except psycopg2.Error:
            return None
