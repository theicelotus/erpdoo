"""How a batch of events reaches the sinks: at-least-once, or exactly once
with Kafka transactions.

When every sink is Kafka, on one cluster (dead-letter topics included),
one transactional producer writes each batch's events to every sink's topic
together with a checkpoint record of the source position, in one Kafka
transaction:

- replication mode: the end of the batch's last transaction (``lsn``), with
  the slot and Postgres' system identifier it belongs to;
- polling mode: the poll's cursors. A poll's events are noted in the state
  store with the poll (see erpdoo.polling), so the checkpoint of a batch
  of noted events lists their ids (``pending``) instead; backfilled events
  are sent the same way in both modes.

The checkpoints go to a compacted topic, keyed by the producer's
transactional.id. At startup erpdoo fences any earlier producer with that
id (Kafka aborts its open transaction), reads the last checkpoint, skips
the transactions at or before its position, confirms the slot to it, and
forgets the noted events it lists. A crash after the Kafka commit but
before the slot is confirmed then sends nothing twice; a crash before the
commit leaves an aborted transaction, which consumers reading with
isolation.level=read_committed never see, and the batch is sent again.

With any other sink (webhook, stdout), or ``delivery: {transactions:
false}``, delivery is at-least-once: every sink in turn, then the slot.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from erpdoo.events import Event
from erpdoo.sinks import DeliveryError

log = logging.getLogger(__name__)

_kafka_log = logging.getLogger(f"{__name__}.kafka")
_kafka_log.addHandler(logging.NullHandler())
_kafka_log.propagate = False

# Errors of one record, which the broker would give it again: with a
# dead-letter topic, the transaction is tried again with the record there.
RECORD_ERRORS = {KafkaError.MSG_SIZE_TOO_LARGE, KafkaError.INVALID_MSG, KafkaError.INVALID_MSG_SIZE,
                 KafkaError.INVALID_RECORD}
ATTEMPTS = 3  # transactions tried per batch, each with the records rejected so far dead-lettered
CHECKPOINT_WINDOW = 100  # offsets read back from the end of the checkpoint topic at first
OPEN_WAIT = 75.0  # seconds to wait for another producer's open transaction while reading checkpoints


def transactional(config: dict) -> tuple[bool, str]:
    """Whether the configured sinks can be written in Kafka transactions,
    and if not, why."""
    if not config["delivery"]["transactions"]:
        return False, "delivery: transactions is false"
    sinks = config["sinks"]
    other = sorted({s["type"] for s in sinks if s["type"] != "kafka"})
    if other:
        return False, (f"the {' and '.join(other)} sink{'s' if len(other) > 1 else ''} cannot take part in a Kafka"
                       " transaction")
    clusters = {s.get("bootstrap_servers") for s in sinks} | {
        s["dead_letter"].get("bootstrap_servers") for s in sinks if isinstance(s.get("dead_letter"), dict)}
    if len(clusters) > 1:
        return False, ("the Kafka sinks and dead-letter topics are on different clusters (bootstrap_servers:"
                       f" {', '.join(sorted(map(str, clusters)))}), and a transaction spans one")
    return True, ""


class Transactions:
    """A transactional producer, shared by the Kafka sinks of one cluster.
    Creating it fences any earlier producer with the same transactional.id:
    Kafka aborts that one's open transaction."""

    def __init__(self, bootstrap_servers: str, transactional_id: str, checkpoint_topic: str | None = None,
                 timeout: float = 30.0) -> None:
        from erpdoo.sinks.kafka import check_reachable, create_topic

        check_reachable(bootstrap_servers)
        self.bootstrap_servers = bootstrap_servers
        self.transactional_id = transactional_id
        self.checkpoint_topic = checkpoint_topic
        self.timeout = timeout
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "transactional.id": transactional_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "logger": _kafka_log,
        })
        self.producer.init_transactions(timeout)
        if checkpoint_topic:
            create_topic(bootstrap_servers, checkpoint_topic, 1, {"cleanup.policy": "compact"})
        # Records of the open transaction: topic, event id, delivery error (set at commit).
        self.produced: list[tuple[str, str | None, list]] = []
        # (topic, event id) -> why the broker refused the record in an earlier attempt.
        self.rejected: dict[tuple[str, str], str] = {}
        self.open = False

    def begin(self) -> None:
        self.producer.begin_transaction()
        self.produced = []
        self.open = True

    def produce(self, topic: str, key: bytes | None, value: bytes, headers: list[tuple[str, bytes]] = ()) -> \
            KafkaError | None:
        """Write a record in the open transaction. Returns the error if the
        producer refused it at once (e.g. too large); the broker's answer
        comes with commit()."""
        result: list[KafkaError | None] = [None]
        event_id = next((v.decode() for k, v in headers if k == "event_id"), None)
        try:
            self.producer.produce(topic, key=key, value=value, headers=list(headers),
                                  on_delivery=lambda err, _msg: result.__setitem__(0, err))
        except KafkaException as e:
            return e.args[0]
        self.produced.append((topic, event_id, result))
        return None

    def commit(self, before: Callable[[], None] | None = None) -> None:
        """Commit the open transaction. Raises Rejected if the broker refused
        records of their own (tried again with those in ``rejected``), and
        DeliveryError otherwise; the transaction is then aborted."""
        try:
            if before is not None:
                self.producer.flush(self.timeout)
                before()
            self.producer.commit_transaction(self.timeout)
        except KafkaException as e:
            error = e.args[0]
            rejected = {(topic, event_id): result[0].str() for topic, event_id, result in self.produced
                        if event_id and result[0] is not None and result[0].code() in RECORD_ERRORS}
            self.abort()
            if rejected.keys() - self.rejected.keys() and not error.fatal():
                self.rejected.update(rejected)
                raise Rejected(rejected) from None
            raise DeliveryError(f"kafka: transaction not committed: {error.str()}") from None
        self.open = False
        self.rejected.clear()

    def abort(self) -> None:
        """Abort the open transaction, if any. If Kafka does not answer, the
        next producer with this transactional.id aborts it."""
        if not self.open:
            return
        self.open = False
        try:
            self.producer.abort_transaction(min(self.timeout, 10))
        except KafkaException as e:
            log.debug("kafka: could not abort the transaction: %s", e)

    def last_checkpoint(self) -> dict | None:
        """The last committed checkpoint of this transactional.id, read back
        from the end of the checkpoint topic."""
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"{self.transactional_id}.checkpoints",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
            "logger": _kafka_log,
        })
        key = self.transactional_id.encode()
        try:
            low, high = consumer.get_watermark_offsets(TopicPartition(self.checkpoint_topic, 0), self.timeout)
            found, end, window = None, high, CHECKPOINT_WINDOW
            while found is None and end > low:
                start = max(low, end - window)
                found = self._scan(consumer, key, start, end)
                end, window = start, window * 4
        finally:
            consumer.close()
        return json.loads(found) if found is not None else None

    def _scan(self, consumer: Consumer, key: bytes, start: int, end: int) -> bytes | None:
        """The value of the last record with ``key`` between offsets ``start``
        and ``end`` (not included)."""
        consumer.assign([TopicPartition(self.checkpoint_topic, 0, start)])
        found = None
        deadline = time.monotonic() + OPEN_WAIT
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
                if msg.offset() >= end:
                    return found
                # Read committed stops at another producer's open transaction until it ends.
                continue
            if msg.offset() >= end:
                return found
            if msg.key() == key:
                found = msg.value()
        log.warning("kafka: %s: another producer's transaction stayed open for %g s; using the checkpoints"
                    " read so far, so events after them may be sent again", self.checkpoint_topic, OPEN_WAIT)
        return found

    def checkpoint(self, value: dict) -> None:
        error = self.produce(self.checkpoint_topic, self.transactional_id.encode(), json.dumps(value).encode())
        if error is not None:
            raise DeliveryError(f"kafka: could not write the checkpoint to {self.checkpoint_topic}: {error.str()}")

    def close(self) -> None:
        self.abort()


class Rejected(Exception):
    """The broker refused records of their own; try again with them dead-lettered."""


class Delivery:
    """At-least-once: every sink delivers in turn, then the caller confirms
    the source position. A crash in between sends the events again."""

    exactly_once = False

    def __init__(self, sinks: list) -> None:
        self.sinks = sinks
        self.checkpoint: dict | None = None  # the last checkpoint at startup (exactly once only)
        # Test hook: runs once the events are written, before they are committed.
        self.before_commit: Callable[[], None] | None = None

    def send(self, events: list[Event], position: dict | None = None, pending: bool = False) -> None:
        """Deliver ``events`` to every sink. ``position`` is how far the
        source has got with them, and ``pending`` says they are noted in the
        state store (both for the checkpoint, when exactly once)."""
        for sink in self.sinks:
            sink.send(events)
        if self.before_commit is not None:
            self.before_commit()

    def close(self) -> None:
        for sink in self.sinks:
            sink.close()


class TransactionalDelivery(Delivery):
    """Exactly once: each batch in one Kafka transaction with a checkpoint."""

    exactly_once = True

    def __init__(self, sinks: list, transactions: Transactions, identity: dict) -> None:
        super().__init__(sinks)
        self.transactions = transactions
        self.identity = identity  # what the positions belong to, e.g. the slot
        self.checkpoint = transactions.last_checkpoint()
        self.position: dict = {}  # carried by every checkpoint; set from a valid one at startup

    def send(self, events: list[Event], position: dict | None = None, pending: bool = False) -> None:
        if position:
            self.position.update(position)
        record = {**self.identity, **self.position, "pending": [e.event_id for e in events] if pending else [],
                  "committed_at": datetime.now(timezone.utc).isoformat()}
        t = self.transactions
        for attempt in range(1, ATTEMPTS + 1):
            t.begin()
            try:
                for sink in self.sinks:
                    sink.send(events)
                t.checkpoint(record)
                t.commit(self.before_commit)
                return
            except Rejected as e:
                if attempt == ATTEMPTS:
                    raise DeliveryError(f"kafka: records still refused after {ATTEMPTS} transactions:"
                                        f" {next(iter(e.args[0].values()))}") from None
                for (topic, event_id), error in e.args[0].items():
                    log.warning("kafka: %s refused event %s (%s); trying the transaction again without it",
                                topic, event_id, error)
            except KafkaException as e:  # e.g. fenced by another erpdoo with this transactional.id
                t.abort()
                raise DeliveryError(f"kafka: transaction failed: {e.args[0].str()}") from None
            except BaseException:
                t.abort()
                raise

    def close(self) -> None:
        super().close()
        self.transactions.close()
