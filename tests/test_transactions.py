import json

import pytest
from confluent_kafka import KafkaError, KafkaException

from erpdoo.config import load_config
from erpdoo.events import Event
from erpdoo.sinks import DeadLetterSink, DeliveryError
from erpdoo.sinks.kafka import KafkaSink
from erpdoo.sinks.transactions import Delivery, Rejected, TransactionalDelivery, transactional


def event(n: int) -> Event:
    return Event(f"id-{n}", "SaleOrderConfirmed", "sale.order", n, {"event_id": f"id-{n}", "record_id": n})


class FakeTransactions:
    """Records what each transaction wrote; the broker refuses event ids in ``refuse`` once."""

    def __init__(self, refuse=(), checkpoint=None):
        self.refuse = set(refuse)
        self.saved = checkpoint
        self.rejected = {}
        self.committed: list[list[tuple[str, str]]] = []
        self.aborted = 0
        self.current = None
        self.timeout = 1.0

    def begin(self):
        self.current = []

    def produce(self, topic, key, value, headers=()):
        self.current.append((topic, value))
        return None

    def checkpoint(self, value):
        self.current.append(("checkpoints", json.dumps(value).encode()))

    def commit(self, before=None):
        ids = {json.loads(v).get("event_id") for t, v in self.current if t != "checkpoints"}
        refused = {("events", i): "Broker: Message size too large" for i in ids & self.refuse}
        if refused:
            self.refuse -= ids
            self.rejected.update(refused)
            self.current = None
            raise Rejected(refused)
        if before:
            before()
        self.committed.append(self.current)
        self.current = None
        self.rejected.clear()

    def abort(self):
        if self.current is not None:
            self.aborted += 1
            self.current = None

    def last_checkpoint(self):
        return self.saved

    def close(self):
        pass


class TxSink(KafkaSink):
    """A KafkaSink writing into a FakeTransactions, without a broker."""

    def __init__(self, transactions, topic="events"):
        self.topic, self.schemas, self.transactions = topic, None, transactions


def config(sinks, **delivery):
    return {"sinks": sinks, "delivery": {"transactions": True, **delivery}}


KAFKA = {"type": "kafka", "bootstrap_servers": "k:9092", "topic": "events"}


def test_transactions_need_every_sink_on_one_kafka_cluster():
    assert transactional(config([KAFKA, {**KAFKA, "topic": "events.avro", "format": "avro"}])) == (True, "")
    ok, why = transactional(config([KAFKA, {"type": "stdout"}]))
    assert not ok and why == "the stdout sink cannot take part in a Kafka transaction"
    ok, why = transactional(config([KAFKA, {"type": "webhook", "url": "u"}, {"type": "stdout"}]))
    assert why == "the stdout and webhook sinks cannot take part in a Kafka transaction"
    ok, why = transactional(config([KAFKA, {**KAFKA, "bootstrap_servers": "other:9092"}]))
    assert not ok and "different clusters" in why
    dead_letter = {**KAFKA, "dead_letter": {"topic": "dlq", "bootstrap_servers": "other:9092"}}
    assert not transactional(config([dead_letter]))[0]
    assert transactional(config([KAFKA], transactions=False)) == (False, "delivery: transactions is false")


def test_delivery_settings(tmp_path):
    path = tmp_path / "erpdoo.yml"
    path.write_text("sinks: [{type: stdout}]\n")
    assert load_config(path, env={})["delivery"] == {
        "transactions": True, "transactional_id": None, "checkpoint_topic": "erpdoo.checkpoints"}
    path.write_text("sinks: [{type: stdout}]\ndelivery: {transactions: false, transactional_id: e-1}\n")
    assert load_config(path, env={})["delivery"]["transactional_id"] == "e-1"
    for bad in ("{transactions: maybe}", "{transaction: true}", "{checkpoint_topic: ''}"):
        path.write_text(f"sinks: [{{type: stdout}}]\ndelivery: {bad}\n")
        with pytest.raises(Exception, match="delivery"):
            load_config(path, env={})


def test_a_batch_and_its_checkpoint_are_one_transaction():
    t = FakeTransactions()
    delivery = TransactionalDelivery([TxSink(t), TxSink(t, "events.avro")], t, {"source": "replication", "slot": "s"})
    delivery.send([event(1), event(2)], {"lsn": 100})
    delivery.send([event(3)], pending=True)  # noted events: the position stays
    first, second = t.committed
    assert [topic for topic, _ in first] == ["events", "events", "events.avro", "events.avro", "checkpoints"]
    checkpoint = json.loads(first[-1][1])
    assert (checkpoint["source"], checkpoint["slot"], checkpoint["lsn"], checkpoint["pending"]) == (
        "replication", "s", 100, [])
    assert json.loads(second[-1][1])["lsn"] == 100
    assert json.loads(second[-1][1])["pending"] == ["id-3"]


def test_records_the_broker_refuses_are_dead_lettered_in_a_second_transaction():
    t = FakeTransactions(refuse={"id-2"})
    sink = DeadLetterSink(TxSink(t), "kafka topic events", "dlq", "k:9092", queue=TxSink(t, "dlq"))
    TransactionalDelivery([sink], t, {}).send([event(1), event(2), event(3)], {"lsn": 7})
    [written] = t.committed
    assert [(topic, json.loads(value).get("event_id") or json.loads(value)["event"]["event_id"])
            for topic, value in written if topic != "checkpoints"] == [
        ("events", "id-1"), ("events", "id-3"), ("dlq", "id-2")]


def test_a_refused_record_without_dead_letter_topic_stops_delivery():
    t = FakeTransactions(refuse={"id-1"})
    with pytest.raises(DeliveryError, match="Message size too large"):
        TransactionalDelivery([TxSink(t)], t, {}).send([event(1)])
    assert t.committed == [] and t.aborted == 1


def test_a_failed_sink_aborts_the_transaction():
    class Broken(TxSink):
        def send(self, events):
            raise KafkaException(KafkaError(KafkaError._FENCED, "fenced"))

    t = FakeTransactions()
    with pytest.raises(DeliveryError, match="transaction failed"):
        TransactionalDelivery([TxSink(t), Broken(t)], t, {}).send([event(1)])
    assert t.committed == [] and t.aborted == 1


def test_the_test_hook_runs_before_the_commit():
    seen = []
    t = FakeTransactions()
    delivery = TransactionalDelivery([TxSink(t)], t, {})
    delivery.before_commit = lambda: seen.append(len(t.committed))
    delivery.send([event(1)])
    assert seen == [0] and len(t.committed) == 1

    class Sink:
        def __init__(self):
            self.got = []

        def send(self, events):
            self.got += events

    plain = Delivery([Sink()])
    plain.before_commit = lambda: seen.append(len(plain.sinks[0].got))
    plain.send([event(1)])
    assert seen == [0, 1]


def test_the_last_checkpoint_is_read_at_start():
    t = FakeTransactions(checkpoint={"source": "replication", "lsn": 5})
    assert TransactionalDelivery([], t, {}).checkpoint == {"source": "replication", "lsn": 5}
