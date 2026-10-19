from erpdoo.replication import TransactionAssembler
from pgmsg import SALE_ORDER, begin, commit, insert, update


def feed_all(assembler, *messages):
    return [txn for m in messages if (txn := assembler.feed(m)) is not None]


def test_transaction_is_returned_only_at_commit():
    a = TransactionAssembler(lambda table: table.replace("_", "."))
    assert feed_all(a, begin(900), SALE_ORDER, update(16384, ("5", "draft", None), ("5", "sale", None))) == []
    assert a.in_transaction
    [txn] = feed_all(a, commit(0x2000, 0x2100))
    assert not a.in_transaction
    assert (txn.xid, txn.commit_lsn, txn.end_lsn) == (900, 0x2000, 0x2100)
    [change] = txn.changes
    assert (change.op, change.table, change.model) == ("update", "sale_order", "sale.order")
    assert change.old == {"id": 5, "state": "draft", "note": None}
    assert change.new == {"id": 5, "state": "sale", "note": None}


def test_relation_is_remembered_across_transactions():
    # Postgres sends a Relation message once per streaming session.
    a = TransactionAssembler()
    feed_all(a, begin(1), SALE_ORDER, insert(16384, "1", "draft", None), commit(0x10, 0x20))
    [txn] = feed_all(a, begin(2), insert(16384, "2", "draft", None), commit(0x30, 0x40))
    assert txn.changes[0].new["id"] == 2 and txn.changes[0].model is None
