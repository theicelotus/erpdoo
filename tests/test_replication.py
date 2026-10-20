from erpdoo.replication import (
    PublicationState, TableState, TransactionAssembler, plan_setup, replica_identity_problem,
)
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



def table(name, full=False, owned=False, readable=True, pk=True):
    return TableState(name, "f" if full else "d", pk, False, owned, readable)


TABLES = ["ir_model", "res_partner", "sale_order"]


def admin_publication(*tables, publishes_changes=True):
    return PublicationState(False, False, False, publishes_changes, frozenset(tables))


def test_admin_publication_that_covers_everything_needs_nothing():
    states = {"ir_model": table("ir_model"), "res_partner": table("res_partner"),
              "sale_order": table("sale_order", full=True)}
    assert plan_setup("erpdoo", "erpdoo", TABLES, ["sale_order"], states,
                      admin_publication(*TABLES, "crm_lead"), False) == ([], [])


def test_admin_publication_missing_tables_gives_the_sql():
    states = {"ir_model": table("ir_model"), "res_partner": table("res_partner", readable=False),
              "sale_order": table("sale_order")}
    steps, missing = plan_setup("erpdoo", "erpdoo", TABLES, ["sale_order"], states,
                                admin_publication("ir_model"), False)
    assert not any(step.allowed for step in steps)
    assert [step.sql for step in steps] == [
        "ALTER TABLE public.sale_order REPLICA IDENTITY FULL",
        "ALTER PUBLICATION erpdoo ADD TABLE public.res_partner, public.sale_order",
        "GRANT SELECT ON public.res_partner TO erpdoo",
    ]
    assert missing == [
        "need REPLICA IDENTITY FULL, so that updates carry the previous row: sale_order",
        "not in publication erpdoo: res_partner, sale_order",
        "not readable by erpdoo: res_partner",
    ]


def test_publication_that_skips_updates_is_reported():
    states = {t: table(t, full=True) for t in TABLES}
    steps, missing = plan_setup("erpdoo", "erpdoo", TABLES, [], states,
                                admin_publication(*TABLES, publishes_changes=False), False)
    assert [s.sql for s in steps] == [
        "ALTER PUBLICATION erpdoo SET (publish = 'insert, update, delete, truncate')"]
    assert not steps[0].allowed


def test_missing_publication_is_created_when_erpdoo_owns_the_tables():
    states = {t: table(t, owned=True) for t in TABLES}
    steps, _ = plan_setup('"Erpdoo"', "erpdoo", TABLES, ["sale_order"], states, None, True)
    assert [(s.sql, s.allowed) for s in steps] == [
        ("ALTER TABLE public.sale_order REPLICA IDENTITY FULL", True),
        ('CREATE PUBLICATION "Erpdoo" FOR TABLE public.ir_model, public.res_partner, public.sale_order', True),
    ]
    # Without CREATE on the database an admin has to create it.
    steps, _ = plan_setup("erpdoo", "erpdoo", TABLES, ["sale_order"], states, None, False)
    assert not steps[1].allowed


def test_own_publication_is_kept_to_the_tables_needed():
    states = {t: table(t, owned=True, full=True) for t in TABLES + ["crm_lead"]}
    pub = PublicationState(True, True, False, True, frozenset(["ir_model", "crm_lead"]))
    steps, _ = plan_setup("erpdoo", "erpdoo", TABLES, [], states, pub, False)
    assert [(s.sql, s.allowed) for s in steps] == [
        ("ALTER PUBLICATION erpdoo ADD TABLE public.res_partner, public.sale_order", True),
        ("ALTER PUBLICATION erpdoo DROP TABLE public.crm_lead", True),
    ]


def test_admin_publication_for_all_tables_is_left_alone():
    states = {t: table(t, full=True) for t in TABLES}
    pub = PublicationState(False, False, True, True, frozenset(TABLES))
    assert plan_setup("erpdoo", "erpdoo", TABLES, [], states, pub, False) == ([], [])


def test_replica_identity_problems():
    assert replica_identity_problem("t", table("t")) is None
    assert replica_identity_problem("t", None) == "t: no such table"
    assert "no primary key" in replica_identity_problem("t", table("t", pk=False))
    assert replica_identity_problem("t", table("t", full=True, pk=False)) is None
