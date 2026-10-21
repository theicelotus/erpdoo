"""Check that events survive erpdoo and Kafka being down, exactly once.

Against the running compose stack, with orders confirmed one per
transaction and two in one transaction:

1. erpdoo stopped: confirm orders, start erpdoo. Each SaleOrderConfirmed
   must arrive exactly once on both topics (JSON Schema and Avro).
2. Redpanda stopped while erpdoo runs: confirm orders. erpdoo cannot
   deliver them, gives up (Kafka does not acknowledge), and waits for
   Kafka after Docker restarts it. While Redpanda is down, the replication
   slot must not be confirmed past any of the transactions; once Redpanda
   is back, each event must arrive exactly once, and the slot must then
   move past them.

Every event is also compared with Odoo (scripts/event_checks.py): events
sent after a restart are rebuilt from the state store's versions.

    python scripts/delivery_test.py
"""

from __future__ import annotations

import argparse
import logging
import shlex
import subprocess
import sys
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from event_checks import OdooCheck  # noqa: E402
from smoke_test import REGISTRY, Events, Odoo, wait_until  # noqa: E402

SETTLE = 10  # seconds to keep reading after the last event, for duplicates


def lsn(text: str) -> int:
    high, low = text.split("/")
    return int(high, 16) << 32 | int(low, 16)


def fmt(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


class SlotWatch:
    """Samples the slot's confirmed position in the background."""

    def __init__(self, dsn: str, slot: str) -> None:
        self.dsn, self.slot = dsn, slot
        self.samples: list[tuple[float, int]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def current(self) -> int:
        with closing(psycopg2.connect(self.dsn)) as conn, conn.cursor() as cur:
            cur.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s", [self.slot])
            return lsn(cur.fetchone()[0])

    def wal_now(self) -> int:
        with closing(psycopg2.connect(self.dsn)) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            return lsn(cur.fetchone()[0])

    def _run(self) -> None:
        while not self._stop.wait(0.5):
            try:
                self.samples.append((time.monotonic(), self.current()))
            except psycopg2.Error:
                pass

    def between(self, start: float, end: float) -> list[int]:
        return [value for t, value in self.samples if start <= t <= end]

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--avro-topic", default="erpdoo.events.avro")
    p.add_argument("--registry", default=REGISTRY)
    p.add_argument("--pg", default="host=localhost dbname=odoo user=erpdoo password=erpdoo",
                   help="a connection that can read pg_replication_slots")
    p.add_argument("--slot", default="erpdoo")
    p.add_argument("--compose", default="docker compose", help="how to run docker compose")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str) -> str:
        return subprocess.run([*compose, *command], check=True, text=True, capture_output=True).stdout

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    # The consumers would log every failed connection while Redpanda is down.
    quiet = logging.getLogger("delivery_test.kafka")
    quiet.addHandler(logging.NullHandler())
    quiet.propagate = False
    feeds = {"json": Events(args.kafka, args.topic, args.registry, logger=quiet),
             "avro": Events(args.kafka, args.avro_topic, args.registry, logger=quiet)}
    in_odoo = OdooCheck(odoo, str(Path(__file__).parent.parent / "rules"))
    partner = odoo("res.partner", "create", {"name": f"Delivery Test {uuid.uuid4().hex[:6]}", "customer_rank": 1})
    product = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "service")], limit=1)[0]
    failures: list[str] = []

    before_commit: dict[int, int] = {}  # order -> WAL position just before its confirmation

    def confirm_orders() -> list[int]:
        """Three orders confirmed one per transaction, two in one."""
        orders = [odoo("sale.order", "create", {"partner_id": partner, "order_line": [(0, 0, {"product_id": product})]})
                  for _ in range(5)]
        for group in [[o] for o in orders[:3]] + [orders[3:]]:
            mark = watch.wal_now()
            before_commit.update(dict.fromkeys(group, mark))
            odoo("sale.order", "action_confirm", group)
        return orders

    def confirmations(feed: Events, order: int) -> list[tuple[float, dict]]:
        return [(t, e) for t, e in feed.seen if e["model"] == "sale.order" and e["record_id"] == order
                and e["event_type"] == "SaleOrderConfirmed"]

    def collect(orders: list[int], timeout: float) -> dict[int, dict]:
        """Read both topics until every order's event arrived on each (or the
        timeout), then SETTLE seconds more; check each arrived once."""
        deadline = time.monotonic() + timeout
        for feed in feeds.values():
            while time.monotonic() < deadline and not all(confirmations(feed, o) for o in orders):
                feed.poll(0.2)
        for feed in feeds.values():
            end = time.monotonic() + SETTLE
            while time.monotonic() < end:
                feed.poll(0.2)
        first = {}
        for name, feed in feeds.items():
            for order in orders:
                got = confirmations(feed, order)
                if len(got) != 1:
                    failures.append(f"order {order}: {len(got)} SaleOrderConfirmed events on the {name} topic,"
                                    " expected exactly one")
                elif name == "json":
                    first[order] = got[0][1]
        for order, event in first.items():
            problems = in_odoo.problems(event)
            if event["changes"].get("state", {}).get("from") != "draft":
                problems.append(f"changes {event['changes']}")
            failures.extend(f"order {order}: {problem}" for problem in problems)
        return first

    watch = SlotWatch(args.pg, args.slot)
    try:
        # 1. erpdoo stopped.
        sh("stop", "erpdoo")
        print("ok   stopped erpdoo")
        orders = confirm_orders()
        print(f"ok   confirmed orders {orders} while erpdoo was stopped")
        sh("start", "erpdoo")
        before = len(failures)
        collect(orders, 120)
        if len(failures) == before:
            print(f"ok   after erpdoo started, each of the {len(orders)} events arrived exactly once on both topics")

        # 2. Redpanda stopped while erpdoo runs.
        stopped_at = time.monotonic()
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        sh("stop", "redpanda")
        print("ok   stopped Redpanda")
        orders = confirm_orders()
        print(f"ok   confirmed orders {orders} while Redpanda was down")
        # erpdoo gives up on delivering once Kafka has not acknowledged for
        # its flush timeout (30 s), and exits; Docker starts it again.
        def gave_up():
            return "stopping, unconfirmed events are sent again after a restart" in sh(
                "logs", "--no-color", "--since", started, "erpdoo")
        try:
            wait_until("erpdoo to give up on Kafka", gave_up, timeout=90)
            print("ok   erpdoo could not deliver (Kafka did not acknowledge) and stopped")
        except SystemExit as e:
            failures.append(str(e))
        down_until = time.monotonic()
        sh("start", "redpanda")
        print("ok   started Redpanda")
        before = len(failures)
        events = collect(orders, 180)
        if len(failures) == before:
            print(f"ok   after Redpanda was back, each of the {len(orders)} events arrived exactly once on both topics")
        during = watch.between(stopped_at, down_until)
        if during:
            # Each order's commit: as its event says, or for an event that
            # never came, at least the WAL position just before it.
            commits = {o: lsn(events[o]["source"]["commit_lsn"]) if o in events else before_commit[o] for o in orders}
            first = min(commits, key=commits.get)
            highest = max(during)
            if highest > commits[first]:
                failures.append(f"the slot was confirmed to {fmt(highest)} while Redpanda was down, past the commit"
                                f" of order {first}'s confirmation at {fmt(commits[first])}, which was not delivered")
            else:
                print(f"ok   while Redpanda was down the slot stayed at or before {fmt(highest)}, before the first"
                      f" undelivered commit ({fmt(commits[first])})")
        if events and during:
            last_commit = max(lsn(e["source"]["commit_lsn"]) for e in events.values())
            try:
                wait_until("the slot to move past the delivered events", lambda: watch.current() > last_commit, 30)
                print("ok   after delivery the slot moved past them")
            except SystemExit as e:
                failures.append(str(e))
        elif not during:
            failures.append("no sample of the slot while Redpanda was down")
    finally:
        watch.stop()
        subprocess.run([*compose, "start", "redpanda", "erpdoo"], capture_output=True)
    for failure in failures:
        print(f"FAIL {failure}")
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
