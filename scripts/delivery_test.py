"""Check that events survive crashes, erpdoo and Kafka being down, exactly once.

Against the running compose stack, with two erpdoos: the dev stack's, whose
stdout sink keeps it at-least-once, and a second one of this test's
(erpdoo-delivery-test), whose sinks are all Kafka, so it delivers in Kafka
transactions. It has its own slot, state, topics (erpdoo.delivery-test,
.avro) and schema namespaces. Orders are confirmed one per transaction and
two in one transaction, and every topic is read with
isolation.level=read_committed:

1. Both erpdoos stopped: confirm orders, start them. Each SaleOrderConfirmed
   must arrive exactly once on every topic (JSON Schema and Avro).
2. Redpanda stopped while they run: confirm orders. Neither can deliver
   them; each gives up (Kafka does not acknowledge, or the transaction does
   not commit) and waits for Kafka after Docker restarts it. While Redpanda
   is down, neither replication slot may be confirmed past any of the
   transactions; once Redpanda is back, each event must arrive exactly
   once, and the slots must then move past them.
3. The second erpdoo crashes after the Kafka commit, before the slot is
   confirmed (its test hook, ERPDOO_TEST_HOLD_ACK): after its restart,
   Postgres streams the transactions again, and it must skip them by its
   checkpoint, so each event arrives exactly once.
4. It crashes before the Kafka commit (the hook file says "before-commit"):
   the events are written in the open transaction (a read_uncommitted
   consumer sees them) but no read_committed consumer may; after its
   restart, each event must arrive exactly once.

Every event is also compared with Odoo (scripts/event_checks.py): events
sent after a restart are rebuilt from the state store's versions.

With --no-transactions the second erpdoo is at-least-once too (delivery:
transactions: false), and steps 3 and 4 must fail: that shows they test
the transactions.

Removes the second erpdoo, its slot, topics and schema subjects at the end.

    python scripts/delivery_test.py [--no-transactions]
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
from contextlib import closing
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from event_checks import OdooCheck  # noqa: E402
from smoke_test import REGISTRY, Events, Odoo, wait_until  # noqa: E402

ROOT = Path(__file__).parent.parent
SETTLE = 10  # seconds to keep reading after the last event, for duplicates
CONTAINER, SLOT = "erpdoo-delivery-test", "erpdoo_delivery_test"
TOPIC, NAMESPACE = "erpdoo.delivery-test", "erpdoo.delivery_test"
HOLD = "/tmp/delivery-test-hold"  # the second erpdoo's ERPDOO_TEST_HOLD_ACK
CONFIG = """# erpdoo.yml for scripts/delivery_test.py: every sink Kafka, so it
# delivers in Kafka transactions (unless turned off below).
sinks:
  - type: kafka
    bootstrap_servers: redpanda:9092
    topic: {topic}
    format: json
    schema_registry: {{url: "http://redpanda:8081", namespace: {namespace}}}
  - type: kafka
    bootstrap_servers: redpanda:9092
    topic: {topic}.avro
    format: avro
    schema_registry: {{url: "http://redpanda:8081", namespace: {namespace}.avro}}
delivery:
  transactions: {transactions}
"""


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
            except (psycopg2.Error, TypeError):
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
    p.add_argument("--admin-sql", default="docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U odoo -d odoo",
                   help="command that runs SQL from stdin as an admin")
    p.add_argument("--no-transactions", action="store_true",
                   help="run the second erpdoo at-least-once; steps 3 and 4 must then fail")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str, check: bool = True) -> str:
        return subprocess.run(command, check=check, text=True, capture_output=True).stdout

    def dc(*command: str) -> str:
        return sh(*compose, *command)

    def logs(container: str, since: str) -> str:
        result = subprocess.run(["docker", "logs", "--since", since, container], text=True, capture_output=True)
        return result.stdout + result.stderr

    def now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def running(container: str) -> bool:
        return sh("docker", "inspect", "-f", "{{.State.Running}}", container, check=False).strip() == "true"

    work = Path(tempfile.mkdtemp(prefix="erpdoo-delivery-test-"))
    config = work / "erpdoo.yml"
    config.write_text(CONFIG.format(topic=TOPIC, namespace=NAMESPACE,
                                    transactions="false" if args.no_transactions else "true"))
    config.chmod(0o644)

    def cleanup() -> None:
        sh("docker", "rm", "-f", CONTAINER, check=False)
        subprocess.run(shlex.split(args.admin_sql), text=True, capture_output=True,
                       input=f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
                             f" WHERE slot_name = '{SLOT}';")
        for topic in (TOPIC, f"{TOPIC}.avro"):
            subprocess.run([*compose, "exec", "-T", "redpanda", "rpk", "topic", "delete", topic], capture_output=True)
        try:
            subjects = json.load(urllib.request.urlopen(f"{args.registry}/subjects", timeout=10))
        except OSError:
            subjects = []
        for subject in subjects:
            if subject.startswith(f"{NAMESPACE}."):
                for query in ("", "?permanent=true"):  # a subject is soft-deleted first
                    try:
                        urllib.request.urlopen(urllib.request.Request(
                            f"{args.registry}/subjects/{subject}{query}", method="DELETE"), timeout=10)
                    except OSError:
                        pass

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    cleanup()  # left over by an interrupted run
    started = now()
    subprocess.run([*compose, "run", "-d", "--no-deps", "--name", CONTAINER,
                    "-v", f"{config}:/delivery-test.yml:ro",
                    "-e", "ERPDOO_CONFIG=/delivery-test.yml", "-e", "ERPDOO_INBOUND_RULES=/no-inbound-rules",
                    "-e", f"ERPDOO_SLOT={SLOT}", "-e", "ERPDOO_STATE=/tmp/delivery-test.db",
                    "-e", f"ERPDOO_TEST_HOLD_ACK={HOLD}", "erpdoo", "erpdoo"], check=True, capture_output=True)
    # Like the dev stack's erpdoo: Docker starts it again when it exits.
    sh("docker", "update", "--restart", "unless-stopped", CONTAINER)

    def streaming():
        text = logs(CONTAINER, started)
        if "Traceback" in text:
            raise SystemExit(f"{CONTAINER} failed:\n{text[-3000:]}")
        return "streaming to" in text

    wait_until(f"{CONTAINER} to stream", streaming, timeout=180)
    mode = "at-least-once" if args.no_transactions else "exactly once"
    expected = "delivery is at-least-once" if args.no_transactions else "delivery is exactly once"
    if expected not in logs(CONTAINER, started):
        print(f"FAIL {CONTAINER} does not log {expected!r}")
        return 1
    print(f"ok   started {CONTAINER}, delivering {mode} (Kafka transactions"
          f" {'off' if args.no_transactions else 'on'}); the dev stack's erpdoo is at-least-once (stdout sink)")

    # The consumers would log every failed connection while Redpanda is down.
    quiet = logging.getLogger("delivery_test.kafka")
    quiet.addHandler(logging.NullHandler())
    quiet.propagate = False

    def feed(topic: str, isolation: str = "read_committed") -> Events:
        return Events(args.kafka, topic, args.registry, logger=quiet, isolation=isolation)

    dev = {"json": feed(args.topic), "avro": feed(args.avro_topic)}
    test = {"json": feed(TOPIC), "avro": feed(f"{TOPIC}.avro")}
    uncommitted = feed(TOPIC, "read_uncommitted")
    in_odoo = OdooCheck(odoo, str(ROOT / "rules"))
    partner = odoo("res.partner", "create", {"name": f"Delivery Test {uuid.uuid4().hex[:6]}", "customer_rank": 1})
    product = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "service")], limit=1)[0]
    failures: list[str] = []

    before_commit: dict[int, int] = {}  # order -> WAL position just before its confirmation

    def confirm_orders() -> list[int]:
        """Three orders confirmed one per transaction, two in one."""
        orders = [odoo("sale.order", "create", {"partner_id": partner, "order_line": [(0, 0, {"product_id": product})]})
                  for _ in range(5)]
        for group in [[o] for o in orders[:3]] + [orders[3:]]:
            mark = watches[args.slot].wal_now()
            before_commit.update(dict.fromkeys(group, mark))
            odoo("sale.order", "action_confirm", group)
        return orders

    def confirmations(events: Events, order: int) -> list[tuple[float, dict]]:
        return [(t, e) for t, e in events.seen if e["model"] == "sale.order" and e["record_id"] == order
                and e["event_type"] == "SaleOrderConfirmed"]

    def collect(feeds: dict[str, dict[str, Events]], orders: list[int], timeout: float) -> dict[str, dict[int, dict]]:
        """Read the topics until every order's event arrived on each (or the
        timeout), then SETTLE seconds more; check each arrived once. Returns
        each erpdoo's events, by order."""
        deadline = time.monotonic() + timeout
        every = [(who, name, events) for who, topics in feeds.items() for name, events in topics.items()]
        for _, _, events in every:
            while time.monotonic() < deadline and not all(confirmations(events, o) for o in orders):
                events.poll(0.2)
        end = time.monotonic() + SETTLE
        while time.monotonic() < end:
            for _, _, events in every:
                events.poll(0.1)
        first: dict[str, dict[int, dict]] = {who: {} for who in feeds}
        for who, name, events in every:
            for order in orders:
                got = confirmations(events, order)
                if len(got) != 1:
                    failures.append(f"{who}: order {order}: {len(got)} SaleOrderConfirmed events on the {name}"
                                    " topic, expected exactly one")
                elif name == "json":
                    first[who][order] = got[0][1]
        for who, found in first.items():
            for order, event in found.items():
                problems = in_odoo.problems(event)
                if event["changes"].get("state", {}).get("from") != "draft":
                    problems.append(f"changes {event['changes']}")
                failures.extend(f"{who}: order {order}: {problem}" for problem in problems)
        return first

    def check_slot(who: str, slot: str, orders: list[int], events: dict[int, dict], start: float, end: float) -> None:
        """The slot stayed before the first undelivered commit between start
        and end, and then moved past the delivered events."""
        during = watches[slot].between(start, end)
        if not during:
            failures.append(f"{who}: no sample of slot {slot} while the events were not delivered")
            return
        # Each order's commit: as its event says, or for an event that
        # never came, at least the WAL position just before it.
        commits = {o: lsn(events[o]["source"]["commit_lsn"]) if o in events else before_commit[o] for o in orders}
        first = min(commits, key=commits.get)
        highest = max(during)
        if highest > commits[first]:
            failures.append(f"{who}: slot {slot} was confirmed to {fmt(highest)} before the events were delivered,"
                            f" past the commit of order {first}'s confirmation at {fmt(commits[first])}")
        else:
            print(f"ok   {who}: meanwhile slot {slot} stayed at or before {fmt(highest)}, before the first"
                  f" undelivered commit ({fmt(commits[first])})")
        if events:
            last_commit = max(lsn(e["source"]["commit_lsn"]) for e in events.values())
            try:
                wait_until(f"slot {slot} to move past the delivered events",
                           lambda: watches[slot].current() > last_commit, 30)
                print(f"ok   {who}: after delivery slot {slot} moved past them")
            except SystemExit as e:
                failures.append(f"{who}: {e}")

    def exactly_once(feeds, orders: list[int], timeout: float, what: str) -> dict[str, dict[int, dict]]:
        before = len(failures)
        found = collect(feeds, orders, timeout)
        if len(failures) == before:
            print(f"ok   {what}, each of the {len(orders)} events arrived exactly once on every topic"
                  f" ({', '.join(feeds)})")
        return found

    def restarted(since: str) -> bool:
        """The second erpdoo exited through its test hook and Docker started it again."""
        text = logs(CONTAINER, since)
        return "(test hook)" in text and text.count("streaming to") >= 1 and running(CONTAINER)

    watches = {slot: SlotWatch(args.pg, slot) for slot in (args.slot, SLOT)}
    both = {"dev erpdoo": dev, CONTAINER: test}
    try:
        # 1. Both erpdoos stopped.
        dc("stop", "erpdoo")
        sh("docker", "stop", CONTAINER)
        print("ok   stopped both erpdoos")
        orders = confirm_orders()
        print(f"ok   confirmed orders {orders} while they were stopped")
        dc("start", "erpdoo")
        sh("docker", "start", CONTAINER)
        exactly_once(both, orders, 120, "after both started")

        # 2. Redpanda stopped while they run.
        stopped_at = time.monotonic()
        since = now()
        dc("stop", "redpanda")
        print("ok   stopped Redpanda")
        orders = confirm_orders()
        print(f"ok   confirmed orders {orders} while Redpanda was down")
        # erpdoo gives up once Kafka has not acknowledged for its flush
        # timeout (30 s), or the transaction not committed in as long, and
        # exits; Docker starts it again.
        for who, text in (("erpdoo", lambda: dc("logs", "--no-color", "--since", since, "erpdoo")),
                          (CONTAINER, lambda: logs(CONTAINER, since))):
            try:
                wait_until(f"{who} to give up on Kafka",
                           lambda: "stopping, unconfirmed events are sent again after a restart" in text(), 90)
                print(f"ok   {who} could not deliver (Kafka did not acknowledge) and stopped")
            except SystemExit as e:
                failures.append(str(e))
        down_until = time.monotonic()
        dc("start", "redpanda")
        print("ok   started Redpanda")
        found = exactly_once(both, orders, 180, "after Redpanda was back")
        check_slot("dev erpdoo", args.slot, orders, found["dev erpdoo"], stopped_at, down_until)
        check_slot(CONTAINER, SLOT, orders, found[CONTAINER], stopped_at, down_until)

        # 3. A crash after the Kafka commit, before the slot is confirmed.
        sh("docker", "exec", CONTAINER, "touch", HOLD)
        held_at, since = time.monotonic(), now()
        orders = confirm_orders()
        print(f"ok   {CONTAINER} holds the slot (test hook); confirmed orders {orders}")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not all(confirmations(test["json"], o) for o in orders):
            test["json"].poll(0.2)
        committed = [o for o in orders if confirmations(test["json"], o)]
        print(f"     committed to Kafka before the crash: {len(committed)} of {len(orders)} events")
        released = time.monotonic()
        sh("docker", "exec", CONTAINER, "rm", "-f", HOLD)
        try:
            wait_until(f"{CONTAINER} to crash and start again", lambda: restarted(since), 90)
            print(f"ok   {CONTAINER} crashed without confirming the slot, and Docker started it again")
        except SystemExit as e:
            failures.append(str(e))
        if "skipping transactions up to it" in logs(CONTAINER, since):
            print(f"     {CONTAINER} read its checkpoint and skipped what it had committed")
        found = exactly_once({CONTAINER: test}, orders, 120, "after the crash after the commit")
        check_slot(CONTAINER, SLOT, orders, found[CONTAINER], held_at, released)

        # 4. A crash before the Kafka commit.
        sh("docker", "exec", CONTAINER, "sh", "-c", f"printf before-commit > {HOLD}")
        since = now()
        orders = confirm_orders()
        print(f"ok   {CONTAINER} holds before its commit (test hook); confirmed orders {orders}")
        try:
            wait_until(f"{CONTAINER} to hold before the commit",
                       lambda: "holding before the commit" in logs(CONTAINER, since), 60)
        except SystemExit as e:
            failures.append(str(e))
        end = time.monotonic() + SETTLE
        while time.monotonic() < end:
            uncommitted.poll(0.1)
            test["json"].poll(0.1)
        written = [o for o in orders if confirmations(uncommitted, o)]
        visible = [o for o in orders if confirmations(test["json"], o)]
        if not written:
            failures.append(f"{CONTAINER}: a read_uncommitted consumer saw none of the held events")
        else:
            print(f"ok   a read_uncommitted consumer sees the written events of orders {written}")
        if visible:
            failures.append(f"{CONTAINER}: a read_committed consumer saw the events of orders {visible} before"
                            " they were committed")
        else:
            print("ok   a read_committed consumer sees none of them")
        sh("docker", "exec", CONTAINER, "rm", "-f", HOLD)
        try:
            wait_until(f"{CONTAINER} to crash and start again", lambda: restarted(since), 90)
            print(f"ok   {CONTAINER} crashed without committing, and Docker started it again")
        except SystemExit as e:
            failures.append(str(e))
        exactly_once({CONTAINER: test}, orders, 120, "after the crash before the commit")
    finally:
        for watch in watches.values():
            watch.stop()
        subprocess.run([*compose, "start", "redpanda", "erpdoo"], capture_output=True)
        cleanup()
        print(f"     removed {CONTAINER}, its slot, topics and schema subjects")
    for failure in failures:
        print(f"FAIL {failure}")
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
