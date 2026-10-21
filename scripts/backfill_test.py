"""Check that erpdoo backfills the window a lost replication slot missed.

Against the running compose stack:
- with erpdoo stopped, confirms an order, cancels a confirmed one and wins
  a lead, then makes Postgres invalidate erpdoo's slot (a small
  max_slot_wal_keep_size and WAL written elsewhere);
- starts erpdoo, which recreates the slot and backfills the window; each
  of the three must arrive exactly once, marked as backfilled, with the
  from and to values of the change (the lead's probability is not tracked
  by Odoo, so that event is approximate and has no "from");
- runs erpdoo backfill for the same window twice more; both runs must give
  the same event ids.
Resets max_slot_wal_keep_size at the end.

    python scripts/backfill_test.py
"""

from __future__ import annotations

import argparse
import re
import shlex
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from smoke_test import Events, Odoo, wait_until  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--compose", default="docker compose", help="how to run docker compose")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str, **kwargs) -> subprocess.CompletedProcess:
        return subprocess.run([*compose, *command], check=True, text=True, capture_output=True, **kwargs)

    def admin(statement: str) -> str:
        return sh("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "odoo", "-d", "postgres",
                  "-Atc", statement).stdout.strip()

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    events = Events(args.kafka, args.topic)
    tag = uuid.uuid4().hex[:6]
    partner = odoo("res.partner", "create", {"name": f"Backfill Test {tag}", "customer_rank": 1})
    product = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "service")], limit=1)[0]

    def order() -> int:
        return odoo("sale.order", "create", {"partner_id": partner, "order_line": [(0, 0, {"product_id": product})]})

    to_confirm, to_cancel = order(), order()
    lead = odoo("crm.lead", "create", {"name": f"Backfill Test {tag}", "type": "opportunity", "partner_id": partner})
    odoo("sale.order", "action_confirm", [to_cancel])
    if not events.wait("sale.order", to_cancel, 30):
        print("FAIL erpdoo is not delivering events")
        return 1
    # A later write erpdoo processes, so that the confirmation above is
    # before the window that is about to be missed.
    time.sleep(1.5)
    odoo("res.partner", "write", [partner], {"comment": "backfill test"})
    time.sleep(3)
    started = datetime.now(timezone.utc)
    failures: list[str] = []
    try:
        admin("ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'")
        admin("SELECT pg_reload_conf()")
        sh("stop", "erpdoo")
        print("ok   stopped erpdoo")
        odoo("sale.order", "action_confirm", [to_confirm])
        wizard = odoo("sale.order.cancel", "create", {"order_id": to_cancel})
        odoo("sale.order.cancel", "action_cancel", [wizard])
        odoo("crm.lead", "action_set_won", [lead])
        print(f"ok   confirmed order {to_confirm}, cancelled order {to_cancel}, won lead {lead}")
        for _ in range(4):
            admin("CREATE TABLE erpdoo_wal_filler AS SELECT g, repeat(md5(g::text), 4) AS pad"
                  " FROM generate_series(1, 800000) g")
            admin("DROP TABLE erpdoo_wal_filler")
            admin("CHECKPOINT")
            if admin("SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'erpdoo'") == "lost":
                break
        else:
            print("FAIL Postgres did not invalidate the slot")
            return 1
        print("ok   Postgres invalidated the slot")
        sh("start", "erpdoo")

        expected = {
            ("sale.order", to_confirm): ("SaleOrderConfirmed", "state", "draft", "sale", False),
            ("sale.order", to_cancel): ("SaleOrderCancelled", "state", "sale", "cancel", False),
            ("crm.lead", lead): ("LeadWon", "probability", None, 100, True),
        }

        def found(model, record_id):
            return [e for _, e in events.seen if e["model"] == model and e["record_id"] == record_id
                    and e["source"].get("backfill")]

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not all(found(*key) for key in expected):
            events.wait("-", 0, 1)  # read for a second
        events.wait("-", 0, 5)  # and a little longer, for duplicates
        first_ids = {}
        for (model, record_id), (event_type, name, old, new, approximate) in expected.items():
            got = found(model, record_id)
            change = got[0]["changes"].get(name) if len(got) == 1 else None
            if [e["event_type"] for e in got] != [event_type]:
                failures.append(f"{model} {record_id}: expected one backfilled {event_type}, got"
                                f" {[e['event_type'] for e in got]}")
            elif change != {"from": old, "to": new} or got[0]["source"]["approximate"] is not approximate:
                failures.append(f"{model} {record_id}: {event_type} with {change}, source {got[0]['source']}")
            else:
                first_ids[(model, record_id)] = got[0]["event_id"]
                print(f"ok   {event_type} for {model} {record_id} backfilled once: {name} {old} -> {new}"
                      f"{' (approximate)' if approximate else ''}")

        log = sh("logs", "--no-color", "--since", started.isoformat(), "erpdoo").stdout
        window = re.search(r"between (\S+) and (\S+) were not streamed", log)
        if not window:
            failures.append("no log line with the missed window")
        else:
            print(f"ok   erpdoo logged the window {window[1]} to {window[2]}")
            # The same backfill by hand, twice: the same ids each time.
            for run in (1, 2):
                before = len(events.seen)
                out = sh("exec", "-T", "erpdoo", "erpdoo", "backfill", "--since", window[1], "--until", window[2])
                events.wait("-", 0, 5)
                again = {(e["model"], e["record_id"]): e["event_id"] for _, e in events.seen[before:]
                         if (e["model"], e["record_id"]) in expected}
                if again != first_ids:
                    failures.append(f"backfill run {run} gave ids {again}, the first gave {first_ids}")
                else:
                    print(f"ok   erpdoo backfill run {run}: the same {len(again)} event ids"
                          f" ({out.stderr.strip().splitlines()[-1].split(': ', 1)[-1]})")
    finally:
        admin("ALTER SYSTEM RESET max_slot_wal_keep_size")
        admin("SELECT pg_reload_conf()")
        sh("start", "erpdoo")
    for failure in failures:
        print(f"FAIL {failure}")
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
