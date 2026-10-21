"""Check that an event sent again after a crash is the same event.

Against the running compose stack: erpdoo delivers a SaleOrderConfirmed
event but, through its test hook (ERPDOO_TEST_HOLD_ACK), does not confirm
the replication slot. The customer is then renamed, erpdoo applies that to
its state store, and crashes. After Docker restarts it, Postgres streams the
confirmation again. The replayed event must be identical to the first, with
the customer's name as it was when the order was confirmed.

    python scripts/replay_test.py
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from smoke_test import Events, Odoo, wait_until  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--exec", default="docker compose exec -T erpdoo",
                   help="command prefix that runs a command where erpdoo runs")
    p.add_argument("--hold-file", default="/var/lib/erpdoo/test-hold-ack",
                   help="erpdoo's ERPDOO_TEST_HOLD_ACK")
    args = p.parse_args()

    def run(*command: str) -> None:
        subprocess.run([*shlex.split(args.exec), *command], check=True)

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    events = Events(args.kafka, args.topic)

    def confirmations(order: int) -> list[dict]:
        return [e for _, e in events.seen if e["model"] == "sale.order" and e["record_id"] == order
                and e["event_type"] == "SaleOrderConfirmed"]

    def wait_for(order: int, count: int, timeout: float) -> list[dict]:
        deadline = time.monotonic() + timeout
        while len(confirmations(order)) < count and time.monotonic() < deadline:
            msg = events.consumer.poll(0.5)
            if msg is not None and not msg.error():
                events.seen.append((time.monotonic(), json.loads(msg.value())))
        return confirmations(order)

    name = f"Replay Test {uuid.uuid4().hex[:8]}"
    partner = odoo("res.partner", "create", {"name": name, "customer_rank": 1})
    product = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "service")], limit=1)[0]

    def new_order() -> int:
        return odoo("sale.order", "create", {"partner_id": partner, "order_line": [(0, 0, {"product_id": product})]})

    order = new_order()
    run("touch", args.hold_file)
    try:
        odoo("sale.order", "action_confirm", [order])
        first = wait_for(order, 1, 30)
        if len(first) != 1:
            print(f"FAIL expected one event before the crash, got {len(first)}")
            return 1
        print(f"ok   delivered {first[0]['event_id']} without confirming the slot")
        odoo("res.partner", "write", [partner], {"name": f"{name} (renamed)"})
        time.sleep(3)  # erpdoo applies the rename to its store meanwhile
    finally:
        run("rm", "-f", args.hold_file)
    print("ok   renamed the customer, then made erpdoo crash")

    # Docker restarts erpdoo, and the slot streams the confirmation again.
    found = wait_for(order, 2, 120)
    results = []
    if len(found) < 2:
        results.append(f"no replayed event within 120 s (got {len(found)})")
    else:
        replayed = found[1]
        got = replayed["record"]["partner_id"]["name"]
        print(f"     replayed {replayed['event_id']} with customer {got!r}")
        if replayed["event_id"] != first[0]["event_id"]:
            results.append("the replayed event has another event_id")
        if got != name:
            results.append(f"the replayed event shows customer {got!r}, expected {name!r} as at confirmation")
        if replayed != first[0]:
            results.append("the replayed event differs from the first: "
                           + json.dumps({"first": first[0], "replayed": replayed}, indent=2))
    # A confirmation after the rename shows the new name.
    later = new_order()
    odoo("sale.order", "action_confirm", [later])
    [after] = wait_for(later, 1, 30) or [None]
    renamed = after and after["record"]["partner_id"]["name"]
    if renamed != f"{name} (renamed)":
        results.append(f"a later order shows customer {renamed!r}")
    else:
        print(f"ok   a later order shows the new name {renamed!r}")

    for problem in results:
        print(f"FAIL {problem}")
    print("OK" if not results else "FAIL")
    return 1 if results else 0


if __name__ == "__main__":
    sys.exit(main())
