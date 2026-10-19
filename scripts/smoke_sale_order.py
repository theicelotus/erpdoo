"""End-to-end check against the running compose stack.

Creates a quotation in Odoo, confirms it (the same action_confirm the
"Confirm" button calls), and waits for exactly one SaleOrderConfirmed event on
Kafka. Then edits the confirmed order and checks no second event appears.

    python scripts/smoke_sale_order.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import uuid
from decimal import Decimal

from confluent_kafka import Consumer, TopicPartition


class Odoo:
    def __init__(self, url: str, db: str, login: str, password: str) -> None:
        self.url = url.rstrip("/") + "/jsonrpc"
        self.db = db
        self.password = password
        self.uid = self._call("common", "login", db, login, password)
        if not self.uid:
            raise SystemExit("Odoo login failed")

    def _call(self, service: str, method: str, *args):
        body = json.dumps({"jsonrpc": "2.0", "method": "call", "params": {
            "service": service, "method": method, "args": args}}).encode()
        req = urllib.request.Request(self.url, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            reply = json.load(resp)
        if "error" in reply:
            raise RuntimeError(reply["error"]["data"]["message"])
        # Odoo leaves "result" out when the method returns None.
        return reply.get("result")

    def __call__(self, model: str, method: str, *args, **kwargs):
        return self._call("object", "execute_kw", self.db, self.uid, self.password, model, method, args, kwargs)


def wait_until(what: str, check, timeout: float = 120):
    """Retry check() until it returns a truthy value, e.g. while the stack starts."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = check()
            if result:
                return result
        except Exception as e:
            if time.monotonic() > deadline:
                raise SystemExit(f"timed out waiting for {what}: {e}")
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for {what}")
        time.sleep(1)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--timeout", type=float, default=30)
    args = p.parse_args()

    wait_until("Odoo", lambda: urllib.request.urlopen(args.odoo.rstrip("/") + "/web/health", timeout=5).status == 200)
    odoo = Odoo(args.odoo, args.db, "admin", "admin")

    consumer = Consumer({"bootstrap.servers": args.kafka, "group.id": f"smoke-{uuid.uuid4()}",
                         "enable.auto.commit": False})

    def topic_partitions():
        topic = consumer.list_topics(args.topic, timeout=10).topics[args.topic]
        return topic.partitions if topic.error is None else None

    # erpdoo creates the topic when it starts.
    partitions = wait_until(f"topic {args.topic}", topic_partitions)
    # Start at the current end of each partition so only new events are seen.
    consumer.assign([TopicPartition(args.topic, pid, consumer.get_watermark_offsets(
        TopicPartition(args.topic, pid), timeout=10)[1]) for pid in partitions])

    partner_id = odoo("res.partner", "search", [("customer_rank", ">", 0)], limit=1)[0]
    product_id = odoo("product.product", "search", [("sale_ok", "=", True)], limit=1)[0]
    order_id = odoo("sale.order", "create", {
        "partner_id": partner_id,
        "order_line": [(0, 0, {"product_id": product_id, "product_uom_qty": 2})],
    })
    name = odoo("sale.order", "read", [order_id], ["name"])[0]["name"]
    print(f"created quotation {name} (id {order_id}), confirming")

    started = time.monotonic()
    odoo("sale.order", "action_confirm", [order_id])
    confirmed_at = time.monotonic()

    def events_for(order: int, wait: float) -> list[dict]:
        found = []
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            msg = consumer.poll(0.2)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            if event["model"] == "sale.order" and event["record_id"] == order:
                found.append(event)
                if not found[1:]:
                    print(f"event after {time.monotonic() - confirmed_at:.2f}s "
                          f"(confirm call took {confirmed_at - started:.2f}s), key {msg.key().decode()}")
                    print(json.dumps(event, indent=2))
                    deadline = time.monotonic() + 3  # keep listening for duplicates
        return found

    found = events_for(order_id, args.timeout)
    if len(found) != 1 or found[0]["event_type"] != "SaleOrderConfirmed":
        print(f"FAIL: expected one SaleOrderConfirmed for order {order_id}, got {len(found)}")
        return 1

    # Related records come from erpdoo's state store; they must match Odoo.
    record = found[0]["record"]
    partner_name = odoo("res.partner", "read", [partner_id], ["name"])[0]["name"]
    product_name = odoo("product.product", "read", [product_id], ["name"])[0]["name"]
    lines = record.get("order_line") or []
    problems = []
    if (record.get("partner_id") or {}).get("name") != partner_name:
        problems.append(f"partner name {record.get('partner_id')!r}, Odoo says {partner_name!r}")
    if [(line.get("product_id") or {}).get("name") for line in lines] != [product_name]:
        problems.append(f"order lines {lines!r}, expected one line for product {product_name!r}")
    elif Decimal(lines[0].get("product_uom_qty") or 0) != 2:  # numeric values are strings
        problems.append(f"line quantity {lines[0].get('product_uom_qty')!r}, expected 2")
    if problems:
        print("FAIL: " + "; ".join(problems))
        return 1

    # A later edit of the confirmed order must not produce another event.
    odoo("sale.order", "write", [order_id], {"client_order_ref": "smoke-test edit"})
    again = events_for(order_id, 5)
    if again:
        print(f"FAIL: editing the confirmed order produced {len(again)} more event(s)")
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
