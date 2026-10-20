"""End-to-end check of the shipped event packs against the running compose stack.

Drives Odoo through its API the way the UI buttons do (confirm, cancel, post,
register payment, validate delivery, mark won) and checks that exactly one
matching event arrives on Kafka for each, with related records that match
Odoo's own data. Also checks actions that must not produce an event.

    python scripts/smoke_test.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import uuid
from datetime import date
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


class Events:
    """Collects events from the topic, starting at its current end."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self.consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"smoke-{uuid.uuid4()}",
                                  "enable.auto.commit": False})

        def topic_partitions():
            meta = self.consumer.list_topics(topic, timeout=10).topics[topic]
            return meta.partitions if meta.error is None else None

        # erpdoo creates the topic when it starts.
        partitions = wait_until(f"topic {topic}", topic_partitions)
        self.consumer.assign([TopicPartition(topic, pid, self.consumer.get_watermark_offsets(
            TopicPartition(topic, pid), timeout=10)[1]) for pid in partitions])
        self.seen: list[tuple[float, dict]] = []

    def wait(self, model: str, record_id: int, timeout: float, settle: float = 3.0) -> list[tuple[float, dict]]:
        """Events for one record; after the first arrives, wait ``settle``
        seconds more to catch duplicates."""
        deadline = time.monotonic() + timeout

        def matches():
            return [(t, e) for t, e in self.seen if e["model"] == model and e["record_id"] == record_id]

        if matches():
            deadline = min(deadline, time.monotonic() + settle)
        while time.monotonic() < deadline:
            msg = self.consumer.poll(0.2)
            if msg is None or msg.error():
                continue
            self.seen.append((time.monotonic(), json.loads(msg.value())))
            if matches():
                deadline = min(deadline, time.monotonic() + settle)
        return matches()


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
    events = Events(args.kafka, args.topic)
    results: list[tuple[str, bool, str]] = []

    def check(name: str, model: str, record_id: int, event_type: str | None, since: float, verify=None):
        """Expect exactly one event_type event for the record (or none if
        event_type is None), then run verify(event) -> list of problems."""
        found = events.wait(model, record_id, args.timeout if event_type else 5)
        types = [e["event_type"] for _, e in found]
        if event_type is None:
            ok, detail = not found, f"no event" if not found else f"unexpected {types}"
        elif types != [event_type]:
            ok, detail = False, f"expected one {event_type}, got {types}"
        else:
            arrived, event = found[0]
            problems = verify(event) if verify else []
            ok = not problems
            detail = f"{arrived - since:.2f}s" if ok else "; ".join(problems)
            if not ok:
                print(json.dumps(event, indent=2))
        results.append((name, ok, detail))
        print(f"{'ok  ' if ok else 'FAIL'} {name}: {detail}")

    def name_of(model, record_id):
        return odoo(model, "read", [record_id], ["name"])[0]["name"]

    partner_id = odoo("res.partner", "search", [("customer_rank", ">", 0)], limit=1)[0]
    partner_name = name_of("res.partner", partner_id)
    goods = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "consu")], limit=1)[0]
    goods_name = name_of("product.product", goods)

    def new_order(qty=2):
        return odoo("sale.order", "create", {
            "partner_id": partner_id, "order_line": [(0, 0, {"product_id": goods, "product_uom_qty": qty})]})

    def partner_matches(record):
        got = (record.get("partner_id") or {}).get("name")
        return [] if got == partner_name else [f"partner {got!r}, Odoo says {partner_name!r}"]

    def lines_match(lines, qty_field, qty, product_field="product_id"):
        got = [((line.get(product_field) or {}).get("name"), Decimal(line.get(qty_field) or 0)) for line in lines]
        return [] if got == [(goods_name, qty)] else [f"lines {got}, expected [({goods_name!r}, {qty})]"]

    # SaleOrderConfirmed, then an edit that must not fire again.
    order = new_order()
    odoo("sale.order", "action_confirm", [order])
    check("confirm quotation", "sale.order", order, "SaleOrderConfirmed", time.monotonic(),
          lambda e: partner_matches(e["record"]) + lines_match(e["record"]["order_line"], "product_uom_qty", 2))
    odoo("sale.order", "write", [order], {"client_order_ref": "smoke-test edit"})
    events.seen = [(t, e) for t, e in events.seen if not (e["model"] == "sale.order" and e["record_id"] == order)]
    check("edit confirmed order", "sale.order", order, None, time.monotonic())

    # DeliveryDone: validate the order's delivery (skip_sms answers the SMS opt-in prompt).
    picking = odoo("stock.picking", "search", [("sale_id", "=", order), ("picking_type_code", "=", "outgoing")])[0]
    odoo("stock.picking", "button_validate", [picking], context={"skip_sms": True})
    state = odoo("stock.picking", "read", [picking], ["state"])[0]["state"]
    check("validate delivery", "stock.picking", picking, "DeliveryDone", time.monotonic(),
          lambda e: ([] if state == "done" else [f"picking state {state}"]) + partner_matches(e["record"])
          + lines_match(e["record"]["move_ids"], "quantity", 2)
          + ([] if (e["record"]["sale_id"] or {}).get("name") == name_of("sale.order", order) else ["sale_id mismatch"]))

    # A receipt is a picking too, but not a delivery.
    [receipt_type] = odoo("stock.picking.type", "search_read", [("code", "=", "incoming")],
                          fields=["default_location_src_id", "default_location_dest_id"], limit=1)
    src, dest = receipt_type["default_location_src_id"][0], receipt_type["default_location_dest_id"][0]
    uom = odoo("product.product", "read", [goods], ["uom_id"])[0]["uom_id"][0]
    receipt = odoo("stock.picking", "create", {
        "picking_type_id": receipt_type["id"], "location_id": src, "location_dest_id": dest,
        "move_ids": [(0, 0, {"name": goods_name, "product_id": goods, "product_uom_qty": 1, "product_uom": uom,
                             "location_id": src, "location_dest_id": dest})]})
    odoo("stock.picking", "action_confirm", [receipt])
    odoo("stock.picking", "button_validate", [receipt], context={"skip_sms": True})
    receipt_state = odoo("stock.picking", "read", [receipt], ["state"])[0]["state"]
    check(f"validate receipt (state {receipt_state})", "stock.picking", receipt, None, time.monotonic())
    if receipt_state != "done":
        results.append(("receipt reached done", False, receipt_state))

    # SaleOrderCancelled through Odoo's cancel wizard, as the Cancel button does.
    order2 = new_order(1)
    odoo("sale.order", "action_confirm", [order2])
    wizard = odoo("sale.order.cancel", "create", {"order_id": order2})
    odoo("sale.order.cancel", "action_cancel", [wizard])
    events.seen = [(t, e) for t, e in events.seen if not (e["model"] == "sale.order" and e["record_id"] == order2)]
    since = time.monotonic()
    found = events.wait("sale.order", order2, args.timeout)
    cancelled = [e for _, e in found if e["event_type"] == "SaleOrderCancelled"]
    ok = len(cancelled) == 1 and cancelled[0]["changes"]["state"] == {"from": "sale", "to": "cancel"}
    results.append(("cancel order", ok, "" if ok else f"got {[e['event_type'] for _, e in found]}"))
    print(f"{'ok  ' if ok else 'FAIL'} cancel order: {'from sale to cancel' if ok else results[-1][2]}")

    # InvoicePosted, then InvoicePaid after registering the full payment.
    def post(move_type):
        move = odoo("account.move", "create", {
            "move_type": move_type, "partner_id": partner_id, "invoice_date": date.today().isoformat(),
            "invoice_line_ids": [(0, 0, {"product_id": goods, "quantity": 3, "price_unit": 100})]})
        odoo("account.move", "action_post", [move])
        return move

    invoice = post("out_invoice")
    check("post customer invoice", "account.move", invoice, "InvoicePosted", time.monotonic(),
          lambda e: partner_matches(e["record"]) + lines_match(e["record"]["invoice_line_ids"], "quantity", 3))
    ctx = {"active_model": "account.move", "active_ids": [invoice]}
    payment = odoo("account.payment.register", "create", {}, context=ctx)
    odoo("account.payment.register", "action_create_payments", [payment], context=ctx)
    events.seen = [(t, e) for t, e in events.seen if not (e["model"] == "account.move" and e["record_id"] == invoice)]
    check("register full payment", "account.move", invoice, "InvoicePaid", time.monotonic(),
          lambda e: partner_matches(e["record"])
          + ([] if e["changes"]["payment_state"]["to"] in ("paid", "in_payment") else [f"{e['changes']}"]))
    check("post vendor bill", "account.move", post("in_invoice"), None, time.monotonic())

    # LeadWon, and a lost lead that must not fire.
    def lead(name):
        return odoo("crm.lead", "create", {"name": name, "type": "opportunity", "partner_id": partner_id})

    won = lead("Smoke test: won")
    odoo("crm.lead", "action_set_won", [won])
    check("mark opportunity won", "crm.lead", won, "LeadWon", time.monotonic(),
          lambda e: partner_matches(e["record"])
          + ([] if (e["record"]["stage_id"] or {}).get("name") == "Won" else [f"stage {e['record']['stage_id']}"]))
    lost = lead("Smoke test: lost")
    odoo("crm.lead", "action_set_lost", [lost])
    check("mark opportunity lost", "crm.lead", lost, None, time.monotonic())

    failed = [name for name, ok, _ in results if not ok]
    print("OK" if not failed else f"FAIL: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
