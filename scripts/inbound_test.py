"""Check inbound messages against the running compose stack, with the
shipped inbound rules (rules/inbound) and the dev stack's integration user:

- DeliveryShipped validates the order's delivery, as the integration user,
  with the tracking number; the outbound DeliveryDone event is marked with
  the message (source.inbound), and the stdout sink, which skips events
  caused by warehouse.events, leaves it out;
- PaymentReceived registers one payment on the invoice; InvoicePaid is
  marked, and stdout still has it (it skips only warehouse.events);
- the same message again is skipped, and a new message for the same
  payment is recognised as carried out already: still one payment;
- a message for an invoice that does not exist goes to the dead-letter
  topic with the reason, and Odoo is not changed;
- with Odoo stopped, a payment message waits (retried, not dead-lettered)
  and is carried out once Odoo is back, once;
- an event not caused by a message (an invoice an admin posted) is not marked.

    python scripts/inbound_test.py
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
import uuid
from datetime import date
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

sys.path.insert(0, str(Path(__file__).parent))
from smoke_test import REGISTRY, Events, Odoo, wait_until  # noqa: E402

INTEGRATION_LOGIN = "erpdoo-integration"


class Plain:
    """A plain JSON topic (the dead-letter topic), from its current end."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self.consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"inbound-test-{uuid.uuid4()}"})
        partitions = self.consumer.list_topics(topic, timeout=10).topics[topic].partitions
        self.consumer.assign([TopicPartition(topic, p, self.consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10)[1]) for p in partitions])
        self.seen: list[dict] = []

    def read(self, seconds: float) -> list[dict]:
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            msg = self.consumer.poll(0.2)
            if msg is not None and not msg.error():
                self.seen.append(json.loads(msg.value()))
        return self.seen


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--registry", default=REGISTRY)
    p.add_argument("--dead-letter", default="erpdoo.inbound.dead-letter")
    p.add_argument("--compose", default="docker compose", help="how to run docker compose")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str) -> str:
        return subprocess.run([*compose, *command], check=True, text=True, capture_output=True).stdout

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    [bot] = odoo("res.users", "search", [("login", "=", INTEGRATION_LOGIN)])
    events = Events(args.kafka, args.topic, args.registry)
    dead = Plain(args.kafka, args.dead_letter)
    producer = Producer({"bootstrap.servers": args.kafka})
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    failures: list[str] = []
    tag = uuid.uuid4().hex[:8]

    def ok(text: str) -> None:
        print(f"ok   {text}")

    def fail(text: str) -> None:
        failures.append(text)
        print(f"FAIL {text}")

    def send(topic: str, message: dict) -> None:
        producer.produce(topic, json.dumps(message).encode(), key=str(message.get("id")).encode())
        producer.flush(10)

    def within(what: str, check, timeout: float = 60) -> bool:
        try:
            wait_until(what, check, timeout)
            return True
        except SystemExit as e:
            fail(str(e))
            return False

    def event_for(model: str, record_id: int, event_type: str) -> dict | None:
        found = [e for _, e in events.wait(model, record_id, 30) if e["event_type"] == event_type]
        return found[0] if found else None

    def stdout_has(event: dict) -> bool:
        time.sleep(3)  # stdout is the last sink
        return event["event_id"] in sh("logs", "--no-color", "--since", started, "erpdoo")

    partner = odoo("res.partner", "create", {"name": f"Inbound Test {tag}", "customer_rank": 1})
    # A consumable that is not tracked in stock, so its delivery is always reserved.
    product = (odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "consu"),
                                                   ("is_storable", "=", False)], limit=1)
               or [odoo("product.product", "create", {"name": "Inbound test item", "type": "consu",
                                                      "is_storable": False, "sale_ok": True})])[0]

    def invoice(amount: float) -> tuple[int, str, float]:
        move = odoo("account.move", "create", {
            "move_type": "out_invoice", "partner_id": partner, "invoice_date": date.today().isoformat(),
            "invoice_line_ids": [(0, 0, {"product_id": product, "quantity": 1, "price_unit": amount})]})
        odoo("account.move", "action_post", [move])
        [row] = odoo("account.move", "read", [move], ["name", "amount_residual"])
        return move, row["name"], row["amount_residual"]

    def payment(name: str, amount: float, payment_id: str, message_id: str | None = None) -> dict:
        return {"id": message_id or f"evt_{uuid.uuid4().hex[:10]}", "type": "PaymentReceived", "invoice": name,
                "amount": amount, "date": date.today().isoformat(), "payment_id": payment_id}

    def payments(payment_id: str) -> int:
        return len(odoo("account.payment", "search", [("memo", "=", payment_id)]))

    def marked(event: dict | None, message: dict, topic: str, rule: str) -> bool:
        return bool(event) and event["source"].get("inbound") == {"message_id": message["id"], "topic": topic,
                                                                  "rule": rule}

    try:
        # DeliveryShipped.
        order = odoo("sale.order", "create", {"partner_id": partner, "order_line": [(0, 0, {"product_id": product})]})
        odoo("sale.order", "action_confirm", [order])
        [picking] = odoo("stock.picking", "search_read", [("sale_id", "=", order)], fields=["name"])
        shipped = {"id": f"wh-{tag}", "type": "DeliveryShipped", "reference": picking["name"],
                   "tracking_number": f"1Z{tag.upper()}"}
        send("warehouse.events", shipped)
        if within("the delivery to be validated",
                  lambda: odoo("stock.picking", "read", [picking["id"]], ["state"])[0]["state"] == "done", 60):
            [row] = odoo("stock.picking", "read", [picking["id"]], ["write_uid", "carrier_tracking_ref"])
            if row["write_uid"][0] != bot or row["carrier_tracking_ref"] != shipped["tracking_number"]:
                fail(f"delivery {picking['name']}: written by {row['write_uid']}, tracking {row['carrier_tracking_ref']}")
            else:
                ok(f"DeliveryShipped validated {picking['name']} as {INTEGRATION_LOGIN}, tracking {shipped['tracking_number']}")
        done = event_for("stock.picking", picking["id"], "DeliveryDone")
        if not marked(done, shipped, "warehouse.events", "DeliveryShipped"):
            fail(f"DeliveryDone not marked with the message: {done and done['source']}")
        else:
            ok(f"DeliveryDone carries source.inbound {done['source']['inbound']}")
            if stdout_has(done):
                fail("the stdout sink got DeliveryDone, though it skips events caused by warehouse.events")
            else:
                ok("the stdout sink (skip_inbound: [warehouse.events]) left it out")

        # PaymentReceived, and an invoice an admin posted.
        move, name, amount = invoice(120)
        posted = event_for("account.move", move, "InvoicePosted")
        if posted is None or "inbound" in posted["source"]:
            fail(f"InvoicePosted by admin: {posted and posted['source']}")
        else:
            ok("InvoicePosted, by an admin, is not marked")
        paid_message = payment(name, amount, f"pi_{tag}")
        send("payments.events", paid_message)
        if within("the invoice to be paid", lambda: odoo("account.move", "read", [move], ["payment_state"])[0][
                "payment_state"] in ("paid", "in_payment"), 60):
            ok(f"PaymentReceived paid {name}: {payments(paid_message['payment_id'])} payment")
        paid = event_for("account.move", move, "InvoicePaid")
        if not marked(paid, paid_message, "payments.events", "PaymentReceived"):
            fail(f"InvoicePaid not marked with the message: {paid and paid['source']}")
        elif not stdout_has(paid):
            fail("the stdout sink left out InvoicePaid, though it skips only warehouse.events")
        else:
            ok("InvoicePaid carries source.inbound, and stdout has it")

        # The same message again, and the same payment in a new message.
        send("payments.events", paid_message)
        again = payment(name, amount, paid_message["payment_id"])
        send("payments.events", again)
        time.sleep(8)
        log = sh("logs", "--no-color", "--since", started, "erpdoo")
        count = payments(paid_message["payment_id"])
        if count != 1:
            fail(f"{count} payments for {paid_message['payment_id']} after the repeats, expected 1")
        elif f"{paid_message['id']} is a repeat" not in log or f"{again['id']}: already carried out" not in log:
            fail("erpdoo did not report the repeat and the already carried out payment")
        else:
            ok("the same message again was skipped as a repeat, and the same payment in a new message was"
               " recognised as already carried out: still 1 payment")

        # A message for an invoice that does not exist.
        missing = payment("INV/1999/99999", 10, f"pi_missing_{tag}")
        send("payments.events", missing)
        dead.read(10)
        letters = [d for d in dead.seen if d.get("message_id") == missing["id"]]
        if len(letters) != 1 or "no account.move with name = 'INV/1999/99999'" not in letters[0]["error"]:
            fail(f"dead letters for the missing invoice: {letters}")
        else:
            ok(f"the message for a missing invoice went to {args.dead_letter}: {letters[0]['error']}")

        # Odoo stopped.
        move2, name2, amount2 = invoice(80)
        sh("stop", "odoo")
        print("ok   stopped Odoo")
        waiting = payment(name2, amount2, f"pi_down_{tag}")
        send("payments.events", waiting)
        dead.read(15)
        sh("start", "odoo")
        print("ok   started Odoo")
        wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"), 120)
        odoo = Odoo(args.odoo, args.db, "admin", "admin")
        if within("the payment sent while Odoo was down", lambda: odoo("account.move", "read", [move2], ["payment_state"])[0][
                "payment_state"] in ("paid", "in_payment"), 120):
            time.sleep(5)
            log = sh("logs", "--no-color", "--since", started, "erpdoo")
            count = payments(waiting["payment_id"])
            letters = [d for d in dead.read(1) if d.get("message_id") == waiting["id"]]
            if count != 1 or letters or "trying again" not in log:
                fail(f"after Odoo was back: {count} payments, {len(letters)} dead letters,"
                     f" retried: {'trying again' in log}")
            else:
                ok("the payment sent while Odoo was down was retried, not dead-lettered, and carried out once")
    finally:
        subprocess.run([*compose, "start", "odoo"], capture_output=True)
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
