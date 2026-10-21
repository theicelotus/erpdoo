"""Check polling mode against the running compose stack, next to replication mode.

Runs a second erpdoo in polling mode (tests/polling_erpdoo.yml), which reads
the dev stack's Odoo through its API only, as the read-only erpdoo-reader
user, with no way to reach Postgres, and writes to its own topics
(erpdoo.polling-test, .avro) under its own schema namespaces. Then:

- drives the smoke test's scenarios through Odoo, one poll at a time, and
  compares the polled events with the ones the dev stack's erpdoo streams
  from replication: the same event types for the same records, with the
  same transitions and records, apart from the expected differences below.
  Every polled event must also fit its registered schema in both formats,
  the two must agree, and its fields must equal Odoo's (event_checks.py);
- stops the polling erpdoo in the middle: an order confirmed and an invoice
  posted meanwhile must give their events once it is back, and nothing sent
  before may come again;
- wins a lead in a long transaction, which commits after the polling
  erpdoo's cursor has passed its write_date (Odoo's time of its start);
  reading the overlap again must find it;
- pauses it while an invoice is posted and paid and a lead is won and lost
  again, so that all of it falls between two polls: both invoice events
  must still come, from Odoo's field tracking, while the lead's win is lost
  (Odoo does not track its probability);
- has it carry out an inbound message, whose event it must mark;
- deletes a lead, which a delete rule must report;
- checks that its registered schemas equal replication mode's, that the
  viewer shows its polls, that it logs its API calls, that the reader user
  can only read, and that it never connected to Postgres.

Expected differences, allowed and listed in the output:

- occurred_at is Odoo's time to the second (write_date or the tracking
  message), not the commit time; event ids and source differ by design;
- approximate events (fields Odoo does not track: LeadWon) take "from"
  from the previous poll;
- fields Odoo does not track, of a record changed again before the poll,
  are as of the poll;
- a state of an untracked field between two polls gives no event;
- only the erpdoo that carried out an inbound message marks its event.

Removes its container, state file, topics and schema subjects at the end.

    python scripts/polling_test.py
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from collections import Counter
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).parent))
from event_checks import OdooCheck, SchemaCheck, same_event  # noqa: E402
from smoke_test import REGISTRY, Events, Odoo, wait_until  # noqa: E402

ROOT = Path(__file__).parent.parent
CONTAINER, TOPIC, NAMESPACE = "erpdoo-polling-test", "erpdoo.polling-test", "erpdoo.polling_test"
STATE = "/var/lib/erpdoo/polling-test.db"
VIEWER = "http://127.0.0.1:8001"
INBOUND_PREFIX = "polling-test."
READER_LOGIN, READER_KEY = "erpdoo-reader", "dev-only-reader-key-not-secret"
INTERVAL = 2  # tests/polling_erpdoo.yml
POLLED = 2 * INTERVAL + 2  # seconds after which a change has surely been polled
LONG = 15  # seconds the long transaction stays open, past several polls (the overlap is 60)
DELETE_RULE = """# Only for the polling test: deletes are found by comparing ids.
LeadDeleted:
  model: crm.lead
  on: [delete]
  fields: [name, type, probability]
"""
# Differ by design: ids from another source, Odoo's times to the second.
BY_DESIGN = {"event_id", "occurred_at", "source"}


def normal(value):
    """A value as both modes should agree on it: decimals as numbers, times to the second."""
    if isinstance(value, str):
        if len(value) >= 19 and value[4:5] == "-" and value[10:11] in (" ", "T"):
            return value[:19]
        try:
            return Decimal(value)
        except InvalidOperation:
            return value
    if isinstance(value, float):
        return Decimal(repr(value))
    return value


def differences(a, b, path: str = "") -> list[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        return [d for k in sorted(a.keys() | b.keys()) if f"{path}.{k}".lstrip(".") not in BY_DESIGN
                for d in differences(a.get(k, "<missing>"), b.get(k, "<missing>"), f"{path}.{k}")]
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in differences(x, y, f"{path}[{i}]")]
    return [] if normal(a) == normal(b) else [f"{path.lstrip('.')}: {a!r} in replication, {b!r} polled"]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events", help="replication mode's JSON Schema topic")
    p.add_argument("--registry", default=REGISTRY)
    p.add_argument("--compose", default="docker compose", help="how to run docker compose")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str, check: bool = True) -> str:
        return subprocess.run(command, check=check, text=True, capture_output=True).stdout

    work = Path(tempfile.mkdtemp(prefix="erpdoo-polling-test-"))
    (work / "rules").mkdir()
    (work / "inbound").mkdir()
    for file in (ROOT / "rules").glob("*.yml"):
        shutil.copy(file, work / "rules")
    (work / "rules" / "polling_test.yml").write_text(DELETE_RULE)
    # The same inbound rules on topics of their own, so that the dev stack's erpdoo does not take the messages.
    for file in (ROOT / "rules" / "inbound").glob("*.yml"):
        (work / "inbound" / file.name).write_text(file.read_text().replace("  topic: ", f"  topic: {INBOUND_PREFIX}"))
    for path in (work, *work.rglob("*")):
        path.chmod(0o755 if path.is_dir() else 0o644)  # erpdoo runs as nobody
    run_erpdoo = [*compose, "run", "-d", "--no-deps", "--name", CONTAINER, "-p", "127.0.0.1:8001:8000",
                  "-v", f"{ROOT / 'tests' / 'polling_erpdoo.yml'}:/polling.yml:ro",
                  "-v", f"{work / 'rules'}:/rules:ro", "-v", f"{work / 'inbound'}:/inbound:ro",
                  "-e", "ERPDOO_CONFIG=/polling.yml", "-e", "ERPDOO_RULES=/rules", "-e", "ERPDOO_INBOUND_RULES=/inbound",
                  "-e", f"ERPDOO_STATE={STATE}", "-e", "ERPDOO_TEST_HOLD_ACK=",
                  # Polling mode must not need Postgres: nothing answers here.
                  "-e", "ERPDOO_PG_DSN=host=postgres-is-not-used-in-polling-mode dbname=odoo",
                  "erpdoo", "erpdoo"]

    def remove_state() -> None:
        sh("docker", "rm", "-f", CONTAINER, check=False)
        sh(*compose, "exec", "-T", "erpdoo", "rm", "-f", STATE, f"{STATE}-wal", f"{STATE}-shm", check=False)

    def logs(since: str | None = None) -> str:
        result = subprocess.run(["docker", "logs", *(["--since", since] if since else []), CONTAINER],
                                text=True, capture_output=True)
        return result.stdout + result.stderr

    def wait_started(since: str | None = None) -> None:
        def started():
            text = logs(since)
            if "Traceback" in text or sh("docker", "inspect", "-f", "{{.State.Running}}", CONTAINER).strip() != "true":
                raise SystemExit(f"the polling erpdoo stopped:\n{text[-3000:]}")
            return " every " in text and "polling http" in text
        wait_until("the polling erpdoo", started, 240)

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    reader = Odoo(args.odoo, args.db, READER_LOGIN, READER_KEY)
    results: list[tuple[str, bool, str]] = []
    expected: list[str] = []  # expected differences seen

    def report(name: str, ok: bool, detail: str) -> None:
        results.append((name, ok, detail))
        print(f"{'ok  ' if ok else 'FAIL'} {name}: {detail}")

    remove_state()
    try:
        started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        subprocess.run(run_erpdoo, check=True, capture_output=True)
        wait_started()
        print(f"     polling erpdoo up: {logs().count('as a baseline')} models read for their baseline")
        # librdkafka's lines (the topics are deleted at the end) are left out.
        quiet = logging.getLogger("polling-test.kafka")
        quiet.addHandler(logging.NullHandler())
        quiet.propagate = False
        replicated = Events(args.kafka, args.topic, args.registry, quiet)
        polled = Events(args.kafka, TOPIC, args.registry, quiet)
        polled_avro = Events(args.kafka, f"{TOPIC}.avro", args.registry, quiet)
        schemas = SchemaCheck(polled.decode.client, args.registry)
        in_odoo = OdooCheck(odoo, str(work / "rules"))

        def deep(event: dict) -> list[str]:
            """A polled event against its schema, its Avro twin and Odoo."""
            problems = schemas.problems(event, polled.schema_ids.get(event["event_id"]), NAMESPACE)
            twin = polled_avro.find(event["event_id"])
            if twin is None:
                return problems + [f"not on {TOPIC}.avro"]
            problems += [f"avro: {p}" for p in schemas.problems(twin, polled_avro.schema_ids[twin["event_id"]],
                                                                 f"{NAMESPACE}.avro")]
            return problems + same_event(twin, event)

        def forget(model: str, record_id: int) -> None:
            for feed in (replicated, polled):
                feed.seen = [(t, e) for t, e in feed.seen if (e["model"], e["record_id"]) != (model, record_id)]

        def step(name: str, model: str, record_id: int, types: list[str], polled_types: list[str] | None = None,
                 allow: dict[str, str] | None = None, odoo_types: list[str] | None = None) -> tuple[list, list]:
            """Both modes' events for one record: replication must give ``types``,
            polling ``polled_types`` (default the same), with the same
            transitions and records, apart from the fields in ``allow``
            (path -> why, an expected difference). Polled events of
            ``odoo_types`` (default all) are compared with Odoo too: those
            the record has not changed since."""
            polled_types = types if polled_types is None else polled_types
            odoo_types = polled_types if odoo_types is None else odoo_types
            mine = [e for _, e in replicated.wait(model, record_id, 30 if types else 5)]
            theirs = [e for _, e in polled.wait(model, record_id, 30 if polled_types else POLLED + 2, settle=POLLED)]
            problems = []
            if sorted(e["event_type"] for e in mine) != sorted(types):
                problems.append(f"replication gave {[e['event_type'] for e in mine]}, expected {types}")
            if sorted(e["event_type"] for e in theirs) != sorted(polled_types):
                problems.append(f"polling gave {[e['event_type'] for e in theirs]}, expected {polled_types}")
            notes = []
            for event in theirs:
                twin = next((e for e in mine if e["event_type"] == event["event_type"]), None)
                if twin is not None:
                    for difference in differences(twin, event):
                        path = difference.split(":")[0]
                        if path in (allow or {}):
                            notes.append(f"{difference} ({allow[path]})")
                        else:
                            problems.append(difference)
                problems += deep(event) + (in_odoo.problems(event) if event["event_type"] in odoo_types else [])
            if polled_types != types:
                notes.append(f"replication gave {types}, polling {polled_types or 'nothing'}"
                             f" ({(allow or {}).get('types', 'expected')})")
            expected.extend(f"{name}: {n}" for n in notes)
            detail = "; ".join(problems) or (
                f"{', '.join(polled_types) or 'no event'} in both modes" if polled_types == types else notes[-1])
            report(name, not problems, detail + (f" ({len(notes)} expected differences)" if notes and not problems
                                                 and polled_types == types else ""))
            return mine, theirs

        # The reader user may only read.
        writable = [f"{m} {op}" for m in ("sale.order", "account.move", "stock.picking", "crm.lead", "res.partner",
                                          "product.product", "ir.model.fields", "mail.tracking.value")
                    for op in ("write", "create", "unlink") if reader(m, "has_access", [], op)]
        said = "may only read the" in logs()
        report("erpdoo-reader can only read", not writable and said,
               "erpdoo logged it, and Odoo refuses it every write, create and unlink checked" if not writable and said
               else f"may: {writable}; erpdoo's log says so: {said}")

        partner_id = odoo("res.partner", "search", [("customer_rank", ">", 0), ("is_company", "=", True)], limit=1)[0]
        goods = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "consu")], limit=1)[0]
        goods_name = odoo("product.product", "read", [goods], ["name"])[0]["name"]
        [sale_tax] = odoo("account.tax", "search_read", [("type_tax_use", "=", "sale")], fields=["name"], limit=1)
        taxed = [(6, 0, [sale_tax["id"]])]

        def new_order(qty=2, product=goods):
            return odoo("sale.order", "create", {"partner_id": partner_id, "order_line": [
                (0, 0, {"product_id": product, "product_uom_qty": qty, "tax_id": taxed})]})

        def post(move_type):
            line = {"product_id": goods, "quantity": 3, "price_unit": 100}
            if move_type == "out_invoice":
                line["tax_ids"] = taxed
            move = odoo("account.move", "create", {
                "move_type": move_type, "partner_id": partner_id, "invoice_date": date.today().isoformat(),
                "invoice_line_ids": [(0, 0, line)]})
            odoo("account.move", "action_post", [move])
            return move

        def pay(move):
            ctx = {"active_model": "account.move", "active_ids": [move]}
            wizard = odoo("account.payment.register", "create", {}, context=ctx)
            odoo("account.payment.register", "action_create_payments", [wizard], context=ctx)

        def lead(name):
            record = odoo("crm.lead", "create", {"name": name, "type": "opportunity", "partner_id": partner_id})
            time.sleep(POLLED)  # polled once before it changes, as a lead usually is
            return record

        # The smoke test's scenarios, one poll at a time.
        order = new_order()
        odoo("sale.order", "action_confirm", [order])
        step("confirm quotation", "sale.order", order, ["SaleOrderConfirmed"])
        odoo("sale.order", "write", [order], {"client_order_ref": "polling-test edit"})
        forget("sale.order", order)
        step("edit confirmed order", "sale.order", order, [])
        picking = odoo("stock.picking", "search", [("sale_id", "=", order), ("picking_type_code", "=", "outgoing")])[0]
        for move in odoo("stock.move", "search_read", [("picking_id", "=", picking)], fields=["product_uom_qty"]):
            odoo("stock.move", "write", [move["id"]], {"quantity": move["product_uom_qty"], "picked": True})
        odoo("stock.picking", "button_validate", [picking], context={"skip_sms": True})
        step("validate delivery", "stock.picking", picking, ["DeliveryDone"])
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
        step("validate receipt", "stock.picking", receipt, [])
        # Confirmed and cancelled at once, often within one poll: both
        # transitions are in Odoo's tracking.
        order2 = new_order(1)
        odoo("sale.order", "action_confirm", [order2])
        wizard = odoo("sale.order.cancel", "create", {"order_id": order2})
        odoo("sale.order.cancel", "action_cancel", [wizard])
        step("confirm and cancel at once", "sale.order", order2, ["SaleOrderConfirmed", "SaleOrderCancelled"],
             odoo_types=["SaleOrderCancelled"])

        # Restart: what happens while polling erpdoo is stopped comes after it is back, once.
        sh("docker", "stop", CONTAINER)
        print("ok   stopped the polling erpdoo")
        order3 = new_order()
        odoo("sale.order", "action_confirm", [order3])
        invoice_while_stopped = post("out_invoice")
        restarted_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        sh("docker", "start", CONTAINER)
        wait_started(restarted_at)
        resumed = "as a baseline" not in logs(restarted_at)
        print(f"ok   started it again{', it resumed from its cursor' if resumed else ''}")
        if not resumed:
            report("resume from the cursor", False, "the restarted erpdoo read a baseline again")
        step("confirmed while stopped", "sale.order", order3, ["SaleOrderConfirmed"])
        step("posted while stopped", "account.move", invoice_while_stopped, ["InvoicePosted"])
        repeated = [i for i, n in Counter(e["event_id"] for e in polled.all).items() if n > 1]
        report("restart: no gap, nothing sent again", not repeated,
               "the two changes made while it was stopped arrived, and no event from before came again"
               if not repeated else f"sent again: {repeated}")

        invoice = post("out_invoice")
        step("post customer invoice", "account.move", invoice, ["InvoicePosted"])
        pay(invoice)
        forget("account.move", invoice)
        step("register full payment", "account.move", invoice, ["InvoicePaid"])
        step("post vendor bill", "account.move", post("in_invoice"), [])
        won = lead("Polling test: won")
        odoo("crm.lead", "action_set_won", [won])
        _, won_events = step("mark opportunity won", "crm.lead", won, ["LeadWon"])
        if any(e["source"].get("approximate") is not True for e in won_events):
            report("LeadWon is approximate", False, f"sources {[e['source'] for e in won_events]}")
        else:
            expected.append("mark opportunity won: approximate (Odoo does not track probability), \"from\" is the"
                            " previous poll's")
        lost = lead("Polling test: lost")
        odoo("crm.lead", "action_set_lost", [lost])
        step("mark opportunity lost", "crm.lead", lost, [])

        # A long transaction: its write_date is when it started (now()), and
        # it commits after another change has moved the cursor past that.
        slow = lead("Polling test: won in a long transaction")
        other = lead("Polling test: won while it runs")
        hold = subprocess.Popen([*compose, "exec", "-T", "postgres", "psql", "-U", "odoo", "-d", args.db,
                                 "-v", "ON_ERROR_STOP=1", "-c",
                                 f"BEGIN; UPDATE crm_lead SET probability = 100, write_date = now() AT TIME ZONE 'UTC'"
                                 f" WHERE id = {slow}; SELECT pg_sleep({LONG}); COMMIT;"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(2)
        odoo("crm.lead", "action_set_won", [other])
        step("won while a long transaction runs", "crm.lead", other, ["LeadWon"])
        if hold.wait(LONG + 30):
            report("long transaction", False, hold.stderr.read())
        step(f"won in a transaction open {LONG} s, committed after the cursor passed it", "crm.lead", slow,
             ["LeadWon"])

        # Between two polls: the polling erpdoo is paused meanwhile.
        between = lead("Polling test: won and lost between two polls")
        sh("docker", "pause", CONTAINER)
        invoice_between = post("out_invoice")
        pay(invoice_between)
        odoo("crm.lead", "action_set_won", [between])
        odoo("crm.lead", "action_set_lost", [between])
        time.sleep(2)  # replication mode streams them meanwhile
        sh("docker", "unpause", CONTAINER)
        step("post and pay between two polls", "account.move", invoice_between, ["InvoicePosted", "InvoicePaid"],
             allow={"record.amount_residual": "not tracked by Odoo: as of the poll, after the payment"},
             odoo_types=[])
        step("win and lose a lead between two polls", "crm.lead", between, ["LeadWon"], [],
             allow={"types": "the win is a state of an untracked field between two polls"})

        # An inbound message the polling erpdoo carries out.
        # A consumable not tracked in stock, so its delivery is always reserved.
        shippable = (odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "consu"),
                                                        ("is_storable", "=", False)], limit=1)
                    or [odoo("product.product", "create", {"name": "Polling test item", "type": "consu",
                                                           "is_storable": False, "sale_ok": True})])[0]
        order4 = new_order(1, shippable)
        odoo("sale.order", "action_confirm", [order4])
        step("confirm order to ship", "sale.order", order4, ["SaleOrderConfirmed"])
        [shipment] = odoo("stock.picking", "search_read", [("sale_id", "=", order4)], fields=["name"])
        message = {"id": f"wh-{uuid.uuid4().hex[:8]}", "type": "DeliveryShipped", "reference": shipment["name"],
                   "tracking_number": "1ZPOLLING"}
        producer = Producer({"bootstrap.servers": args.kafka})
        producer.produce(f"{INBOUND_PREFIX}warehouse.events", json.dumps(message).encode(),
                         key=message["id"].encode())
        producer.flush(10)
        wait_until("the polling erpdoo to validate the delivery",
                   lambda: odoo("stock.picking", "read", [shipment["id"]], ["state"])[0]["state"] == "done", 60)
        mine, theirs = step("inbound message carried out in polling mode", "stock.picking", shipment["id"],
                            ["DeliveryDone"])
        marker = {"message_id": message["id"], "topic": f"{INBOUND_PREFIX}warehouse.events", "rule": "DeliveryShipped"}
        ok = [e["source"].get("inbound") for e in theirs] == [marker] and [
            "inbound" in e["source"] for e in mine] == [False]
        report("inbound marker in polling mode", ok,
               f"marked {marker}; replication mode's erpdoo, which did not carry it out, does not mark it" if ok
               else f"polled {[e['source'] for e in theirs]}, replication {[e['source'] for e in mine]}")
        if ok:
            expected.append("inbound message: only the erpdoo that carried it out marks its event")

        # A delete, found by comparing ids (deletes: 10 in tests/polling_erpdoo.yml).
        gone = lead("Polling test: deleted")
        odoo("crm.lead", "unlink", [gone])
        found = [e for _, e in polled.wait("crm.lead", gone, 40)]
        ok = [e["event_type"] for e in found] == ["LeadDeleted"] and found[0]["record"]["name"] == "Polling test: deleted"
        problems = deep(found[0]) if ok else []
        report("delete found by comparing ids", ok and not problems,
               "; ".join(problems) or (f"LeadDeleted for lead {gone}, with its last polled values" if ok
                                       else f"got {found}"))

        # The schemas registered in polling mode are replication mode's.
        types = sorted({e["event_type"] for e in polled.all} - {"LeadDeleted"})
        unlike = []
        for event_type in types:
            for theirs_ns, mine_ns in ((NAMESPACE, "erpdoo"), (f"{NAMESPACE}.avro", "erpdoo.avro")):
                pair = [json.loads(json.load(urllib.request.urlopen(
                    f"{args.registry}/subjects/{ns}.{event_type}/versions/latest", timeout=10))["schema"])
                    for ns in (mine_ns, theirs_ns)]
                for schema in pair:
                    schema.pop("title", None)
                    schema.pop("namespace", None)
                if pair[0] != pair[1]:
                    unlike.append(f"{theirs_ns}.{event_type}")
        report("schemas equal replication mode's", not unlike,
               f"{len(types)} event types, JSON Schema and Avro" if not unlike else f"differ: {unlike}")

        # Every polled event decodes and fits its schema, in both formats.
        polled.wait("-", 0, 1)
        invalid = [f"{e['event_type']} {e['event_id']}: {p}" for feed, ns in ((polled, NAMESPACE),
                                                                            (polled_avro, f"{NAMESPACE}.avro"))
                   for e in feed.all for p in schemas.problems(e, feed.schema_ids[e["event_id"]], ns)]
        report("every polled event fits its registered schema, in both formats", not invalid,
               f"{len(polled.all)} JSON Schema and {len(polled_avro.all)} Avro events" if not invalid
               else "; ".join(invalid[:5]))

        # The viewer shows polls, and the log their API calls.
        items = json.load(urllib.request.urlopen(f"{VIEWER}/recent", timeout=10))
        status = json.load(urllib.request.urlopen(f"{VIEWER}/slot", timeout=10))
        shown = [i for i in items if i.get("label", "").startswith("poll ") and i["events"]]
        report("viewer shows the polls", bool(shown) and status["text"].startswith("polling every"),
               f"{len(shown)} polls with events; status: {status['text']}")
        text = logs(started_at)
        calls = [int(line.split(" API calls in ")[0].rsplit(", ", 1)[1]) for line in text.splitlines()
                 if " API calls in " in line]
        report("API calls per poll logged", bool(calls),
               f"{len(calls)} polls logged with changes, {min(calls)} to {max(calls)} API calls each" if calls
               else "no poll line with its API calls")

        # It never connected to Postgres.
        address = sh("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                      CONTAINER).strip()
        connections = sh(*compose, "exec", "-T", "postgres", "psql", "-U", "odoo", "-d", "postgres", "-Atc",
                         f"SELECT count(*) FROM pg_stat_activity WHERE client_addr = '{address}'").strip()
        report("no Postgres access", connections == "0" and "psycopg2" not in text,
               f"no connection from {address}, and its Postgres address does not exist")
    finally:
        sh("docker", "unpause", CONTAINER, check=False)
        remove_state()
        for topic in (TOPIC, f"{TOPIC}.avro", f"{TOPIC}.inbound.dead-letter", f"{INBOUND_PREFIX}warehouse.events",
                      f"{INBOUND_PREFIX}payments.events"):
            sh(*compose, "exec", "-T", "redpanda", "rpk", "topic", "delete", topic, check=False)
        sh(*compose, "exec", "-T", "redpanda", "rpk", "group", "delete", "erpdoo-polling-test", check=False)
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
        shutil.rmtree(work, ignore_errors=True)
        print("     removed the polling erpdoo, its state, topics and schema subjects")
    for note in expected:
        print(f"     expected difference: {note}")
    failed = [name for name, ok, _ in results if not ok]
    print("OK" if not failed else f"FAIL: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
