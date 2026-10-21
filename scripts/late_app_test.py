"""Check that the rules of an app installed while erpdoo runs are backfilled
from the app's install, exactly once, in replication and in polling mode.

Against the running compose stack, with two erpdoos of its own next to the
dev stack's, both running the rules in tests/rules_late_app before the app
they need is installed: one in replication mode (tests/late_app_erpdoo.yml,
its own slot) and one in polling mode (tests/late_app_polling_erpdoo.yml).

- pauses the polling erpdoo, installs the test module in tests/odoo_addons
  (model erpdoo.test.ticket), and waits until the replication erpdoo asks
  an admin for the SQL that lets it stream the new table;
- before that SQL runs, opens tickets (one of them twice) and escalates one
  (Odoo tracks the state, not the priority), then unpauses the polling
  erpdoo and runs the SQL;
- once both have activated the rules, opens one more ticket;
- in each mode every change must arrive exactly once: the ones from before
  the activation as backfilled events (state from Odoo's tracking, with its
  from and to; priority approximate, "from" null), the last one streamed
  (replication) or polled, not backfilled. Each event must fit its
  registered schema, and the tracked ones must have the same ids in both
  modes;
- restarts both erpdoos, which must send nothing again.

Uninstalls the module and removes both erpdoos, the slot, the topics and
the schema subjects at the end.

    python scripts/late_app_test.py
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from event_checks import SchemaCheck  # noqa: E402
from smoke_test import REGISTRY, Events, Odoo, wait_until  # noqa: E402

ROOT = Path(__file__).parent.parent
MODULE, MODEL, TABLE = "erpdoo_late_app", "erpdoo.test.ticket", "erpdoo_test_ticket"
SLOT = "erpdoo_late_app_test"
REPLICATION, POLLING = "erpdoo-late-app-test", "erpdoo-late-app-test-polling"
TOPICS = {REPLICATION: "erpdoo.late-app-test", POLLING: "erpdoo.late-app-test.polling"}
NAMESPACES = {REPLICATION: "erpdoo.late_app_test", POLLING: "erpdoo.late_app_test.polling"}
CONFIGS = {REPLICATION: "late_app_erpdoo.yml", POLLING: "late_app_polling_erpdoo.yml"}
SETTLE = 8  # seconds to wait for duplicates once every event is in


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--registry", default=REGISTRY)
    p.add_argument("--compose", default="docker compose", help="how to run docker compose")
    p.add_argument("--admin-sql", default="docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U odoo -d odoo",
                   help="command that runs SQL from stdin as an admin")
    args = p.parse_args()
    compose = shlex.split(args.compose)

    def sh(*command: str, check: bool = True) -> str:
        return subprocess.run(command, check=check, text=True, capture_output=True).stdout

    def admin(statements: str) -> None:
        subprocess.run(shlex.split(args.admin_sql), input=statements, text=True, check=True, capture_output=True)

    def logs(container: str) -> str:
        result = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
        return result.stdout + result.stderr

    def wait_log(container: str, pattern: str, timeout: float = 180) -> re.Match:
        def found():
            text = logs(container)
            if "Traceback" in text or sh("docker", "inspect", "-f", "{{.State.Status}}", container).strip() == "exited":
                raise SystemExit(f"{container} stopped:\n{text[-3000:]}")
            return re.search(pattern, text)
        try:
            return wait_until(f"{pattern!r} in the log of {container}", found, timeout)
        except SystemExit:
            print(f"     last lines of {container}'s log:\n" + "\n".join(logs(container).splitlines()[-40:]))
            raise

    def start(container: str) -> None:
        extra = ["-e", "ERPDOO_PG_DSN=host=postgres-is-not-used-in-polling-mode dbname=odoo"] if container == POLLING else []
        subprocess.run([*compose, "run", "-d", "--no-deps", "--name", container,
                        "-v", f"{ROOT / 'tests' / CONFIGS[container]}:/late-app.yml:ro",
                        "-v", f"{ROOT / 'tests' / 'rules_late_app'}:/rules:ro",
                        "-e", "ERPDOO_CONFIG=/late-app.yml", "-e", "ERPDOO_RULES=/rules",
                        "-e", "ERPDOO_INBOUND_RULES=/no-inbound-rules", "-e", f"ERPDOO_SLOT={SLOT}",
                        "-e", "ERPDOO_STATE=/tmp/late-app-test.db", "-e", "ERPDOO_TEST_HOLD_ACK=", *extra,
                        "erpdoo", "erpdoo"], check=True, capture_output=True)

    def cleanup() -> None:
        for container in (REPLICATION, POLLING):
            sh("docker", "unpause", container, check=False)
            sh("docker", "rm", "-f", container, check=False)
        admin(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{SLOT}';")
        for topic in TOPICS.values():
            sh(*compose, "exec", "-T", "redpanda", "rpk", "topic", "delete", topic, check=False)
        try:
            subjects = json.load(urllib.request.urlopen(f"{args.registry}/subjects", timeout=10))
        except OSError:
            subjects = []
        for subject in subjects:
            if any(subject.startswith(f"{ns}.") for ns in NAMESPACES.values()):
                for query in ("", "?permanent=true"):  # a subject is soft-deleted first
                    try:
                        urllib.request.urlopen(urllib.request.Request(
                            f"{args.registry}/subjects/{subject}{query}", method="DELETE"), timeout=10)
                    except OSError:
                        pass

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    odoo("ir.module.module", "update_list")
    [module] = odoo("ir.module.module", "search_read", [("name", "=", MODULE)], fields=["state"])
    if module["state"] == "installed":  # left over by an interrupted run
        odoo("ir.module.module", "button_immediate_uninstall", [module["id"]])
    cleanup()
    failures: list[str] = []
    try:
        for container in (REPLICATION, POLLING):
            start(container)
        wait_log(REPLICATION, "streaming to")
        wait_log(POLLING, "polling http")
        for container in (REPLICATION, POLLING):
            if "rule TicketOpened skipped" not in logs(container):
                failures.append(f"{container} did not skip TicketOpened before the app was installed")
        print("ok   both erpdoos run, the ticket rules inactive: the app is not installed")
        feeds = {c: Events(args.kafka, TOPICS[c], args.registry) for c in (REPLICATION, POLLING)}
        schemas = SchemaCheck(feeds[REPLICATION].decode.client, args.registry)

        # Changes made before erpdoo streams (polls) the app's records.
        sh("docker", "pause", POLLING)
        odoo("ir.module.module", "button_immediate_install", [module["id"]])
        print(f"ok   installed {MODULE} (model {MODEL}); polling erpdoo paused")
        asked = wait_log(REPLICATION, rf"erpdoo needs these changes in Postgres.*?\n((?:  .*\n)+?)Run this SQL"
                                      rf".*?\n\n((?:  .*;\n?)+)", 120)
        sql = [line.strip() for line in asked[2].splitlines() if line.strip()]
        if not any(TABLE in line for line in sql):
            failures.append(f"erpdoo did not ask to publish {TABLE}: {sql}")
        print(f"ok   replication erpdoo waits for an admin: {' '.join(sql)}")
        tag = uuid.uuid4().hex[:6]
        partner = odoo("res.partner", "create", {"name": f"Late App Test {tag}"})

        def ticket(name: str) -> int:
            return odoo(MODEL, "create", {"name": f"{name} {tag}", "partner_id": partner})

        twice, once, escalated = ticket("Opened twice"), ticket("Opened once"), ticket("Escalated")
        for state in ("open", "done", "open"):
            odoo(MODEL, "write", [twice], {"state": state})
        odoo(MODEL, "write", [once], {"state": "open"})
        # Polling sees times to the second: a write in the second of the
        # create would look like part of it, not an approximate change.
        time.sleep(1.1)
        odoo(MODEL, "write", [escalated], {"priority": "high"})
        print(f"ok   before the SQL: opened ticket {twice} twice and {once} once, escalated {escalated}")
        sh("docker", "unpause", POLLING)
        admin("\n".join(sql))
        print("ok   ran the SQL as an admin; polling erpdoo unpaused")

        # After the activation: streamed or polled as usual.
        backfilled = wait_log(REPLICATION, rf"{re.escape(MODEL)} was created at .*: (\d+) events", 120)
        print(f"ok   replication: {backfilled[0]}")
        wait_log(POLLING, rf"polling: {re.escape(MODEL)} was created at", 180)
        wait_log(POLLING, r"poll \d+: .* [1-9]\d* events", 60)
        print("ok   polling: " + re.search(rf"{re.escape(MODEL)} was created at [^\n]*", logs(POLLING))[0])
        later = ticket("Opened later")
        odoo(MODEL, "write", [later], {"state": "open"})

        before = {(twice, "TicketOpened", "new", "open"), (twice, "TicketOpened", "done", "open"),
                  (once, "TicketOpened", "new", "open"), (escalated, "TicketEscalated", None, "high")}
        ids: dict[str, dict] = {}
        for container, feed in feeds.items():
            mode = "replication" if container == REPLICATION else "polling"

            def mine():
                return [e for e in feed.all if e["model"] == MODEL and e["record_id"] in (twice, once, escalated, later)]

            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and len(mine()) < len(before) + 1:
                feed.poll(0.5)
            end = time.monotonic() + SETTLE
            while time.monotonic() < end:
                feed.poll(0.5)
            got = mine()
            seen = []
            for e in got:
                name = next(iter(e["changes"]))
                seen.append((e["record_id"], e["event_type"], e["changes"][name]["from"], e["changes"][name]["to"]))
            expected = sorted([*before, (later, "TicketOpened", "new", "open")], key=str)
            if sorted(seen, key=str) != expected:
                failures.append(f"{mode}: expected (record, event, from, to) {expected}, got {sorted(seen, key=str)}")
                continue
            wrong = []
            for e, (record_id, event_type, old, _) in zip(got, seen):
                source = e["source"]
                after = record_id == later
                if bool(source.get("backfill")) == after:
                    wrong.append(f"{event_type} for {record_id}: backfill {source.get('backfill')}, expected {not after}")
                if not after and source["approximate"] is not (event_type == "TicketEscalated"):
                    wrong.append(f"{event_type} for {record_id}: approximate {source['approximate']}")
                if after and mode == "replication" and "xid" not in source:
                    wrong.append(f"{event_type} for {record_id} not streamed: {source}")
                problems = schemas.problems(e, feed.schema_ids[e["event_id"]], NAMESPACES[container])
                if problems:
                    wrong.append(f"{event_type} for {record_id} does not fit its schema: {problems}")
            if wrong:
                failures += [f"{mode}: {w}" for w in wrong]
                continue
            ids[mode] = {e["source"]["mail_message_id"]: e["event_id"] for e in got if "mail_message_id" in e["source"]}
            print(f"ok   {mode}: each change once, {len(before)} backfilled from before the activation"
                  f" (TicketEscalated approximate), the later one {'streamed' if mode == 'replication' else 'polled'};"
                  " each fits its schema")
        if len(ids) == 2:
            shared = ids["replication"].keys() & ids["polling"].keys()
            if len(shared) != 3 or any(ids["replication"][m] != ids["polling"][m] for m in shared):
                failures.append(f"tracked events differ in ids: replication {ids['replication']}, polling {ids['polling']}")
            else:
                print("ok   the backfilled tracked events have the same ids in both modes")

        # Nothing again after a restart.
        counts = {c: len(f.all) for c, f in feeds.items()}
        for container in (REPLICATION, POLLING):
            sh("docker", "restart", container)
        wait_until("both erpdoos started again",
                   lambda: logs(REPLICATION).count("streaming to") >= 2 and logs(POLLING).count("polling http") >= 2)
        time.sleep(SETTLE)
        for container, feed in feeds.items():
            while feed.poll(0.5) is not None:
                pass
            again = [e["event_type"] for e in feed.all[counts[container]:] if e["model"] == MODEL]
            if again:
                failures.append(f"{container} sent events again after a restart: {again}")
        if not any("after a restart" in f for f in failures):
            print("ok   restarted both: nothing sent again")
    finally:
        cleanup()
        odoo("ir.module.module", "button_immediate_uninstall", [module["id"]])
        print(f"     uninstalled {MODULE}, removed both erpdoos, slot {SLOT}, their topics and schema subjects")
    for failure in failures:
        print(f"FAIL {failure}")
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
