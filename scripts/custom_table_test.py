"""Check that erpdoo finds the table of a model that sets its own _table.

Against the running compose stack: installs the test module in
tests/odoo_addons (model erpdoo.test.gadget, table erpdoo_custom_gadgets)
and runs a second erpdoo with the rule in tests/rules_custom_table, its own
slot and topic. That erpdoo must name the custom table in the SQL it asks
an admin to run; after running that SQL, activating a gadget must give a
GadgetActivated event. Uninstalls the module and removes the slot at the end.

    python scripts/custom_table_test.py
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from smoke_test import Events, Odoo, wait_until  # noqa: E402

MODULE, MODEL, TABLE = "erpdoo_custom_table", "erpdoo.test.gadget", "erpdoo_custom_gadgets"
SLOT, TOPIC, CONTAINER = "erpdoo_custom_table_test", "erpdoo.custom-table-test", "erpdoo-custom-table-test"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--admin-sql", default="docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U odoo -d odoo",
                   help="command that runs SQL from stdin as an admin")
    args = p.parse_args()

    erpdoo = ["docker", "compose", "run", "--rm", "--no-deps", "-T",
              "-v", f"{Path(__file__).parent.parent / 'tests' / 'rules_custom_table'}:/rules:ro",
              "-e", "ERPDOO_RULES=/rules", "-e", f"ERPDOO_SLOT={SLOT}", "-e", "ERPDOO_STATE=/tmp/custom.db",
              "-e", f"ERPDOO_TOPIC={TOPIC}", "-e", "ERPDOO_TEST_HOLD_ACK=", "erpdoo"]

    def admin(statements: str) -> None:
        subprocess.run(shlex.split(args.admin_sql), input=statements, text=True, check=True, capture_output=True)

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    odoo("ir.module.module", "update_list")
    [module] = odoo("ir.module.module", "search_read", [("name", "=", MODULE)], fields=["state"])
    if module["state"] != "installed":
        odoo("ir.module.module", "button_immediate_install", [module["id"]])
    print(f"ok   installed {MODULE}: model {MODEL} with _table = {TABLE}")
    failures = []
    try:
        # erpdoo's role may not publish the table: it stops and asks for SQL
        # that must name the custom table.
        first = subprocess.run([*erpdoo, "timeout", "60", "erpdoo"], capture_output=True, text=True)
        sql = [line.strip() for line in first.stderr.splitlines() if line.startswith("  ") and line.endswith(";")]
        if first.returncode != 2 or not any(TABLE in line for line in sql):
            print(first.stderr[-2000:])
            print(f"FAIL erpdoo did not ask to publish {TABLE}")
            return 1
        print(f"ok   erpdoo found the table and asked an admin for: {' '.join(sql)}")
        admin("\n".join(sql))

        run = subprocess.Popen([*erpdoo[:3], "--name", CONTAINER, *erpdoo[3:], "erpdoo"], stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        log = []
        deadline = time.monotonic() + 120
        for line in run.stderr:
            log.append(line)
            if "streaming to" in line or time.monotonic() > deadline:
                break
        # Keep reading erpdoo's log, so that a full pipe cannot block it.
        threading.Thread(target=lambda: [log.append(line) for line in run.stderr], daemon=True).start()
        found = [line for line in log if f"model {MODEL} uses table {TABLE}" in line]
        print("ok   " + found[0].split(": ", 1)[1].strip() if found else "FAIL no log line about the found table")
        if not found:
            failures.append("log line")

        # The same table from tables: in erpdoo.yml, instead of by its columns.
        config = Path(__file__).parent.parent / "tests" / "custom_table_erpdoo.yml"
        mapped = subprocess.run([*erpdoo[:-1], "-v", f"{config}:/override.yml:ro", "-e", "ERPDOO_CONFIG=/override.yml",
                                 "erpdoo", "python", "-m", "erpdoo.models", MODEL], capture_output=True, text=True)
        if f"{MODEL} -> table {TABLE}," not in mapped.stdout or "found by its columns" in mapped.stdout:
            failures.append(f"tables: in erpdoo.yml not used: {mapped.stdout[-500:]}{mapped.stderr[-500:]}")
        else:
            print(f"ok   with tables: in erpdoo.yml, {MODEL} -> {TABLE} without detection")

        events = Events(args.kafka, TOPIC)
        partner = odoo("res.partner", "search", [("customer_rank", ">", 0)], limit=1)[0]
        partner_name = odoo("res.partner", "read", [partner], ["name"])[0]["name"]
        name = f"Gadget {uuid.uuid4().hex[:6]}"
        gadget = odoo(MODEL, "create", {"name": name, "partner_id": partner})
        odoo(MODEL, "write", [gadget], {"state": "active"})
        got = [e for _, e in events.wait(MODEL, gadget, 30)]
        expected = {"id": gadget, "name": name, "state": "active", "partner_id": {"id": partner, "name": partner_name}}
        if [e["event_type"] for e in got] != ["GadgetActivated"] or got[0]["record"] != expected:
            failures.append(f"expected one GadgetActivated with {expected}, got {json.dumps(got)}")
        else:
            print(f"ok   GadgetActivated for {name}, source table {got[0]['source']['table']}")
        run.terminate()
        run.wait(60)
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
        admin(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{SLOT}';")
        odoo("ir.module.module", "button_immediate_uninstall", [module["id"]])
        print(f"     uninstalled {MODULE} and dropped slot {SLOT}")
    for failure in failures:
        print(f"FAIL {failure}")
    print("OK" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
