"""Create the "odoo" database with demo data and the apps the shipped event
packs use, unless that is done already.

Running odoo -i again on installed apps reloads their data and rewrites
records (every CRM lead, for one), which looks like real changes to erpdoo
and its backfill. Docker Compose starts this container again whenever it
starts erpdoo, so it checks first.
"""

import os
import sys

import psycopg2

APPS = ["sale_management", "sale_stock", "crm"]
ODOO = ["/entrypoint.sh", "odoo", "-d", "odoo", "-i", ",".join(APPS), "--stop-after-init", "--no-http"]


def installed() -> bool:
    try:
        conn = psycopg2.connect(host=os.environ["HOST"], user=os.environ["USER"], password=os.environ["PASSWORD"],
                                dbname="odoo")
    except psycopg2.OperationalError:
        return False  # no database yet
    with conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ir_module_module') IS NOT NULL")
        if not cur.fetchone()[0]:
            return False
        cur.execute("SELECT count(*) FROM ir_module_module WHERE name = ANY(%s) AND state = 'installed'", [APPS])
        return cur.fetchone()[0] == len(APPS)


if installed():
    print(f"apps already installed: {', '.join(APPS)}", flush=True)
    sys.exit(0)
os.execv(ODOO[0], ODOO)
