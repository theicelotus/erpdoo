"""Create the "odoo" database with demo data and the apps the shipped event
packs use, and the integration user erpdoo's inbound rules act as, unless
that is done already.

Running odoo -i again on installed apps reloads their data and rewrites
records (every CRM lead, for one), which looks like real changes to erpdoo
and its backfill. Docker Compose starts this container again whenever it
starts erpdoo, so it checks first, and installs only the apps missing.
"""

import os
import subprocess
import sys

import psycopg2

APPS = ["sale_management", "sale_stock", "crm", "stock_delivery"]
ODOO = ["/entrypoint.sh", "odoo"]

# The integration user (see "Inbound" in the README): an internal user with
# only the rights the shipped inbound rules need, and a fixed API key that
# docker-compose.yml gives erpdoo. Dev stack only, like admin/admin.
INTEGRATION_LOGIN = "erpdoo-integration"
DEV_API_KEY = "dev-only-integration-key-not-secret"
CREATE_USER = f"""
from odoo.addons.base.models.res_users import INDEX_SIZE, KEY_CRYPT_CONTEXT
groups = [env.ref(x).id for x in ("base.group_user", "stock.group_stock_user", "account.group_account_invoice")]
user = env["res.users"].with_context(no_reset_password=True).create({{
    "name": "erpdoo integration", "login": "{INTEGRATION_LOGIN}", "groups_id": [(6, 0, groups)]}})
# What res.users.apikeys._generate does, with a known key instead of a random one.
env.cr.execute("INSERT INTO res_users_apikeys (name, user_id, scope, expiration_date, key, index)"
               " VALUES (%s, %s, NULL, NULL, %s, %s)",
               ["erpdoo (dev stack)", user.id, KEY_CRYPT_CONTEXT.hash("{DEV_API_KEY}"), "{DEV_API_KEY}"[:INDEX_SIZE]])
env.cr.commit()
print("created integration user", user.login, user.id)
"""


def query(sql: str, params=()):
    try:
        conn = psycopg2.connect(host=os.environ["HOST"], user=os.environ["USER"], password=os.environ["PASSWORD"],
                                dbname="odoo")
    except psycopg2.OperationalError:
        return None  # no database yet
    with conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ir_module_module') IS NOT NULL")
        if not cur.fetchone()[0]:
            return None
        cur.execute(sql, params)
        return cur.fetchall()


installed = {name for (name,) in query("SELECT name FROM ir_module_module WHERE state = 'installed'") or []}
missing = [app for app in APPS if app not in installed]
if missing:
    print(f"installing {', '.join(missing)}", flush=True)
    subprocess.run([*ODOO, "-d", "odoo", "-i", ",".join(missing), "--stop-after-init", "--no-http"], check=True)
else:
    print(f"apps already installed: {', '.join(APPS)}", flush=True)
if not query("SELECT 1 FROM res_users WHERE login = %s", [INTEGRATION_LOGIN]):
    subprocess.run([*ODOO, "shell", "-d", "odoo", "--no-http"], input=CREATE_USER, text=True, check=True)
sys.exit(0)
