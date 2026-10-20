"""WAL written per sale order confirmation, on the running dev stack.

Creates orders for one stocked product (so each confirmation also creates a
delivery), then confirms them one at a time and divides the WAL Postgres
wrote meanwhile by the number of orders. Odoo's background jobs write a
little WAL too, so the number is an average over the run.

The total depends on when checkpoints happen: the first change to a page
after one writes the whole page (a full-page image). The figure without
full-page images is stable from run to run; it comes from pg_walinspect,
which the script installs in the "postgres" database (not Odoo's) and
which needs a superuser.

    python scripts/wal_cost.py --orders 200
"""

from __future__ import annotations

import argparse
import sys
from contextlib import closing
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from smoke_test import Odoo  # noqa: E402

RULE_TABLES = ("account_move", "crm_lead", "sale_order", "stock_picking")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--admin-pg", default="host=localhost dbname=postgres user=odoo password=odoo",
                   help="a superuser connection, for pg_walinspect")
    p.add_argument("--orders", type=int, default=200)
    p.add_argument("--warmup", type=int, default=20)
    p.add_argument("--top", type=int, default=0, help="also list the relations writing the most WAL")
    args = p.parse_args()

    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    partner = odoo("res.partner", "search", [("customer_rank", ">", 0)], limit=1)[0]
    goods = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "consu")], limit=1)[0]
    orders = [odoo("sale.order", "create", {
        "partner_id": partner, "order_line": [(0, 0, {"product_id": goods, "product_uom_qty": 1})]})
        for _ in range(args.warmup + args.orders)]

    with closing(psycopg2.connect(args.admin_pg)) as conn, conn.cursor() as cur:
        conn.autocommit = True
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_walinspect")
        with closing(psycopg2.connect(args.admin_pg.replace("dbname=postgres", f"dbname={args.db}"))) as odoo_db:
            with odoo_db.cursor() as odoo_cur:
                odoo_cur.execute("SELECT relname, relreplident FROM pg_class WHERE relname = ANY(%s) ORDER BY 1",
                                 [list(RULE_TABLES)])
                print("replica identity:", ", ".join(f"{t}={r}" for t, r in odoo_cur.fetchall()))
        for order in orders[:args.warmup]:
            odoo("sale.order", "action_confirm", [order])
        cur.execute("SELECT pg_current_wal_lsn()")
        start = cur.fetchone()[0]
        for order in orders[args.warmup:]:
            odoo("sale.order", "action_confirm", [order])
        cur.execute("SELECT pg_current_wal_lsn()")
        end = cur.fetchone()[0]
        cur.execute("SELECT %s::pg_lsn - %s::pg_lsn, sum(record_size), sum(fpi_size)"
                    " FROM pg_get_wal_stats(%s, %s)", [end, start, start, end])
        total, records, fpi = (int(v) for v in cur.fetchone())
        # Each record counted once, under the relation of its first block.
        cur.execute(
            "SELECT node, sum(size) FROM ("
            "  SELECT (array_agg(relfilenode ORDER BY block_id))[1] AS node,"
            "         max(record_length) - coalesce(sum(block_fpi_length), 0) AS size"
            "  FROM pg_get_wal_block_info(%s, %s) GROUP BY start_lsn) r GROUP BY node", [start, end])
        by_node = dict(cur.fetchall())
        with closing(psycopg2.connect(args.admin_pg.replace("dbname=postgres", f"dbname={args.db}"))) as odoo_db:
            with odoo_db.cursor() as odoo_cur:
                odoo_cur.execute("SELECT relfilenode, relname FROM pg_class WHERE relfilenode = ANY(%s)",
                                 [list(by_node)])
                names = dict(odoo_cur.fetchall())
    n = args.orders
    print(f"{n} confirmations: {total / n:.0f} bytes of WAL each,"
          f" {records / n:.0f} without full-page images ({fpi / n:.0f} in full-page images)")
    rule_tables = sum(v for k, v in by_node.items() if names.get(k) in RULE_TABLES)
    print(f"  {rule_tables / n:.0f} of them on the tables of the shipped rules ({', '.join(RULE_TABLES)})")
    top = sorted(((v, names.get(k, k)) for k, v in by_node.items()), reverse=True)[:args.top]
    for size, name in top:
        print(f"  {size / n:7.0f}  {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
