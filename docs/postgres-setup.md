# Setting up Postgres

In replication mode, erpdoo reads Odoo's Postgres through a logical
replication slot, as a role that can stream and read but never write to
Odoo's tables. This page is the one-time setup an admin does for it, and
what erpdoo checks and asks for when your rules or apps change. Hosted Odoo
(Odoo Online, Odoo.sh) usually gives no replication access; there, use
[polling mode](polling.md).

- [What erpdoo needs](#what-erpdoo-needs)
- [The SQL](#the-sql)
- [Replica identity](#replica-identity)
- [Your own rules and apps installed later](#your-own-rules-and-apps-installed-later)
- [Tables of custom models](#tables-of-custom-models)
- [The public schema](#the-public-schema)
- [Removing erpdoo](#removing-erpdoo)

## What erpdoo needs

erpdoo only needs a role with `REPLICATION`, `CONNECT` on Odoo's database
and `SELECT` on the tables it streams. It never writes to Odoo's tables.
An admin (a superuser, or the role that owns Odoo's tables) prepares the
rest once.

- Postgres must run with `wal_level=logical` (a restart), with a
  replication slot and a WAL sender to spare (`max_replication_slots`,
  `max_wal_senders`).
- If erpdoo connects from another host, `pg_hba.conf` needs an ordinary
  entry for Odoo's database; logical replication does not use the
  `replication` entries.
- erpdoo connects with `ERPDOO_PG_DSN`, a libpq connection string such as
  `host=db dbname=odoo user=erpdoo password=...`.
- Limit how much WAL the slot may hold with `max_slot_wal_keep_size` (see
  [Slot health](slot-health.md#limit-what-a-slot-can-hold)).

## The SQL

For the shipped rules, run this in Odoo's database (named `odoo` here):

```sql
CREATE ROLE erpdoo WITH LOGIN REPLICATION PASSWORD 'change-me';
GRANT CONNECT ON DATABASE odoo TO erpdoo;

-- The rules' tables, the related records they include (with the join
-- tables of many2many fields such as taxes), and ir_model /
-- ir_model_fields, which tell erpdoo when an Odoo app is installed.
CREATE PUBLICATION erpdoo FOR TABLE
  account_move, account_move_line, account_move_line_account_tax_rel, account_tax,
  account_tax_sale_order_line_rel, crm_lead, crm_stage, ir_model, ir_model_fields,
  product_product, product_template, res_partner, sale_order, sale_order_line,
  stock_move, stock_picking, stock_picking_type;

GRANT SELECT ON
  account_move, account_move_line, account_move_line_account_tax_rel, account_tax,
  account_tax_sale_order_line_rel, crm_lead, crm_stage, ir_model, ir_model_fields,
  product_product, product_template, res_partner, sale_order, sale_order_line,
  stock_move, stock_picking, stock_picking_type
  TO erpdoo;

-- For backfill: Odoo's field tracking. Of mail_message only which record
-- changed when, not the messages themselves. ir_model_data says when an
-- app installed later was installed (see "Apps installed later").
GRANT SELECT (id, model, res_id, date) ON mail_message TO erpdoo;
GRANT SELECT ON mail_tracking_value, ir_model_fields_selection, ir_model_data TO erpdoo;
```

The last two grants are for [backfill](backfill.md) and
[apps installed later](backfill.md#apps-installed-later).

erpdoo creates its replication slot itself, which `REPLICATION` allows. The
publication and slot names are set with `ERPDOO_PUBLICATION` and
`ERPDOO_SLOT` (both `erpdoo` by default). The dev stack runs the same steps
from [docker/postgres/setup-erpdoo.sql](../docker/postgres/setup-erpdoo.sql),
in a one-shot `db-setup` container that runs as a database admin would,
before erpdoo starts, so the dev stack's erpdoo also runs with a role that
can stream and read but not write.

If erpdoo's role is allowed to make these changes, for example because it
owns Odoo's tables, erpdoo makes them itself and no admin step is needed.

Inbound messages need nothing more: they act through Odoo's API as the
integration user, and erpdoo only reads Postgres' current WAL position for
them (`pg_current_wal_insert_lsn()`, open to every role). See
[Inbound](inbound.md).

## Replica identity

Tables keep their default replica identity (the primary key): erpdoo takes
the previous row of each update and delete from its
[state store](architecture.md#the-state-store), so `REPLICA IDENTITY FULL`
is not needed.

- Before publishing a table, or asking an admin to, erpdoo checks that it
  has a primary key or another replica identity: without one, Postgres
  would reject Odoo's updates and deletes on it, so erpdoo refuses to start
  and names the table.
- erpdoo does not undo `REPLICA IDENTITY FULL` set for earlier versions;
  `ALTER TABLE <table> REPLICA IDENTITY DEFAULT` gives the smaller WAL (the
  dev stack's setup script does this). How much smaller is in
  [WAL per order confirmation](performance.md#wal-per-order-confirmation).
- In exchange, erpdoo copies the rules' own tables into its state store,
  which costs disk space and a longer first copy on a large database (see
  [The state store](architecture.md#the-state-store)).

## Your own rules and apps installed later

With your own rules the list of tables differs, and you don't have to work
it out: erpdoo checks the publication at startup and, if anything is
missing, stops with a list and the SQL that fixes it:

```
ERROR erpdoo: erpdoo needs these changes in Postgres, which its role erpdoo is not allowed to make:
  not in publication erpdoo: purchase_order, purchase_order_line
  not readable by erpdoo: purchase_order, purchase_order_line
Run this SQL as an admin (a superuser, or the owner of Odoo's tables and of the publication):

  ALTER PUBLICATION erpdoo ADD TABLE public.purchase_order, public.purchase_order_line;
  GRANT SELECT ON public.purchase_order, public.purchase_order_line TO erpdoo;
```

When an Odoo app installed later activates a rule (Purchase, for the rule
above), erpdoo logs the same message instead of stopping, keeps the other
rules running, and checks again every 30 seconds; the new rule starts once
an admin has run the SQL, and the changes made since the install are
backfilled (see [Apps installed later](backfill.md#apps-installed-later)).
erpdoo only adds to a publication it did not create, so tables an admin
published for other uses stay.

## Tables of custom models

Odoo does not store a model's table name, so erpdoo uses Odoo's naming rule
(`sale.order` → `sale_order`) plus the core models known to differ
(`ir.actions.*`). For a model that sets its own `_table`, erpdoo looks for
the table no other model uses, with an `id`, whose columns include every
stored field of the model, logs which one it found, and does not guess when
two fit equally well. To be sure, or for a model it cannot find, set the
table in [erpdoo.yml](../erpdoo.yml):

```yaml
tables:
  x_custom.gadget: x_custom_gadgets
```

`python -m erpdoo.models <model>` shows how a model is mapped, and
[scripts/custom_table_test.py](../scripts/custom_table_test.py) checks both
with a small test module ([tests/odoo_addons](../tests/odoo_addons)) that
sets its own `_table`.

## The public schema

One thing the grants above do not cover: when Odoo creates a database it
runs `GRANT CREATE ON SCHEMA public TO PUBLIC`, so every role, erpdoo's
included, can create new tables of its own in `public` (not change Odoo's).
erpdoo never does. On Postgres 15 and later the database's owner also owns
`public`, so if Odoo's role owns the database you can take this away from
every other role with `REVOKE CREATE ON SCHEMA public FROM PUBLIC`.

## Removing erpdoo

If you remove erpdoo, drop its slot (`SELECT pg_drop_replication_slot('erpdoo')`),
or Postgres keeps WAL for it.
