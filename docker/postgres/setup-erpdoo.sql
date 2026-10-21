-- Prepares Odoo's database for erpdoo, so that erpdoo's own role needs no
-- more than REPLICATION, CONNECT and SELECT on the tables it streams.
-- Runs as an admin (the "odoo" superuser in the dev stack) once Odoo's
-- tables exist, on every `docker compose up`; each step can run again.
--
-- The tables are the ones the shipped rules need. erpdoo checks them at
-- startup and, if any are missing, prints the SQL to add them.

\set ON_ERROR_STOP on
\set tables 'account_move, account_move_line, account_move_line_account_tax_rel, account_tax, account_tax_sale_order_line_rel, crm_lead, crm_stage, ir_model, ir_model_fields, product_product, product_template, res_partner, sale_order, sale_order_line, stock_move, stock_picking, stock_picking_type'

SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'erpdoo') AS create_role \gset
\if :create_role
CREATE ROLE erpdoo WITH LOGIN REPLICATION PASSWORD 'erpdoo';
\endif
-- Dev stacks created before this script let erpdoo inherit the odoo role.
SELECT pg_has_role('erpdoo', 'odoo', 'MEMBER') AS inherits_odoo \gset
\if :inherits_odoo
REVOKE odoo FROM erpdoo;
\endif

GRANT CONNECT ON DATABASE odoo TO erpdoo;

-- Earlier dev stacks set REPLICA IDENTITY FULL on the rule tables. erpdoo
-- now takes each change's previous row from its state store, so they go
-- back to the default (the primary key), which writes less WAL.
SELECT format('ALTER TABLE %I REPLICA IDENTITY DEFAULT', relname)
  FROM pg_class
 WHERE relname IN ('account_move', 'crm_lead', 'sale_order', 'stock_picking')
   AND relnamespace = 'public'::regnamespace AND relreplident = 'f' \gexec

SELECT NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'erpdoo') AS create_publication \gset
\if :create_publication
CREATE PUBLICATION erpdoo FOR TABLE :tables;
\else
-- Older dev stacks had erpdoo create it, so erpdoo owned it.
ALTER PUBLICATION erpdoo OWNER TO CURRENT_USER;
-- Add what is missing, keeping tables added since for other apps.
SELECT format('ALTER PUBLICATION erpdoo ADD TABLE %I', t)
  FROM unnest(string_to_array(:'tables', ', ')) AS t
 WHERE NOT EXISTS (SELECT FROM pg_publication_tables p
                    WHERE p.pubname = 'erpdoo' AND p.schemaname = 'public' AND p.tablename = t) \gexec
\endif

GRANT SELECT ON :tables TO erpdoo;
