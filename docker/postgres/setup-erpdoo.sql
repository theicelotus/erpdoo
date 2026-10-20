-- Prepares Odoo's database for erpdoo, so that erpdoo's own role needs no
-- more than REPLICATION, CONNECT and SELECT on the tables it streams.
-- Runs as an admin (the "odoo" superuser in the dev stack) once Odoo's
-- tables exist, on every `docker compose up`; each step can run again.
--
-- The tables are the ones the shipped rules need. erpdoo checks them at
-- startup and, if any are missing, prints the SQL to add them.

\set ON_ERROR_STOP on
\set tables 'account_move, account_move_line, crm_lead, crm_stage, ir_model, ir_model_fields, product_product, product_template, res_partner, sale_order, sale_order_line, stock_move, stock_picking, stock_picking_type'

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

-- Tables with rules: Postgres sends the whole previous row with each
-- update, so a rule sees the value a field is leaving.
ALTER TABLE account_move REPLICA IDENTITY FULL;
ALTER TABLE crm_lead REPLICA IDENTITY FULL;
ALTER TABLE sale_order REPLICA IDENTITY FULL;
ALTER TABLE stock_picking REPLICA IDENTITY FULL;

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
