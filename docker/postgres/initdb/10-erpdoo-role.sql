-- Runs once, when the Postgres data volume is first created.
-- erpdoo gets its own login with the REPLICATION attribute.
CREATE ROLE erpdoo WITH LOGIN REPLICATION PASSWORD 'erpdoo';

-- Dev stack only: inheriting the privileges of "odoo" (which owns Odoo's
-- tables) lets erpdoo create its publication and set REPLICA IDENTITY on
-- them. SET FALSE stops it from switching to the odoo role, which is a
-- superuser in this stack.
GRANT odoo TO erpdoo WITH INHERIT TRUE, SET FALSE;
