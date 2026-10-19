-- A custom default that calls an untrusted function must not be silently
-- replaced. The operator must inspect it before retrying the upgrade.
\set VERBOSITY terse
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq VERSION '1.13.0';
CREATE FUNCTION pgmq.now() RETURNS timestamptz
LANGUAGE sql AS $$ SELECT pg_catalog.now() $$;
SET search_path = pgmq, pg_catalog;
SELECT pgmq.create('custom_default_q');
RESET search_path;
ALTER TABLE pgmq.q_custom_default_q ALTER COLUMN enqueued_at
  SET DEFAULT pgmq.now() + '1 day'::pg_catalog.interval;
ALTER EXTENSION pgmq UPDATE TO '1.13.1';
SELECT extversion AS version_after_rejected_upgrade
  FROM pg_catalog.pg_extension WHERE extname = 'pgmq';
SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS preserved_custom_default
  FROM pg_catalog.pg_attrdef d
  JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
 WHERE d.adrelid = 'pgmq.q_custom_default_q'::pg_catalog.regclass
   AND a.attname = 'enqueued_at';
ALTER TABLE pgmq.q_custom_default_q ALTER COLUMN enqueued_at
  SET DEFAULT pg_catalog.now() + '1 day'::pg_catalog.interval;
ALTER EXTENSION pgmq UPDATE TO '1.13.1';
SELECT extversion AS version_after_repair
  FROM pg_catalog.pg_extension WHERE extname = 'pgmq';
SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS preserved_safe_default
  FROM pg_catalog.pg_attrdef d
  JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
 WHERE d.adrelid = 'pgmq.q_custom_default_q'::pg_catalog.regclass
   AND a.attname = 'enqueued_at';
