-- A writable pre-existing pgmq schema must not let an attacker-defined
-- pgmq.now() become a queue-table default when pgmq.create runs after install.
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE SCHEMA IF NOT EXISTS pgmq;
CREATE TABLE public.pgmq_install_canary (via text);
CREATE ROLE pgmq_install_attacker;
GRANT CREATE, USAGE ON SCHEMA pgmq TO pgmq_install_attacker;
GRANT INSERT ON public.pgmq_install_canary TO pgmq_install_attacker;
SET ROLE pgmq_install_attacker;
CREATE FUNCTION pgmq.now() RETURNS timestamptz LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_install_canary VALUES ('pgmq.now');
  RETURN pg_catalog.now();
END $f$;
RESET ROLE;

SET search_path = public, pg_catalog;
CREATE EXTENSION pgmq;
SHOW search_path;
SELECT pgmq.create('install_shadow_q');
-- Exercise the queue table's now() default, which is resolved at table
-- creation and would call the planted function on an unprotected install.
SELECT pgmq.send('install_shadow_q', '{}'::jsonb);
SELECT count(*) AS canary_hits FROM public.pgmq_install_canary;
RESET search_path;

DROP EXTENSION pgmq CASCADE;
DROP TABLE public.pgmq_install_canary;
DROP SCHEMA pgmq CASCADE;
DROP ROLE pgmq_install_attacker;
