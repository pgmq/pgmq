-- Manual upgrade regression: install the v1.13.0 base SQL alongside the
-- current extension scripts, then run pg_regress with test/manual as inputdir.
-- A queue creator can add an overload in pgmq before a privileged extension
-- upgrade. The trigger migration reads a varchar queue name from the throttle
-- table; its internal call must select pgmq.format_table_name(text, text).
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq VERSION '1.13.0';
SELECT pgmq.create('upgrade_shadow_q');
SELECT pgmq.enable_notify_insert('upgrade_shadow_q');

CREATE TABLE public.pgmq_upgrade_canary (hit boolean);
CREATE ROLE pgmq_upgrade_attacker;
GRANT CREATE, USAGE ON SCHEMA pgmq TO pgmq_upgrade_attacker;
GRANT INSERT ON public.pgmq_upgrade_canary TO pgmq_upgrade_attacker;
SET ROLE pgmq_upgrade_attacker;
CREATE FUNCTION pgmq.format_table_name(varchar, text) RETURNS text
LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_upgrade_canary VALUES (true);
  RETURN pgmq.format_table_name($1::pg_catalog.text, $2);
END $f$;
CREATE FUNCTION pgmq.format(text, text) RETURNS text
LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_upgrade_canary VALUES (true);
  RETURN pg_catalog.format($1, $2);
END $f$;
RESET ROLE;

ALTER EXTENSION pgmq UPDATE TO '1.13.1';
SELECT count(*) AS canary_hits FROM public.pgmq_upgrade_canary;

-- A no-op wrapper call must not promote SET LOCAL to a session setting.
SET search_path = public;
BEGIN;
SET LOCAL search_path = pg_catalog;
SELECT pgmq.convert_archive_partitioned('missing_upgrade_q');
COMMIT;
SHOW search_path;

DROP FUNCTION pgmq.format_table_name(varchar, text);
DROP FUNCTION pgmq.format(text, text);
DROP TABLE public.pgmq_upgrade_canary;
REVOKE CREATE, USAGE ON SCHEMA pgmq FROM pgmq_upgrade_attacker;
DROP ROLE pgmq_upgrade_attacker;
DROP EXTENSION pgmq CASCADE;
