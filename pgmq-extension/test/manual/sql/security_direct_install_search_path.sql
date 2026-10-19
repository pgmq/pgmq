-- Run in a disposable empty database with:
-- psql -X -v ON_ERROR_STOP=1 -v pgmq_sql=/path/to/pgmq.sql \
--   -f test/manual/sql/security_direct_install_search_path.sql
-- This tests statement-by-statement loading of pgmq.sql. PostgreSQL's
-- CREATE EXTENSION command uses a different installation path.
\set ON_ERROR_STOP on
CREATE SCHEMA pgmq;
CREATE TABLE public.pgmq_direct_install_canary (via text);
CREATE ROLE pgmq_direct_install_attacker;
GRANT CREATE, USAGE ON SCHEMA pgmq TO pgmq_direct_install_attacker;
GRANT INSERT ON public.pgmq_direct_install_canary TO pgmq_direct_install_attacker;
SET ROLE pgmq_direct_install_attacker;
CREATE FUNCTION pgmq.now() RETURNS timestamptz LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_direct_install_canary VALUES ('pgmq.now');
  RETURN pg_catalog.now();
END $f$;
RESET ROLE;

SET search_path = pgmq, pg_catalog;
\i :pgmq_sql
SELECT current_setting('search_path') OPERATOR(pg_catalog.=)
       'pgmq, pg_catalog'::pg_catalog.text AS path_restored;
-- The pgmq.meta default was created by the installation script. Executing
-- that default reveals whether installation bound the planted pgmq.now().
INSERT INTO pgmq.meta(queue_name, is_partitioned, is_unlogged)
  VALUES ('direct_install_probe', false, false);
SELECT count(*) AS canary_hits FROM public.pgmq_direct_install_canary;

-- Remove role-owned objects before asserting, so a red run can be repeated.
RESET search_path;
DROP SCHEMA pgmq CASCADE;
REVOKE INSERT ON public.pgmq_direct_install_canary FROM pgmq_direct_install_attacker;
DROP ROLE pgmq_direct_install_attacker;
DO $assert$
BEGIN
  IF (SELECT count(*) FROM public.pgmq_direct_install_canary) <> 0 THEN
    RAISE EXCEPTION 'installation bound attacker-defined pgmq.now()';
  END IF;
END
$assert$;
DROP TABLE public.pgmq_direct_install_canary;
