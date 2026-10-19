-- Security regression: search_path / unqualified built-in privilege escalation.
--
-- pgmq functions are SECURITY INVOKER. If a function pins no search_path and
-- calls a built-in unqualified, the built-in is resolved against the *caller's*
-- search_path at run time. A role that can create objects in a schema on that
-- path (e.g. public) can plant an exact-signature shadow of a built-in and have
-- it run with the caller's privileges. The insert-notification trigger is the
-- worst case: it runs implicitly with the inserting role's search_path.
--
-- This test plants shadow built-ins (and a shadow operator) in public and in
-- pgmq. A queue creator needs CREATE on pgmq, so that schema is not safe to
-- include in a function's search_path. All but the two public partition
-- wrappers pin a path with only pg_catalog and pg_temp; the canary table
-- must stay empty.

-- Stabilize output and ensure clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- A queue-creating role can create objects in the pgmq schema.
CREATE ROLE pgmq_shadow_attacker;
GRANT CREATE, USAGE ON SCHEMA public TO pgmq_shadow_attacker;
GRANT CREATE, USAGE ON SCHEMA pgmq TO pgmq_shadow_attacker;

-- Canary: any planted shadow that executes records a row here.
CREATE TABLE public.pgmq_canary (via text);
GRANT INSERT ON public.pgmq_canary TO PUBLIC;

-- Plant exact-signature shadows of the built-ins/operator pgmq calls, as the
-- unprivileged role. Each still returns the correct value so pgmq keeps working
-- whether or not the shadow is picked; only the canary reveals which ran.
SET ROLE pgmq_shadow_attacker;

CREATE FUNCTION public.make_interval(secs integer) RETURNS interval
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('make_interval');
  RETURN pg_catalog.make_interval(secs => secs::float8);
END $f$;

CREATE FUNCTION public.pg_sleep(numeric) RETURNS void
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pg_sleep');
END $f$;

CREATE FUNCTION public.to_timestamp(integer) RETURNS timestamptz
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('to_timestamp');
  RETURN pg_catalog.to_timestamp($1::float8);
END $f$;

CREATE FUNCTION public.pg_advisory_xact_lock(integer) RETURNS void
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pg_advisory_xact_lock');
  PERFORM pg_catalog.pg_advisory_xact_lock($1::bigint);
END $f$;

CREATE FUNCTION public.pgmq_shadow_mul(integer, interval) RETURNS interval
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('op_int_mul_interval');
  RETURN $1::float8 OPERATOR(pg_catalog.*) $2;
END $f$;
CREATE OPERATOR public.* (LEFTARG = integer, RIGHTARG = interval, FUNCTION = public.pgmq_shadow_mul);

-- These built-ins are referenced UNQUALIFIED in pgmq. Public shadows probe
-- the new pin, but cannot win against the base's pgmq, pg_catalog path.
CREATE FUNCTION public.lower(text) RETURNS text
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('lower');
  RETURN pg_catalog.lower($1);
END $f$;

CREATE FUNCTION public.pg_try_advisory_xact_lock(bigint) RETURNS boolean
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pg_try_advisory_xact_lock');
  RETURN pg_catalog.pg_try_advisory_xact_lock($1);
END $f$;

-- clock_timestamp() is referenced UNQUALIFIED inside notify_queue_listeners
-- (both the throttle predicate and the last_notified_at assignment) and in most
-- other functions. Like lower() above, it is defended solely by the pinned
-- search_path, so this shadow probes the pin on the trigger's own code path.
CREATE FUNCTION public.clock_timestamp() RETURNS timestamptz
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('clock_timestamp');
  RETURN pg_catalog.clock_timestamp();
END $f$;

-- These objects show why pinning pgmq before pg_catalog is insufficient. A
-- queue creator can install them in pgmq without owning any extension object.
CREATE FUNCTION pgmq.clock_timestamp() RETURNS timestamptz
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.clock_timestamp');
  RETURN pg_catalog.clock_timestamp();
END $f$;

CREATE FUNCTION pgmq.lower(text) RETURNS text
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.lower');
  RETURN pg_catalog.lower($1);
END $f$;

-- The base pins pgmq before pg_catalog. This exact-signature shadow can win
-- in read_grouped_rr's dynamic SQL if a visible group reaches its lock check.
CREATE FUNCTION pgmq.pg_try_advisory_xact_lock(bigint) RETURNS boolean
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.pg_try_advisory_xact_lock');
  RETURN pg_catalog.pg_try_advisory_xact_lock($1);
END $f$;

-- Schema qualification does not prevent a more exact overload from winning.
-- pgmq.meta.queue_name is varchar, while these PGMQ entry points take text.
CREATE FUNCTION pgmq.metrics(varchar) RETURNS pgmq.metrics_result
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.metrics(varchar)');
  RETURN pgmq.metrics($1::pg_catalog.text);
END $f$;

CREATE FUNCTION pgmq.create_fifo_index(varchar) RETURNS void
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.create_fifo_index(varchar)');
  PERFORM pgmq.create_fifo_index($1::pg_catalog.text);
END $f$;

CREATE FUNCTION pgmq.pgmq_shadow_mul(integer, interval) RETURNS interval
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('pgmq.op_int_mul_interval');
  RETURN $1::float8 OPERATOR(pg_catalog.*) $2;
END $f$;
CREATE OPERATOR pgmq.* (LEFTARG = integer, RIGHTARG = interval, FUNCTION = pgmq.pgmq_shadow_mul);

RESET ROLE;

-- Put the attacker-writable schema first on the caller's search_path, so an
-- unqualified reference would resolve to the shadow.
SET search_path = public, pg_catalog;

-- Sanity: the shadows are reachable from this search_path (they write the canary
-- when called directly).
SELECT public.make_interval(secs => 1) IS NOT NULL AS shadow_reachable;
SELECT count(*) > 0 AS shadow_planted_ok FROM public.pgmq_canary;

-- From here, any canary row means a pgmq function executed a planted shadow.
TRUNCATE public.pgmq_canary;

-- create -> acquire_queue_lock -> pg_advisory_xact_lock(hashtext(...))
SELECT pgmq.create('sec_test_q');

-- These calls read the varchar queue name from pgmq.meta and delegate to text
-- parameters. They must not select the attacker-defined varchar overloads.
SELECT count(*) = 1 AS metrics_all_ok FROM pgmq.metrics_all();
SELECT pgmq.create_fifo_indexes_all();

-- enable_notify_insert -> to_timestamp(0); throttle > 0 forces the trigger's
-- (throttle_interval_ms * INTERVAL '1 millisecond') operator to be evaluated.
SELECT pgmq.enable_notify_insert('sec_test_q', 100);

-- send -> INSERT fires notify_queue_listeners trigger (int * interval operator)
SELECT msg_id > 0 AS sent_ok
  FROM pgmq.send('sec_test_q', '{"a": 1}') AS t(msg_id);

-- read -> make_interval(secs => vt)
SELECT count(*) = 1 AS read_ok
  FROM pgmq.read('sec_test_q', 30, 1);

-- read_with_poll -> make_interval + pg_sleep (message is invisible, so it polls)
SELECT count(*) AS polled_rows
  FROM pgmq.read_with_poll('sec_test_q', 30, 1, 1, 100);

-- read_grouped_rr -> dynamic SQL uses unqualified pg_try_advisory_xact_lock.
-- The first message is invisible in the default group. A separate group must
-- have a visible head, or the lock predicate need not run.
SELECT msg_id > 0 AS sent2_ok
  FROM pgmq.send('sec_test_q', '{"b": 2}', '{"x-pgmq-group": "sec_test_group"}'::pg_catalog.jsonb) AS t(msg_id);
SELECT count(*) = 1 AS grouped_read_ok
  FROM pgmq.read_grouped_rr('sec_test_q', 30, 1);
SELECT count(*) AS grouped_lock_shadow_hits FROM public.pgmq_canary
 WHERE via OPERATOR(pg_catalog.=) 'pgmq.pg_try_advisory_xact_lock';

-- update_notify_insert -> to_timestamp(0)
SELECT pgmq.update_notify_insert('sec_test_q', 200);

-- Check these calls before resetting the canary for the direct INSERT below.
SELECT count(*) AS function_canary_hits FROM public.pgmq_canary;
TRUNCATE public.pgmq_canary;

-- The notify trigger is the worst-case U1 surface: it fires implicitly under the
-- INSERTING role's search_path, so its own pinned search_path is the only defense.
-- Firing it via pgmq.send() (above) is not enough to prove that pin: a trigger
-- without its own pin would inherit send()'s pinned path and still be safe. The
-- real vector is a DIRECT INSERT into the queue table under the attacker path, so
-- exercise exactly that here. clock_timestamp() (planted above) is evaluated by
-- the trigger's throttle predicate; only the trigger's pin keeps the shadow out.
-- (vt uses pg_catalog.clock_timestamp() so this top-level INSERT, which runs
-- under the attacker path, does not itself trip the canary.)
INSERT INTO pgmq.q_sec_test_q (vt, message)
  VALUES (pg_catalog.clock_timestamp(), '{"c": 3}');
SELECT count(*) AS direct_insert_canary_hits FROM public.pgmq_canary;
TRUNCATE public.pgmq_canary;

-- The public partition wrapper has no pinned path, so its own comparison
-- must name the pg_catalog equality operator. A missing archive is a no-op
-- that reaches the path-restoration comparison without calling pg_partman.
SET ROLE pgmq_shadow_attacker;
CREATE FUNCTION public.pgmq_shadow_text_eq(text, text) RETURNS boolean
  LANGUAGE plpgsql AS $f$
BEGIN
  INSERT INTO public.pgmq_canary VALUES ('public.text_eq');
  RETURN $1 OPERATOR(pg_catalog.=) $2;
END $f$;
CREATE OPERATOR public.= (LEFTARG = text, RIGHTARG = text, FUNCTION = public.pgmq_shadow_text_eq);
RESET ROLE;
SELECT pgmq.convert_archive_partitioned('missing_sec_test_q');
SELECT count(*) FILTER (WHERE via OPERATOR(pg_catalog.=) 'public.text_eq') AS wrapper_operator_hits
  FROM public.pgmq_canary;
DROP OPERATOR public.= (text, text);
DROP FUNCTION public.pgmq_shadow_text_eq(text, text);

-- The escalation is blocked: no planted shadow ran inside any pgmq function.
SELECT count(*) AS canary_hits,
       coalesce(string_agg(DISTINCT via, ', ' ORDER BY via), '(none)') AS shadows_run
  FROM public.pgmq_canary;

RESET search_path;

-- Cleanup.
SELECT pgmq.drop_queue('sec_test_q');
DROP OPERATOR public.* (integer, interval);
DROP FUNCTION public.make_interval(integer);
DROP FUNCTION public.pg_sleep(numeric);
DROP FUNCTION public.to_timestamp(integer);
DROP FUNCTION public.pg_advisory_xact_lock(integer);
DROP FUNCTION public.pgmq_shadow_mul(integer, interval);
DROP FUNCTION public.lower(text);
DROP FUNCTION public.pg_try_advisory_xact_lock(bigint);
DROP FUNCTION public.clock_timestamp();
DROP OPERATOR pgmq.* (integer, interval);
DROP FUNCTION pgmq.pgmq_shadow_mul(integer, interval);
DROP FUNCTION pgmq.lower(text);
DROP FUNCTION pgmq.pg_try_advisory_xact_lock(bigint);
DROP FUNCTION pgmq.metrics(varchar);
DROP FUNCTION pgmq.create_fifo_index(varchar);
DROP FUNCTION pgmq.clock_timestamp();
DROP TABLE public.pgmq_canary;
REVOKE CREATE, USAGE ON SCHEMA public FROM pgmq_shadow_attacker;
REVOKE CREATE, USAGE ON SCHEMA pgmq FROM pgmq_shadow_attacker;
DROP ROLE pgmq_shadow_attacker;
DROP EXTENSION pgmq CASCADE;
