-- Insert notifications never fired for a queue whose name is not all lowercase.
-- notify_queue_listeners() rebuilt the queue name from the trigger table name,
-- but format_table_name lowercases it, so the queue MyQueue lives in
-- pgmq.q_myqueue and the rebuilt name (myqueue) matched no row in
-- notify_insert_throttle: the throttle UPDATE touched nothing, so the guarded
-- PG_NOTIFY was never reached. The queue name is now passed to the trigger
-- function as an argument by enable_notify_insert.

CREATE OR REPLACE FUNCTION pgmq.notify_queue_listeners()
RETURNS TRIGGER AS $$
DECLARE
  v_queue_name  TEXT; -- Queue name, as stored in pgmq.meta
  updated_count INTEGER; -- Number of rows updated (0 or 1)
BEGIN
  -- enable_notify_insert passes the queue name as a trigger argument. The table
  -- name cannot be turned back into it: format_table_name lowercases, so the
  -- queue MyQueue lives in pgmq.q_myqueue and stripping the prefix yields
  -- myqueue, which matches no row in notify_insert_throttle. Triggers created
  -- before the argument existed fall back to the table name.
  v_queue_name := COALESCE(TG_ARGV[0], substring(TG_TABLE_NAME from 3));

  UPDATE pgmq.notify_insert_throttle
  SET last_notified_at = clock_timestamp()
  WHERE queue_name = v_queue_name
    AND (
      throttle_interval_ms = 0 -- No throttling configured
          OR clock_timestamp() - last_notified_at >=
             (throttle_interval_ms * INTERVAL '1 millisecond') -- Throttle interval has elapsed
    );

  -- Check how many rows were updated (will be 0 or 1)
  GET DIAGNOSTICS updated_count = ROW_COUNT;

  IF updated_count > 0 THEN
    PERFORM PG_NOTIFY('pgmq.q_' || v_queue_name || '.' || TG_OP, NULL);
  END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  v_queue_name TEXT := queue_name;
  v_throttle_interval_ms INTEGER := throttle_interval_ms;
BEGIN
  -- Validate that throttle_interval_ms is non-negative
  IF v_throttle_interval_ms < 0 THEN
    RAISE EXCEPTION 'throttle_interval_ms must be non-negative';
  END IF;

  -- Validate that the queue table exists
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgmq' AND table_name = qtable) THEN
    RAISE EXCEPTION 'Queue "%" does not exist. Create it first using pgmq.create()', v_queue_name;
  END IF;

  PERFORM pgmq.disable_notify_insert(v_queue_name);

  INSERT INTO pgmq.notify_insert_throttle (queue_name, throttle_interval_ms)
  VALUES (v_queue_name, v_throttle_interval_ms)
  ON CONFLICT ON CONSTRAINT notify_insert_throttle_queue_name_key DO UPDATE
      SET throttle_interval_ms = EXCLUDED.throttle_interval_ms,
          last_notified_at = to_timestamp(0);

  EXECUTE FORMAT(
    $QUERY$
    CREATE CONSTRAINT TRIGGER trigger_notify_queue_insert_listeners
    AFTER INSERT ON pgmq.%I
    DEFERRABLE FOR EACH ROW
    EXECUTE PROCEDURE pgmq.notify_queue_listeners(%L)
    $QUERY$,
    qtable, v_queue_name
  );
END;
$$ LANGUAGE plpgsql;

-- Triggers created by the previous enable_notify_insert carry no argument, so
-- they keep taking the fallback path and a queue with uppercase characters in
-- its name stays silent. Re-create them with the queue name attached, following
-- the loop from the 1.9.0 to 1.10.0 migration. Throttle settings are read from
-- notify_insert_throttle rather than reset, so enabled queues keep their
-- interval and only the trigger definition changes.
DO $$
DECLARE
    throttle_record RECORD;
    qtable TEXT;
BEGIN
    FOR throttle_record IN SELECT queue_name FROM pgmq.notify_insert_throttle LOOP
        qtable := pgmq.format_table_name(throttle_record.queue_name, 'q');

        IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pgmq'
            AND table_name = qtable
        ) THEN
            EXECUTE FORMAT(
                'DROP TRIGGER IF EXISTS trigger_notify_queue_insert_listeners ON pgmq.%I',
                qtable
            );
            EXECUTE FORMAT(
                $QUERY$
                CREATE CONSTRAINT TRIGGER trigger_notify_queue_insert_listeners
                AFTER INSERT ON pgmq.%I
                DEFERRABLE FOR EACH ROW
                EXECUTE PROCEDURE pgmq.notify_queue_listeners(%L)
                $QUERY$,
                qtable, throttle_record.queue_name
            );
        END IF;
    END LOOP;
END;
$$;

-- New queues must also grant pg_monitor the minimal metrics privileges
-- (column-level SELECT on vt/enqueued_at and SELECT on the msg_id sequence),
-- so redefine the queue-creation functions to match sql/pgmq.sql. Without this,
-- queues created after the upgrade would leave pg_monitor unable to compute
-- their metrics.

CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  -- Let pg_monitor compute queue metrics (pgmq.metrics / pgmq.metrics_all) without exposing
  -- message payloads: column-level SELECT on the metrics columns only, plus the msg_id sequence.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  -- Let pg_monitor compute queue metrics (pgmq.metrics / pgmq.metrics_all) without exposing
  -- message payloads: column-level SELECT on the metrics columns only, plus the msg_id sequence.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000',
  premake INTEGER DEFAULT 4
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
  a_partition_col TEXT;
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
  fq_qtable TEXT := 'pgmq.' || qtable;
  fq_atable TEXT := 'pgmq.' || atable;
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);
  PERFORM pgmq._ensure_pg_partman_installed();
  IF premake < 1 THEN
    RAISE EXCEPTION 'premake must be at least 1, got %', premake;
  END IF;
  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    ) PARTITION BY RANGE (%I)
    $QUERY$,
    qtable, partition_col
  );

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_premake := %s,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_qtable,
    partition_col,
    partition_interval,
    premake
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (%I);
    $QUERY$,
    qtable || '_part_idx', qtable, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, true, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT NOT NULL,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    ) PARTITION BY RANGE (%I);
    $QUERY$,
    atable, a_partition_col
  );

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_premake := %s,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_atable,
    a_partition_col,
    partition_interval,
    premake
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  -- Let pg_monitor compute queue metrics (pgmq.metrics / pgmq.metrics_all) without exposing
  -- message payloads: column-level SELECT on the metrics columns only, plus the msg_id sequence.
  -- For partitioned queues, granting on the parent is sufficient: metrics query the parent and
  -- the privilege check is performed there, not on individual partitions.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);

END;
$$ LANGUAGE plpgsql;

-- pg_monitor could read every table and sequence in the pgmq schema, including
-- queue and archive tables, i.e. message payloads and headers. pg_monitor is
-- meant for monitoring, not application data. This migration removes that broad
-- access and replaces it with the minimal privileges the monitoring functions
-- (pgmq.metrics / pgmq.metrics_all / pgmq.list_queues) actually need: USAGE on the
-- schema and SELECT on pgmq.meta (kept from earlier versions), plus column-level
-- SELECT on the metrics columns (vt, enqueued_at) of each queue table and SELECT on
-- each queue's msg_id sequence. Message payloads (the message/headers columns) and
-- archive tables are never exposed to pg_monitor. This matches the grants that
-- pgmq.create_non_partitioned / create_unlogged / create_partitioned now apply to
-- new queues, so fresh installs and upgraded installs end up with the same ACLs.

-- 1. Remove the default privileges that granted SELECT on every new table and
--    sequence in the pgmq schema to pg_monitor. They belong to the role that ran
--    the original grant (usually the role that installed pgmq), which is not
--    necessarily the role running this upgrade.
DO $$
DECLARE
    acl RECORD;
BEGIN
    FOR acl IN
        SELECT DISTINCT d.defaclrole::regrole AS role_name
        FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        CROSS JOIN LATERAL aclexplode(d.defaclacl) a
        WHERE n.nspname = 'pgmq'
          AND d.defaclobjtype IN ('r', 'S')
          AND a.grantee = 'pg_monitor'::regrole
    LOOP
        BEGIN
            EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor', acl.role_name);
            EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON SEQUENCES FROM pg_monitor', acl.role_name);
        EXCEPTION WHEN insufficient_privilege OR undefined_object THEN
            RAISE WARNING 'pgmq: could not remove default privileges of role % granting SELECT to pg_monitor in schema pgmq; run "ALTER DEFAULT PRIVILEGES FOR ROLE % IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor" (and the same ON SEQUENCES) as that role or a superuser', acl.role_name, acl.role_name;
        END;
    END LOOP;
END;
$$;

-- 2. Revoke SELECT from pg_monitor on every relation in the pgmq schema except
--    pgmq.meta: queue and archive tables, partitioned parents and their
--    pg_partman children, sequences, and any user-added views/matviews/foreign
--    tables over them. REVOKE by a role that is not the object owner and lacks
--    grant option is a no-op that only warns; step 4 reports anything left behind.
DO $$
DECLARE
    rel RECORD;
BEGIN
    FOR rel IN
        SELECT DISTINCT c.oid::regclass AS rel_name,
               c.relkind = 'S' AS is_sequence
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE n.nspname = 'pgmq'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
          AND c.relname <> 'meta'
          AND a.grantee = 'pg_monitor'::regrole
          AND a.privilege_type = 'SELECT'
    LOOP
        BEGIN
            EXECUTE FORMAT(
                'REVOKE SELECT ON %s %s FROM pg_monitor',
                CASE WHEN rel.is_sequence THEN 'SEQUENCE' ELSE 'TABLE' END,
                rel.rel_name
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'pgmq: could not revoke SELECT on % from pg_monitor; run "REVOKE SELECT ON % FROM pg_monitor" as its owner or a superuser', rel.rel_name, rel.rel_name;
        END;
    END LOOP;
END;
$$;

-- 3. Grant pg_monitor the minimal access the metrics functions need on every
--    existing queue: column-level SELECT on (vt, enqueued_at) of the queue table
--    and SELECT on its msg_id sequence. For partitioned queues the grant on the
--    parent is sufficient (metrics query the parent). This mirrors the grants that
--    pgmq.create_* now apply to new queues. Re-running can apply grants that
--    previously failed due to insufficient privileges.
DO $$
DECLARE
    q RECORD;
    qtable TEXT;
    qseq TEXT;
BEGIN
    FOR q IN SELECT queue_name FROM pgmq.meta LOOP
        qtable := pgmq.format_table_name(q.queue_name, 'q');
        qseq := qtable || '_msg_id_seq';
        IF to_regclass(FORMAT('pgmq.%I', qtable)) IS NOT NULL THEN
            BEGIN
                EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT (vt, enqueued_at) on pgmq.% to pg_monitor; run it as the table owner or a superuser so queue metrics work for pg_monitor', quote_ident(qtable);
            END;
        END IF;
        IF to_regclass(FORMAT('pgmq.%I', qseq)) IS NOT NULL THEN
            BEGIN
                EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qseq);
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT on sequence pgmq.% to pg_monitor; run it as the sequence owner or a superuser so queue metrics work for pg_monitor', quote_ident(qseq);
            END;
        END IF;
    END LOOP;
END;
$$;

-- 4. Report any queue/archive table that still exposes whole-table SELECT to
--    pg_monitor -- for example a grant made WITH GRANT OPTION by a role this
--    upgrade cannot act as, which REVOKE silently skips. Only heap-like relations
--    (tables, partitioned parents, views, matviews, foreign tables) can leak
--    payloads, so sequences are excluded: pg_monitor keeps whole-sequence SELECT
--    by design (step 3), and sequences hold no message data. Column-level grants
--    live in pg_attribute, not pg_class.relacl, so the safe (vt, enqueued_at)
--    grants from step 3 are not reported here. This only warns; it never fails
--    the upgrade.
DO $$
DECLARE
    leftover TEXT;
BEGIN
    SELECT string_agg(c.oid::regclass::text, ', ' ORDER BY c.oid::regclass::text)
    INTO leftover
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL aclexplode(c.relacl) a
    WHERE n.nspname = 'pgmq'
      AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND c.relname <> 'meta'
      AND a.grantee = 'pg_monitor'::regrole
      AND a.privilege_type = 'SELECT';

    IF leftover IS NOT NULL THEN
        RAISE WARNING 'pgmq: pg_monitor still has whole-table SELECT on: %. These may expose message payloads; revoke them as the object owner or a superuser.', leftover;
    END IF;
END;
$$;
