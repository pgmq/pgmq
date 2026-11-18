-- Extension scripts normally run with pgmq before pg_catalog. A queue creator
-- can add functions in pgmq, so keep the upgrade itself on a trusted path.
SET LOCAL search_path = pg_catalog, pg_temp;

-- create() and create_unlogged() on an existing queue ran CREATE INDEX IF NOT EXISTS
-- on the queue and archive tables. CREATE INDEX takes a SHARE lock before it checks
-- whether the index exists, so the call waited for every open transaction that had
-- written to the queue, and every new send/read/delete queued up behind it. The DDL
-- now runs only when the object is missing (to_regclass() takes no lock), and the
-- pg_monitor grants are skipped when the privileges are already there.

CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
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

  -- CREATE INDEX takes a SHARE lock on the table before it checks IF NOT EXISTS, so on an existing queue it
  -- would wait for every open transaction that has written to it. to_regclass() takes no lock: only run the
  -- DDL when the index is actually missing, which keeps create() idempotent (it still repairs a missing index).
  IF to_regclass(FORMAT('pgmq.%I', qtable || '_vt_idx')) IS NULL THEN
    EXECUTE FORMAT(
      $QUERY$
      CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
      $QUERY$,
      qtable || '_vt_idx', qtable
    );
  END IF;

  IF to_regclass(FORMAT('pgmq.%I', 'archived_at_idx_' || queue_name)) IS NULL THEN
    EXECUTE FORMAT(
      $QUERY$
      CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
      $QUERY$,
      'archived_at_idx_' || queue_name, atable
    );
  END IF;

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
  -- Skip the grants when pg_monitor already has them, so a create() on an existing queue doesn't rewrite catalog rows.
  IF NOT (
    has_column_privilege('pg_monitor', FORMAT('pgmq.%I', qtable), 'vt', 'SELECT')
    AND has_column_privilege('pg_monitor', FORMAT('pgmq.%I', qtable), 'enqueued_at', 'SELECT')
  ) THEN
    EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  END IF;
  IF NOT has_sequence_privilege('pg_monitor', FORMAT('pgmq.%I', qtable_seq), 'SELECT') THEN
    EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);
  END IF;

END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
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

  -- CREATE INDEX takes a SHARE lock on the table before it checks IF NOT EXISTS, so on an existing queue it
  -- would wait for every open transaction that has written to it. to_regclass() takes no lock: only run the
  -- DDL when the index is actually missing, which keeps create() idempotent (it still repairs a missing index).
  IF to_regclass(FORMAT('pgmq.%I', qtable || '_vt_idx')) IS NULL THEN
    EXECUTE FORMAT(
      $QUERY$
      CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
      $QUERY$,
      qtable || '_vt_idx', qtable
    );
  END IF;

  IF to_regclass(FORMAT('pgmq.%I', 'archived_at_idx_' || queue_name)) IS NULL THEN
    EXECUTE FORMAT(
      $QUERY$
      CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
      $QUERY$,
      'archived_at_idx_' || queue_name, atable
    );
  END IF;

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
  -- Skip the grants when pg_monitor already has them, so a create() on an existing queue doesn't rewrite catalog rows.
  IF NOT (
    has_column_privilege('pg_monitor', FORMAT('pgmq.%I', qtable), 'vt', 'SELECT')
    AND has_column_privilege('pg_monitor', FORMAT('pgmq.%I', qtable), 'enqueued_at', 'SELECT')
  ) THEN
    EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  END IF;
  IF NOT has_sequence_privilege('pg_monitor', FORMAT('pgmq.%I', qtable_seq), 'SELECT') THEN
    EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);
  END IF;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;
