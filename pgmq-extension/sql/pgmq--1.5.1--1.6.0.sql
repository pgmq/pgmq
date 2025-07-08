-- used for consistently creating a lock for a specific queue
CREATE OR REPLACE FUNCTION pgmq.acquire_queue_lock(queue_name TEXT) 
RETURNS void AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('pgmq.queue_' || queue_name));
END;
$$ LANGUAGE plpgsql;

-- fixes length comparison
DROP FUNCTION pgmq."validate_queue_name";
CREATE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) > 47 THEN
    -- complete table identifier must be <= 63
    -- https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    -- e.g. template_pgmq_q_my_queue is an identifier for my_queue when partitioned
    -- template_pgmq_q_ (16) + <a max length queue name> (47) = 63 
    RAISE EXCEPTION 'queue name is too long, maximum length is 47 characters';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- adds advisory lock
-- returns false when queue does not exist
DROP FUNCTION pgmq.drop_queue(TEXT);
CREATE FUNCTION pgmq.drop_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    qtable_seq TEXT := qtable || '_msg_id_seq';
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
    fq_atable TEXT := 'pgmq.' || atable;
    partitioned BOOLEAN;
BEGIN
    PERFORM pgmq.acquire_queue_lock(queue_name);
    EXECUTE FORMAT(
        $QUERY$
        SELECT is_partitioned FROM pgmq.meta WHERE queue_name = %L
        $QUERY$,
        queue_name
    ) INTO partitioned;

    -- check if the queue exists
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = qtable and table_schema = 'pgmq'
    ) THEN
        RAISE NOTICE 'pgmq queue `%` does not exist', queue_name;
        RETURN FALSE;
    END IF;

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = %L
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM %I.part_config where parent_table in (%L, %L)
          $QUERY$,
          pgmq._get_pg_partman_schema(), fq_qtable, fq_atable
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- adds advisory lock
DROP FUNCTION pgmq.create_non_partitioned(TEXT);
CREATE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
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
END;
$$ LANGUAGE plpgsql;

-- adds advisory lock
DROP FUNCTION pgmq.create_unlogged(TEXT);
CREATE FUNCTION pgmq.create_unlogged(queue_name TEXT)
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
END;
$$ LANGUAGE plpgsql;

-- adds advisory lock
DROP FUNCTION pgmq.create_partitioned(TEXT, TEXT, TEXT);
CREATE FUNCTION pgmq.create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000'
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
  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
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
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_qtable,
    partition_col,
    partition_interval
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
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_atable,
    a_partition_col,
    partition_interval
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

END;
$$ LANGUAGE plpgsql;

-- detach queue, sequence, and archive objects from extension and make pgmq.meta pg_dump-able
DO $$
DECLARE
    queue_record RECORD;
    detach_cmd TEXT;
    detached_object TEXT;
BEGIN

    -- add pgmq.meta as a member object to the extension
    PERFORM pg_catalog.pg_extension_config_dump('pgmq.meta', '');

    -- iterate through all queues and remove their objects from the extension
    FOR queue_record IN SELECT queue_name FROM pgmq.meta LOOP
        -- queue table
        BEGIN
            detach_cmd := format('ALTER EXTENSION pgmq DROP TABLE pgmq.%I', 'q_'  || queue_record.queue_name);
            EXECUTE detach_cmd;
            detached_object := format('TABLE pgmq.q_%s', queue_record.queue_name);
            RAISE NOTICE 'Detached: %', detached_object;
        EXCEPTION WHEN others THEN
            RAISE WARNING 'Failed to detach queue table for %: %', queue_record.queue_name, SQLERRM;
        END;

        -- queue sequence
        BEGIN
            detach_cmd := format('ALTER EXTENSION pgmq DROP SEQUENCE pgmq.%I', 'q_' || queue_record.queue_name || '_msg_id_seq');
            EXECUTE detach_cmd;
            detached_object := format('SEQUENCE pgmq.q_%s_msg_id_seq', queue_record.queue_name);
            RAISE NOTICE 'Detached: %', detached_object;
        EXCEPTION WHEN others THEN
            RAISE WARNING 'Failed to detach queue sequence for %: %', queue_record.queue_name, SQLERRM;
        END;

        -- archive table
        BEGIN
            detach_cmd := format('ALTER EXTENSION pgmq DROP TABLE pgmq.%I', 'a_' || queue_record.queue_name);
            EXECUTE detach_cmd;
            detached_object := format('TABLE pgmq.a_%s', queue_record.queue_name);
            RAISE NOTICE 'Detached: %', detached_object;
        EXCEPTION WHEN others THEN
            RAISE WARNING 'Failed to detach archive table for %: %', queue_record.queue_name, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- deprecate detach_archive function
DROP FUNCTION pgmq.detach_archive(TEXT);
CREATE FUNCTION pgmq."detach_archive"(queue_name TEXT)
RETURNS VOID AS $$
DECLARE
  atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
  RAISE WARNING 'detach_archive(queue_name) is deprecated and is a no-op. It will be removed in PGMQ v2.0. Archive tables are no longer member objects.';
END
$$ LANGUAGE plpgsql;