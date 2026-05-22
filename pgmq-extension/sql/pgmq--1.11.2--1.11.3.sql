-- Table to track notification throttling for queue deletes
CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.notify_delete_throttle (
    queue_name           VARCHAR UNIQUE NOT NULL
       CONSTRAINT notify_delete_throttle_meta_queue_name_fk
            REFERENCES pgmq.meta (queue_name)
            ON DELETE CASCADE,
    throttle_interval_ms INTEGER NOT NULL DEFAULT 0,
    last_notified_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT to_timestamp(0)
);

CREATE INDEX IF NOT EXISTS idx_notify_delete_throttle_active
    ON pgmq.notify_delete_throttle (queue_name, last_notified_at)
    WHERE throttle_interval_ms > 0;

-- Allow `pgmq.notify_delete_throttle` to be dumped by `pg_dump` when pgmq is installed as an extension
DO
$$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgmq') THEN
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.notify_delete_throttle', '');
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION pgmq.notify_queue_delete_listeners()
RETURNS TRIGGER AS $$
DECLARE
  queue_name_extracted TEXT; -- Queue name extracted from trigger table name
  updated_count        INTEGER; -- Number of rows updated (0 or 1)
BEGIN
  queue_name_extracted := substring(TG_TABLE_NAME from 3);

  UPDATE pgmq.notify_delete_throttle
  SET last_notified_at = clock_timestamp()
  WHERE queue_name = queue_name_extracted
    AND (
      throttle_interval_ms = 0 -- No throttling configured
          OR clock_timestamp() - last_notified_at >=
             (throttle_interval_ms * INTERVAL '1 millisecond') -- Throttle interval has elapsed
    );

  -- Check how many rows were updated (will be 0 or 1)
  GET DIAGNOSTICS updated_count = ROW_COUNT;

  IF updated_count > 0 THEN
    PERFORM PG_NOTIFY('pgmq.' || TG_TABLE_NAME || '.' || TG_OP, NULL);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.enable_notify_delete(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
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

  PERFORM pgmq.disable_notify_delete(v_queue_name);

  INSERT INTO pgmq.notify_delete_throttle (queue_name, throttle_interval_ms)
  VALUES (v_queue_name, v_throttle_interval_ms)
  ON CONFLICT ON CONSTRAINT notify_delete_throttle_queue_name_key DO UPDATE
      SET throttle_interval_ms = EXCLUDED.throttle_interval_ms,
          last_notified_at = to_timestamp(0);

  EXECUTE FORMAT(
    $QUERY$
    CREATE CONSTRAINT TRIGGER trigger_notify_queue_delete_listeners
    AFTER DELETE ON pgmq.%I
    DEFERRABLE FOR EACH ROW
    EXECUTE PROCEDURE pgmq.notify_queue_delete_listeners()
    $QUERY$,
    qtable
  );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.disable_notify_delete(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  v_queue_name TEXT := queue_name;
BEGIN
  EXECUTE FORMAT(
    $QUERY$
    DROP TRIGGER IF EXISTS trigger_notify_queue_delete_listeners ON pgmq.%I;
    $QUERY$,
    qtable
  );

  DELETE FROM pgmq.notify_delete_throttle ndt WHERE ndt.queue_name = v_queue_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.list_notify_delete_throttles()
    RETURNS TABLE
            (
                queue_name           text,
                throttle_interval_ms integer,
                last_notified_at     TIMESTAMP WITH TIME ZONE
            )
    LANGUAGE sql
    STABLE
AS
$$
    SELECT queue_name, throttle_interval_ms, last_notified_at
    FROM pgmq.notify_delete_throttle
    ORDER BY queue_name;
$$;

CREATE OR REPLACE FUNCTION pgmq.update_notify_delete(queue_name text, throttle_interval_ms integer)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF throttle_interval_ms < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative, got: %', throttle_interval_ms;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE meta.queue_name = update_notify_delete.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist. Create the queue first using pgmq.create()', queue_name;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.notify_delete_throttle WHERE notify_delete_throttle.queue_name = update_notify_delete.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not have notify_delete enabled. Enable it first using pgmq.enable_notify_delete()', queue_name;
    END IF;

    UPDATE pgmq.notify_delete_throttle
    SET throttle_interval_ms = update_notify_delete.throttle_interval_ms,
        last_notified_at = to_timestamp(0)
    WHERE notify_delete_throttle.queue_name = update_notify_delete.queue_name;
END;
$$;
