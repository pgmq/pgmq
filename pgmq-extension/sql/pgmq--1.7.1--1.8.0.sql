-- Table to track notification throttling for queues
CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.notify_insert_throttle (
    queue_name           VARCHAR UNIQUE NOT NULL -- Queue name (without 'q_' prefix)
       CONSTRAINT notify_insert_throttle_meta_queue_name_fk
            REFERENCES pgmq.meta (queue_name)
            ON DELETE CASCADE,
    throttle_interval_ms INTEGER NOT NULL DEFAULT 0, -- Min milliseconds between notifications (0 = no throttling)
    last_notified_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT to_timestamp(0) -- Timestamp of last sent notification
);

CREATE INDEX IF NOT EXISTS idx_notify_throttle_active
    ON pgmq.notify_insert_throttle (queue_name, last_notified_at)
    WHERE throttle_interval_ms > 0;

SELECT pg_catalog.pg_extension_config_dump('pgmq.notify_insert_throttle', '');

CREATE OR REPLACE FUNCTION pgmq.notify_queue_listeners()
RETURNS TRIGGER AS $$
DECLARE
  queue_name_extracted TEXT; -- Queue name extracted from trigger table name
  updated_count        INTEGER; -- Number of rows updated (0 or 1)
BEGIN
  queue_name_extracted := substring(TG_TABLE_NAME from 3);

  UPDATE pgmq.notify_insert_throttle
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
    EXECUTE PROCEDURE pgmq.notify_queue_listeners()
    $QUERY$,
    qtable
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.disable_notify_insert(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q');
  v_queue_name TEXT := queue_name;
BEGIN
  EXECUTE FORMAT(
    $QUERY$
    DROP TRIGGER IF EXISTS trigger_notify_queue_insert_listeners ON pgmq.%I;
    $QUERY$,
    qtable
  );

  DELETE FROM pgmq.notify_insert_throttle nit WHERE nit.queue_name = v_queue_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    new_vt TIMESTAMP WITH TIME ZONE;
BEGIN
    new_vt := clock_timestamp() + make_interval(secs => vt);
    
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = $1
        WHERE msg_id = ANY($2)
        RETURNING *;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING new_vt, msg_ids;
END;
$$ LANGUAGE plpgsql;
