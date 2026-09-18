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
