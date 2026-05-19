-- read_grouped_head_with_poll
-- reads the head of N different FIFO groups in a single operation, with polling support
CREATE OR REPLACE FUNCTION pgmq.read_grouped_head_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMPTZ;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF clock_timestamp() >= stop_at THEN
        RETURN;
      END IF;

      FOR r IN
        SELECT * FROM pgmq.read_grouped_head(queue_name, vt, qty)
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
