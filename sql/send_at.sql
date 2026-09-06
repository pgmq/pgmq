-- Support for sending messages scheduled at specific datetime
CREATE OR REPLACE FUNCTION pgmq.send_at(
    queue_name TEXT,
    msg JSONB,
    send_at TIMESTAMPTZ
) RETURNS BIGINT AS $$
DECLARE
    delay_seconds INT;
BEGIN
    delay_seconds := GREATEST(0, EXTRACT(EPOCH FROM (send_at - CLOCK_TIMESTAMP()))::INT);
    RETURN pgmq.send(queue_name, msg, delay_seconds);
END;
$$ LANGUAGE plpgsql;
