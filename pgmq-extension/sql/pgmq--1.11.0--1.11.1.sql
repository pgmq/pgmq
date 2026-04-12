-- Allow `pgmq.topic_bindings` to be dumped by `pg_dump` when pgmq is installed as an extension
DO
$$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgmq') THEN
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.topic_bindings', '');
    END IF;
END
$$;


-- read_grouped_head:  read the head of N different FIFO groups in a single operation.
-- This supports horizontal scaling by processing groups in parallel while ensuring message ordering is preserved per group.
CREATE OR REPLACE FUNCTION pgmq.read_grouped_head(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            -- Determine the absolute head (oldest) message id per FIFO group, regardless of visibility
            SELECT
                COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                MIN(msg_id) AS head_msg_id
            FROM pgmq.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        selected_messages AS (
            -- Take at most 1 message per group
            SELECT g.head_msg_id msg_id
            FROM fifo_groups g
            JOIN pgmq.%1$I q ON q.msg_id = g.head_msg_id
	        WHERE q.vt <= clock_timestamp()
            ORDER BY q.msg_id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%1$I m
        SET
            vt = clock_timestamp() + %2$L,
            read_ct = read_ct + 1,
            last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pgmq._ensure_pg_partman_installed()
RETURNS void AS $$
BEGIN
  IF NOT pgmq._extension_exists('pg_partman') THEN
    RAISE EXCEPTION 'pg_partman is required for partitioned queues';
  END IF;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS pgmq.enable_notify_insert(queue_name text);
