------------------------------------------------------------
-- Schema, tables, records, privileges, indexes, etc
------------------------------------------------------------
-- We don't need to create the `pgmq` schema because it is automatically
-- created by postgres due to being declared in extension control file

-- Table where queues and metadata about them is stored
CREATE TABLE pgmq.meta (
    queue_name VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    is_unlogged BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Grant permission to pg_monitor to all tables and sequences
GRANT USAGE ON SCHEMA pgmq TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgmq TO pg_monitor;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgmq TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT SELECT ON TABLES TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT SELECT ON SEQUENCES TO pg_monitor;

-- This type has the shape of a message in a queue, and is often returned by
-- pgmq functions that return messages
CREATE TYPE pgmq.message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB
);

------------------------------------------------------------
-- Functions
------------------------------------------------------------
-- read
-- reads a number of messages from a queue, setting a visibility timeout on them
CREATE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pgmq.q_%s
            WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.q_%s m
        SET
            vt = clock_timestamp() + interval '%s seconds',
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
        $QUERY$,
        queue_name, queue_name, vt
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

---- read_with_poll
---- reads a number of messages from a queue, setting a visibility timeout on them
CREATE FUNCTION pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMP;
    sql TEXT;
BEGIN
    stop_at := clock_timestamp() + FORMAT('%s seconds', max_poll_seconds)::interval;
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      sql := FORMAT(
          $QUERY$
          WITH cte AS
          (
              SELECT msg_id
              FROM pgmq.q_%s
              WHERE vt <= clock_timestamp()
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.q_%s m
          SET
              vt = clock_timestamp() + interval '%s seconds',
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message;
          $QUERY$,
          queue_name, queue_name, vt
      );

      FOR r IN
        EXECUTE sql USING qty
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

---- archive
---- removes a message from the queue, and sends it to the archive, where its
---- saved permanently.
CREATE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.q_%s
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pgmq.a_%s (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        queue_name, queue_name
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- archive
---- removes an array of message ids from the queue, and sends it to the archive,
---- where these messages will be saved permanently.
CREATE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.q_%s
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, message
        )
        INSERT INTO pgmq.a_%s (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        queue_name, queue_name
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes a message id from the queue permanently
CREATE FUNCTION pgmq.delete(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.q_%s
        WHERE msg_id = $1
        RETURNING msg_id
        $QUERY$,
        queue_name
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

---- delete
---- deletes an array of message ids from the queue permanently
CREATE FUNCTION pgmq.delete(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.q_%s
        WHERE msg_id = ANY($1)
        RETURNING msg_id
        $QUERY$,
        queue_name
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- send
-- sends a message to a queue, optionally with a delay
CREATE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.q_%s (vt, message)
        VALUES ((clock_timestamp() + interval '%s seconds'), $1)
        RETURNING msg_id;
        $QUERY$,
        queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msg;
END;
$$ LANGUAGE plpgsql;

-- send_batch
-- sends an array of list of messages to a queue, optionally with a delay
CREATE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER DEFAULT 0
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
BEGIN
    sql := FORMAT(
        $QUERY$
        INSERT INTO pgmq.q_%s (vt, message)
        SELECT clock_timestamp() + interval '%s seconds', unnest($1)
        RETURNING msg_id;
        $QUERY$,
        queue_name, delay
    );
    RETURN QUERY EXECUTE sql USING msgs;
END;
$$ LANGUAGE plpgsql;

-- returned by pgmq.metrics() and pgmq.metrics_all
CREATE TYPE pgmq.metrics_result AS (
    queue_name text,
    queue_length bigint,
    newest_msg_age_sec int,
    oldest_msg_age_sec int,
    total_messages bigint,
    scrape_time timestamp with time zone
);

-- get metrics for a single queue
CREATE OR REPLACE FUNCTION pgmq.metrics(queue_name TEXT)
RETURNS pgmq.metrics_result AS $$
DECLARE
    result_row pgmq.metrics_result;
    query TEXT;
BEGIN
    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                NOW() as scrape_time
            FROM pgmq.q_%s
        ),
        all_metrics AS (
            SELECT CASE
                WHEN is_called THEN last_value ELSE 0
                END as total_messages
            FROM pgmq.q_%s_msg_id_seq
        )
        SELECT 
            '%s' as queue_name,
            q_summary.queue_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages,
            q_summary.scrape_time
        FROM q_summary, all_metrics
        $QUERY$,
        queue_name, queue_name, queue_name
    );
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql;

-- get metrics for all queues
CREATE OR REPLACE FUNCTION pgmq."metrics_all"()
RETURNS SETOF pgmq.metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pgmq.metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
        result_row := pgmq.metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
