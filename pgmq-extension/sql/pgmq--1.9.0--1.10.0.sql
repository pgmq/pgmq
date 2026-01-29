-- Add last_read_at column to all existing queue tables (q_*) and archive tables (a_*)
DO $$
DECLARE
    queue_record RECORD;
    qtable TEXT;
    atable TEXT;
BEGIN
    FOR queue_record IN SELECT queue_name FROM pgmq.meta LOOP
        qtable := 'q_' || queue_record.queue_name;
        atable := 'a_' || queue_record.queue_name;

        -- Add last_read_at to queue table if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'pgmq'
            AND table_name = qtable
            AND column_name = 'last_read_at'
        ) THEN
            EXECUTE FORMAT('ALTER TABLE pgmq.%I ADD COLUMN last_read_at TIMESTAMP WITH TIME ZONE', qtable);
        END IF;

        -- Add last_read_at to archive table if it doesn't exist
        IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pgmq'
            AND table_name = atable
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'pgmq'
            AND table_name = atable
            AND column_name = 'last_read_at'
        ) THEN
            EXECUTE FORMAT('ALTER TABLE pgmq.%I ADD COLUMN last_read_at TIMESTAMP WITH TIME ZONE', atable);
        END IF;
    END LOOP;
END;
$$;

-- The functions that use this type are being dropped and recreated below
DROP TYPE IF EXISTS pgmq.message_record CASCADE;

CREATE TYPE pgmq.message_record AS (
    msg_id BIGINT,
    read_ct INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt TIMESTAMP WITH TIME ZONE,
    message JSONB,
    headers JSONB
);

------------------------------------------------------------
-- Migration: Updated functions
------------------------------------------------------------

DROP FUNCTION IF EXISTS pgmq.read_grouped_rr(
queue_name TEXT,
    vt INTEGER,
    qty INTEGER
);
CREATE FUNCTION pgmq.read_grouped_rr(
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
        eligible_groups AS (
            -- Only groups whose head message is currently visible
            -- Acquire a transaction-level advisory lock per group to prevent concurrent selection
            SELECT
                g.fifo_key,
                g.head_msg_id,
                ROW_NUMBER() OVER (ORDER BY g.head_msg_id) AS group_priority
            FROM fifo_groups g
            JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
            WHERE h.vt <= clock_timestamp()
              AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
        ),
        available_messages AS (
            -- All currently visible messages starting at the head for each eligible group
            SELECT
                m.msg_id,
                eg.group_priority,
                ROW_NUMBER() OVER (
                    PARTITION BY eg.fifo_key
                    ORDER BY m.msg_id
                ) AS msg_rank_in_group
            FROM pgmq.%3$I m
            JOIN eligible_groups eg
              ON COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group') = eg.fifo_key
            WHERE m.vt <= clock_timestamp()
              AND m.msg_id >= eg.head_msg_id
        ),
        ordered_messages AS (
            -- Layered round-robin: take rank 1 of all groups by group_priority, then rank 2, etc.
            -- Assign selection order before locking
            SELECT msg_id, ROW_NUMBER() OVER (ORDER BY msg_rank_in_group, group_priority) as selection_order
            FROM available_messages
        ),
        selected_messages AS (
            -- Lock the messages in the correct order, preserving selection_order
            SELECT om.msg_id, om.selection_order
            FROM ordered_messages om
            JOIN pgmq.%4$I m ON m.msg_id = om.msg_id
            WHERE om.selection_order <= $1
            ORDER BY om.selection_order
            FOR UPDATE OF m SKIP LOCKED
        ),
        updated_messages AS (
            UPDATE pgmq.%5$I m
            SET
                vt = clock_timestamp() + %6$L,
                read_ct = read_ct + 1,
                last_read_at = clock_timestamp()
            FROM selected_messages sm
            WHERE m.msg_id = sm.msg_id
              AND m.vt <= clock_timestamp() -- final guard to avoid duplicate reads under races
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers, sm.selection_order
        )
        SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
        FROM updated_messages
        ORDER BY selection_order;
        $QUERY$,
        qtable, qtable, qtable, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS pgmq.read_grouped_rr_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER,
    poll_interval_ms INTEGER
);
CREATE FUNCTION pgmq.read_grouped_rr_with_poll(
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
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      FOR r IN
        SELECT * FROM pgmq.read_grouped_rr(queue_name, vt, qty)
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


DROP FUNCTION IF EXISTS pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    conditional JSONB
);
CREATE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp() AND CASE
                WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                ELSE 1
            END = 1
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            last_read_at = clock_timestamp(),
            vt = clock_timestamp() + %L,
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.*;
        $QUERY$,
        qtable, conditional, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pgmq.read_grouped(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
);
CREATE FUNCTION pgmq.read_grouped(
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
            -- Find the minimum msg_id for each FIFO group that's ready to be processed
            SELECT
                COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') as fifo_key,
                MIN(msg_id) as min_msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp()
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        locked_groups AS (
            -- Lock the first available message in each FIFO group
            SELECT
                m.msg_id,
                fg.fifo_key
            FROM pgmq.%I m
            INNER JOIN fifo_groups fg ON
                COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group') = fg.fifo_key
                AND m.msg_id = fg.min_msg_id
            WHERE m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC
            FOR UPDATE SKIP LOCKED
        ),
        group_priorities AS (
            -- Assign priority to groups based on their oldest message
            SELECT
                fifo_key,
                msg_id as min_msg_id,
                ROW_NUMBER() OVER (ORDER BY msg_id) as group_priority
            FROM locked_groups
        ),
        available_messages AS (
            -- Get messages prioritizing filling batch from earliest group first
            SELECT
                m.msg_id,
                gp.group_priority,
                ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY m.msg_id) as msg_rank_in_group
            FROM pgmq.%I m
            INNER JOIN group_priorities gp ON
                COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
            WHERE m.vt <= clock_timestamp()
            AND m.msg_id >= gp.min_msg_id  -- Only messages from min_msg_id onwards in each group
            AND NOT EXISTS (
                -- Ensure no earlier message in this group is currently being processed
                SELECT 1
                FROM pgmq.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-group', '_default_fifo_group') =
                      COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group')
                AND m2.vt > clock_timestamp()
                AND m2.msg_id < m.msg_id
            )
        ),
        batch_selection AS (
            -- Select messages to fill batch, prioritizing earliest group
            SELECT
                msg_id,
                ROW_NUMBER() OVER (ORDER BY group_priority, msg_rank_in_group) as overall_rank
            FROM available_messages
        ),
        selected_messages AS (
            -- Limit to requested quantity
            SELECT msg_id
            FROM batch_selection
            WHERE overall_rank <= $1
            ORDER BY msg_id
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            vt = clock_timestamp() + %L,
            read_ct = read_ct + 1,
            last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, qtable, qtable, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS pgmq.read_grouped_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER,
    poll_interval_ms INTEGER
);
CREATE FUNCTION pgmq.read_grouped_with_poll(
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
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      FOR r IN
        SELECT * FROM pgmq.read_grouped(queue_name, vt, qty)
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

DROP FUNCTION IF EXISTS pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER,
    poll_interval_ms INTEGER,
    conditional JSONB
);
CREATE FUNCTION pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMP;
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      sql := FORMAT(
          $QUERY$
          WITH cte AS
          (
              SELECT msg_id
              FROM pgmq.%I
              WHERE vt <= clock_timestamp() AND CASE
                  WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                  ELSE 1
              END = 1
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%I m
          SET
              last_read_at = clock_timestamp(),
              vt = clock_timestamp() + %L,
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.*;
          $QUERY$,
          qtable, conditional, qtable, make_interval(secs => vt)
      );

      FOR r IN
        EXECUTE sql USING qty
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

-- Update archive functions to include last_read_at column
CREATE OR REPLACE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH archived AS (
            DELETE FROM pgmq.%I
            WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        FROM archived
        RETURNING msg_id;
        $QUERY$,
        qtable, atable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pgmq.pop(
    queue_name TEXT,
    qty INTEGER
);
CREATE FUNCTION pgmq.pop(queue_name TEXT, qty INTEGER DEFAULT 1)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH cte AS
            (
                SELECT msg_id
                FROM pgmq.%I
                WHERE vt <= clock_timestamp()
                ORDER BY msg_id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
        DELETE from pgmq.%I
        WHERE msg_id IN (select msg_id from cte)
        RETURNING *;
        $QUERY$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

-- Drop old set_vt overloads (they'll be recreated below)
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT, INTEGER);
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT[], INTEGER);
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT[], TIMESTAMP WITH TIME ZONE);

-- Sets timestamp vt of a message, returns it (base implementation)
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt TIMESTAMP WITH TIME ZONE)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = $1
        WHERE msg_id = $2
        RETURNING *;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_id;
END;
$$ LANGUAGE plpgsql;

-- Sets integer vt of a message, returns it
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pgmq.message_record AS $$
    SELECT * FROM pgmq.set_vt(queue_name, msg_id, clock_timestamp() + make_interval(secs => vt));
$$ LANGUAGE sql;

-- Sets timestamp vt of multiple messages, returns them (base implementation)
CREATE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt TIMESTAMP WITH TIME ZONE
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = $1
        WHERE msg_id = ANY($2)
        RETURNING *;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_ids;
END;
$$ LANGUAGE plpgsql;

-- Sets integer vt of multiple messages, returns them
CREATE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
    SELECT * FROM pgmq.set_vt(queue_name, msg_ids, clock_timestamp() + make_interval(secs => vt));
$$ LANGUAGE sql;
