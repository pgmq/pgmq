-- FIFO queue support with message group keys
-- This migration adds support for FIFO queues using headers to specify message group IDs

-- Create the read_fifo function that respects FIFO ordering within message groups
CREATE FUNCTION pgmq.read_fifo(
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
        WITH fifo_groups AS (
            -- Determine the absolute head (oldest) message id per FIFO group, regardless of visibility
            SELECT 
                COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') AS fifo_key,
                MIN(msg_id) AS head_msg_id
            FROM pgmq.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
        ),
        eligible_groups AS (
            -- Only consider groups whose head message is currently visible and matches the optional filter
            -- Also acquire a transaction-level advisory lock on the group to prevent concurrent selection
            SELECT g.fifo_key, g.head_msg_id
            FROM fifo_groups g
            JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
            WHERE h.vt <= clock_timestamp()
            AND CASE
                WHEN %3$L != '{}'::jsonb THEN (h.message @> %3$L)::integer
                ELSE 1
            END = 1
            AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
        ),
        available_messages AS (
            -- Select the head message for each eligible group
            SELECT m.msg_id
            FROM pgmq.%4$I m
            INNER JOIN eligible_groups eg ON m.msg_id = eg.head_msg_id
            ORDER BY m.msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%5$I m
        SET
            vt = clock_timestamp() + %6$L,
            read_ct = read_ct + 1
        FROM available_messages am
        WHERE m.msg_id = am.msg_id
          AND m.vt <= clock_timestamp() -- prevent double reads if VT changed concurrently
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, qtable, conditional, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

-- Create read_fifo_with_poll function for polling support
CREATE FUNCTION pgmq.read_fifo_with_poll(
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
          WITH fifo_groups AS (
              -- Determine the absolute head (oldest) message id per FIFO group, regardless of visibility
              SELECT 
                  COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') AS fifo_key,
                  MIN(msg_id) AS head_msg_id
              FROM pgmq.%1$I
              GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
          ),
          eligible_groups AS (
              -- Only consider groups whose head message is currently visible and matches the optional filter
              -- Also acquire a transaction-level advisory lock on the group to prevent concurrent selection
              SELECT g.fifo_key, g.head_msg_id
              FROM fifo_groups g
              JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
              WHERE h.vt <= clock_timestamp()
              AND CASE
                  WHEN %3$L != '{}'::jsonb THEN (h.message @> %3$L)::integer
                  ELSE 1
              END = 1
              AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
          ),
          available_messages AS (
              -- Select the head message for each eligible group
              SELECT m.msg_id
              FROM pgmq.%4$I m
              INNER JOIN eligible_groups eg ON m.msg_id = eg.head_msg_id
              ORDER BY m.msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%5$I m
          SET
              vt = clock_timestamp() + %6$L,
              read_ct = read_ct + 1
          FROM available_messages am
          WHERE m.msg_id = am.msg_id
            AND m.vt <= clock_timestamp() -- prevent double reads if VT changed concurrently
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, qtable, conditional, qtable, qtable, make_interval(secs => vt)
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

-- Create an index on headers for better FIFO performance
-- This will help with the FIFO group lookups
CREATE OR REPLACE FUNCTION pgmq._create_fifo_index_if_not_exists(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    index_name TEXT := qtable || '_fifo_idx';
BEGIN
    -- Create GIN index on headers for efficient FIFO key lookups
    EXECUTE FORMAT(
        $QUERY$
        CREATE INDEX IF NOT EXISTS %I ON pgmq.%I USING GIN (headers);
        $QUERY$,
        index_name, qtable
    );
END;
$$ LANGUAGE plpgsql;

-- Helper function to create FIFO indexes on existing queues
CREATE FUNCTION pgmq.create_fifo_index(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq._create_fifo_index_if_not_exists(queue_name);
END;
$$ LANGUAGE plpgsql;

-- Helper function to create FIFO indexes on all existing queues
CREATE FUNCTION pgmq.create_fifo_indexes_all()
RETURNS void AS $$
DECLARE
    queue_record RECORD;
BEGIN
    FOR queue_record IN SELECT queue_name FROM pgmq.meta LOOP
        PERFORM pgmq.create_fifo_index(queue_record.queue_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create read_fifo_sqs_style function that mimics AWS SQS FIFO batch retrieval behavior
-- This function attempts to return as many messages as possible from the same message group
CREATE FUNCTION pgmq.read_fifo_sqs_style(
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
        WITH fifo_groups AS (
            -- Determine the absolute head (oldest) message id per FIFO group, regardless of visibility
            SELECT 
                COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') AS fifo_key,
                MIN(msg_id) AS head_msg_id
            FROM pgmq.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
        ),
        group_priorities AS (
            -- Consider only groups whose head is visible and matches the filter, and acquire a group lock
            SELECT 
                g.fifo_key,
                g.head_msg_id,
                ROW_NUMBER() OVER (ORDER BY g.head_msg_id) AS group_priority
            FROM fifo_groups g
            JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
            WHERE h.vt <= clock_timestamp()
            AND CASE
                WHEN %3$L != '{}'::jsonb THEN (h.message @> %3$L)::integer
                ELSE 1
            END = 1
            AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
        ),
        available_messages AS (
            -- Get messages prioritizing filling batch from earliest eligible group first
            SELECT 
                m.msg_id,
                gp.group_priority,
                ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY m.msg_id) AS msg_rank_in_group
            FROM pgmq.%4$I m
            INNER JOIN group_priorities gp ON 
                COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = gp.fifo_key
            WHERE m.vt <= clock_timestamp()
            AND CASE
                WHEN %3$L != '{}'::jsonb THEN (m.message @> %3$L)::integer
                ELSE 1
            END = 1
            AND m.msg_id >= gp.head_msg_id  -- Only messages from the group head onwards
        ),
        batch_selection AS (
            -- Select messages to fill batch, prioritizing earliest eligible group
            SELECT 
                msg_id,
                ROW_NUMBER() OVER (ORDER BY group_priority, msg_rank_in_group) AS overall_rank
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
        UPDATE pgmq.%5$I m
        SET
            vt = clock_timestamp() + %6$L,
            read_ct = read_ct + 1
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
          AND m.vt <= clock_timestamp() -- prevent double reads if VT changed concurrently
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, qtable, conditional, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

-- Create read_fifo_sqs_style_with_poll function for polling support
CREATE FUNCTION pgmq.read_fifo_sqs_style_with_poll(
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
          WITH fifo_groups AS (
              -- Determine the absolute head (oldest) message id per FIFO group, regardless of visibility
              SELECT 
                  COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') AS fifo_key,
                  MIN(msg_id) AS head_msg_id
              FROM pgmq.%1$I
              GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
          ),
          group_priorities AS (
              -- Consider only groups whose head is visible and matches the filter, and acquire a group lock
              SELECT 
                  g.fifo_key,
                  g.head_msg_id,
                  ROW_NUMBER() OVER (ORDER BY g.head_msg_id) AS group_priority
              FROM fifo_groups g
              JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
              WHERE h.vt <= clock_timestamp()
              AND CASE
                  WHEN %3$L != '{}'::jsonb THEN (h.message @> %3$L)::integer
                  ELSE 1
              END = 1
              AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
          ),
          available_messages AS (
              -- Get messages prioritizing filling batch from earliest eligible group first
              SELECT 
                  m.msg_id,
                  gp.group_priority,
                  ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY m.msg_id) AS msg_rank_in_group
              FROM pgmq.%4$I m
              INNER JOIN group_priorities gp ON 
                  COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = gp.fifo_key
              WHERE m.vt <= clock_timestamp()
              AND CASE
                  WHEN %3$L != '{}'::jsonb THEN (m.message @> %3$L)::integer
                  ELSE 1
              END = 1
              AND m.msg_id >= gp.head_msg_id  -- Only messages from the group head onwards
          ),
          batch_selection AS (
              -- Select messages to fill batch, prioritizing earliest eligible group
              SELECT 
                  msg_id,
                  ROW_NUMBER() OVER (ORDER BY group_priority, msg_rank_in_group) AS overall_rank
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
          UPDATE pgmq.%5$I m
          SET
              vt = clock_timestamp() + %6$L,
              read_ct = read_ct + 1
          FROM selected_messages sm
          WHERE m.msg_id = sm.msg_id
            AND m.vt <= clock_timestamp() -- prevent double reads if VT changed concurrently
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, qtable, conditional, qtable, qtable, make_interval(secs => vt)
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
