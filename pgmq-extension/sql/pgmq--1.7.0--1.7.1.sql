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
            -- Get the minimum msg_id for each FIFO group that's ready to be processed
            SELECT 
                COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') as fifo_key,
                MIN(msg_id) as min_msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp() 
            AND CASE
                WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                ELSE 1
            END = 1
            AND NOT EXISTS (
                -- Ensure no message in this group is currently being processed
                SELECT 1
                FROM pgmq.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-fifo', '_default_fifo_group') = 
                      COALESCE(pgmq.%I.headers->>'x-pgmq-fifo', '_default_fifo_group')
                AND m2.vt > clock_timestamp()
                AND m2.msg_id < pgmq.%I.msg_id
            )
            GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
        ),
        available_messages AS (
            -- Get messages that are the next in line for their FIFO group
            SELECT m.msg_id
            FROM pgmq.%I m
            INNER JOIN fifo_groups fg ON 
                COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = fg.fifo_key
                AND m.msg_id = fg.min_msg_id
            WHERE m.vt <= clock_timestamp()
            AND CASE
                WHEN %L != '{}'::jsonb THEN (m.message @> %2$L)::integer
                ELSE 1
            END = 1
            ORDER BY m.msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            vt = clock_timestamp() + %9$L,
            read_ct = read_ct + 1
        FROM available_messages am
        WHERE m.msg_id = am.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, conditional, qtable, qtable, qtable, qtable, conditional, qtable, make_interval(secs => vt)
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
              -- Get the minimum msg_id for each FIFO group that's ready to be processed
              SELECT 
                  COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') as fifo_key,
                  MIN(msg_id) as min_msg_id
              FROM pgmq.%I
              WHERE vt <= clock_timestamp() 
              AND CASE
                  WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                  ELSE 1
              END = 1
              AND NOT EXISTS (
                  -- Ensure no message in this group is currently being processed
                  SELECT 1
                  FROM pgmq.%I m2
                  WHERE COALESCE(m2.headers->>'x-pgmq-fifo', '_default_fifo_group') = 
                        COALESCE(pgmq.%I.headers->>'x-pgmq-fifo', '_default_fifo_group')
                  AND m2.vt > clock_timestamp()
                  AND m2.msg_id < pgmq.%I.msg_id
              )
              GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
          ),
          available_messages AS (
              -- Get messages that are the next in line for their FIFO group
              SELECT m.msg_id
              FROM pgmq.%I m
              INNER JOIN fifo_groups fg ON 
                  COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = fg.fifo_key
                  AND m.msg_id = fg.min_msg_id
              WHERE m.vt <= clock_timestamp()
              AND CASE
                  WHEN %L != '{}'::jsonb THEN (m.message @> %2$L)::integer
                  ELSE 1
              END = 1
              ORDER BY m.msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%I m
          SET
              vt = clock_timestamp() + %9$L,
              read_ct = read_ct + 1
          FROM available_messages am
          WHERE m.msg_id = am.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, conditional, qtable, qtable, qtable, qtable, conditional, qtable, make_interval(secs => vt)
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
            -- Find the minimum msg_id for each FIFO group that's ready to be processed
            SELECT 
                COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') as fifo_key,
                MIN(msg_id) as min_msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp() 
            AND CASE
                WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                ELSE 1
            END = 1
            GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
        ),
        locked_groups AS (
            -- Lock the first available message in each FIFO group
            SELECT 
                m.msg_id,
                fg.fifo_key
            FROM pgmq.%I m
            INNER JOIN fifo_groups fg ON 
                COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = fg.fifo_key
                AND m.msg_id = fg.min_msg_id
            WHERE m.vt <= clock_timestamp()
            AND CASE
                WHEN %L != '{}'::jsonb THEN (m.message @> %4$L)::integer
                ELSE 1
            END = 1
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
                COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = gp.fifo_key
            WHERE m.vt <= clock_timestamp()
            AND CASE
                WHEN %L != '{}'::jsonb THEN (m.message @> %6$L)::integer
                ELSE 1
            END = 1
            AND m.msg_id >= gp.min_msg_id  -- Only messages from min_msg_id onwards in each group
            AND NOT EXISTS (
                -- Ensure no earlier message in this group is currently being processed
                SELECT 1
                FROM pgmq.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-fifo', '_default_fifo_group') = 
                      COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group')
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
            read_ct = read_ct + 1
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, conditional, qtable, conditional, qtable, conditional, qtable, qtable, make_interval(secs => vt)
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
              -- Find the minimum msg_id for each FIFO group that's ready to be processed
              SELECT 
                  COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group') as fifo_key,
                  MIN(msg_id) as min_msg_id
              FROM pgmq.%I
              WHERE vt <= clock_timestamp() 
              AND CASE
                  WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
                  ELSE 1
              END = 1
              GROUP BY COALESCE(headers->>'x-pgmq-fifo', '_default_fifo_group')
          ),
          locked_groups AS (
              -- Lock the first available message in each FIFO group
              SELECT 
                  m.msg_id,
                  fg.fifo_key
              FROM pgmq.%I m
              INNER JOIN fifo_groups fg ON 
                  COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = fg.fifo_key
                  AND m.msg_id = fg.min_msg_id
              WHERE m.vt <= clock_timestamp()
              AND CASE
                  WHEN %L != '{}'::jsonb THEN (m.message @> %4$L)::integer
                  ELSE 1
              END = 1
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
                  COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group') = gp.fifo_key
              WHERE m.vt <= clock_timestamp()
              AND CASE
                  WHEN %L != '{}'::jsonb THEN (m.message @> %6$L)::integer
                  ELSE 1
              END = 1
              AND m.msg_id >= gp.min_msg_id  -- Only messages from min_msg_id onwards in each group
              AND NOT EXISTS (
                  -- Ensure no earlier message in this group is currently being processed
                  SELECT 1
                  FROM pgmq.%I m2
                  WHERE COALESCE(m2.headers->>'x-pgmq-fifo', '_default_fifo_group') = 
                        COALESCE(m.headers->>'x-pgmq-fifo', '_default_fifo_group')
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
              read_ct = read_ct + 1
          FROM selected_messages sm
          WHERE m.msg_id = sm.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, conditional, qtable, conditional, qtable, conditional, qtable, qtable, make_interval(secs => vt)
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