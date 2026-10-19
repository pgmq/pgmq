-- Extension scripts normally run with pgmq before pg_catalog. A queue creator
-- can add functions in pgmq, so keep the upgrade itself on a trusted path.
SET LOCAL search_path = pg_catalog, pg_temp;

-- Insert notifications never fired for a queue whose name is not all lowercase.
-- notify_queue_listeners() rebuilt the queue name from the trigger table name,
-- but format_table_name lowercases it, so the queue MyQueue lives in
-- pgmq.q_myqueue and the rebuilt name (myqueue) matched no row in
-- notify_insert_throttle: the throttle UPDATE touched nothing, so the guarded
-- PG_NOTIFY was never reached. The queue name is now passed to the trigger
-- function as an argument by enable_notify_insert.

CREATE OR REPLACE FUNCTION pgmq.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        qtable := pgmq.format_table_name(throttle_record.queue_name::pg_catalog.text, 'q'::pg_catalog.text);

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


-- Security hardening: pin search_path and schema-qualify built-ins (GHSA private)
--
-- pgmq functions are SECURITY INVOKER and previously pinned no search_path, so
-- unqualified references to built-in functions, operators and casts were
-- resolved against the caller's search_path at run time. A role able to create
-- objects in a schema on a privileged caller's search_path (e.g. public) could
-- plant an exact-signature object that shadowed a built-in and have it executed
-- with the caller's privileges. The insert-notification trigger function was
-- especially exposed because it runs implicitly, with the inserting role's
-- search_path, so a pinned search_path is its only defense.
--
-- Functions below pin `SET search_path = pg_catalog, pg_temp`, which also
-- governs dynamic SQL. The public partition wrappers are the exception: their
-- pinned implementations call pg_partman, which changes the session path, so
-- the wrappers restore the caller's value. Internal pgmq calls use exact
-- argument types to prevent attacker-created overloads in pgmq.

CREATE OR REPLACE FUNCTION pgmq.acquire_queue_lock(queue_name TEXT)
RETURNS void AS $$
BEGIN
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pgmq.queue_' || queue_name));
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_grouped_rr(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        qtable, qtable, qtable, qtable, qtable, pg_catalog.make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_grouped_rr_with_poll(
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
    stop_at := clock_timestamp() + pg_catalog.make_interval(secs => max_poll_seconds);
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
        PERFORM pg_catalog.pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_grouped_head(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        qtable, pg_catalog.make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

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
    stop_at := clock_timestamp() + pg_catalog.make_interval(secs => max_poll_seconds);
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
        PERFORM pg_catalog.pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.format_table_name(queue_name text, prefix text)
RETURNS TEXT AS $$
BEGIN
    IF queue_name ~ '\$|;|--|'''
    THEN
        RAISE EXCEPTION 'queue name contains invalid characters: $, ;, --, or \''';
    END IF;
    RETURN lower(prefix || '_' || queue_name);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, conditional, qtable, pg_catalog.make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_grouped(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        filtered_groups as (
            SELECT * FROM group_priorities gp
            WHERE NOT EXISTS (
                -- Ensure no earlier message in this group is currently being processed
                SELECT 1
                FROM pgmq.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
                AND m2.vt > clock_timestamp()
                AND m2.msg_id < gp.min_msg_id
            )
        ),
        available_messages as (
            SELECT gp.fifo_key, t.msg_id,gp.group_priority,
                ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY t.msg_id) as msg_rank_in_group
            FROM filtered_groups gp
            CROSS JOIN LATERAL (
                SELECT *
                FROM pgmq.%I t
                WHERE COALESCE(t.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
                AND t.vt <= clock_timestamp()
                ORDER BY msg_id
                LIMIT $1  -- tip to limit query impact, we know we need at most qty in each group
            ) t
            ORDER BY gp.group_priority
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
        qtable, qtable, qtable, qtable, qtable, pg_catalog.make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_grouped_with_poll(
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
    stop_at := clock_timestamp() + pg_catalog.make_interval(secs => max_poll_seconds);
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
        PERFORM pg_catalog.pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.read_with_poll(
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
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    stop_at := clock_timestamp() + pg_catalog.make_interval(secs => max_poll_seconds);
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
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, conditional, qtable, pg_catalog.make_interval(secs => vt)
      );

      FOR r IN
        EXECUTE sql USING qty
      LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_catalog.pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
    atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
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
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.archive(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
    atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
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
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.delete(
    queue_name TEXT,
    msg_id BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BIGINT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.%I
        WHERE msg_id = $1
        RETURNING msg_id
        $QUERY$,
        qtable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN NOT (result IS NULL);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.delete(
    queue_name TEXT,
    msg_ids BIGINT[]
)
RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
        $QUERY$
        DELETE FROM pgmq.%I
        WHERE msg_id = ANY($1)
        RETURNING msg_id
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB,
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
            $QUERY$
        INSERT INTO pgmq.%I (vt, message, headers)
        VALUES ($2, $1, $3)
        RETURNING msg_id;
        $QUERY$,
            qtable
           );
    RETURN QUERY EXECUTE sql USING msg, delay, headers;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL::pg_catalog.jsonb, clock_timestamp());
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, headers, clock_timestamp());
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL::pg_catalog.jsonb, clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, NULL::pg_catalog.jsonb, delay);
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send(
    queue_name TEXT,
    msg JSONB,
    headers JSONB,
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send(queue_name, msg, headers, clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._validate_batch_params(
    msgs JSONB[],
    headers JSONB[]
) RETURNS void AS $$
BEGIN
    -- Validate that msgs is not NULL or empty
    IF msgs IS NULL OR array_length(msgs, 1) IS NULL THEN
        RAISE EXCEPTION 'msgs cannot be NULL or empty';
    END IF;

    -- Validate that headers array length matches msgs array length if headers is provided
    -- Note: array_length returns NULL for empty arrays, so we use COALESCE to treat empty arrays as length 0
    IF headers IS NOT NULL AND COALESCE(array_length(headers, 1), 0) != COALESCE(array_length(msgs, 1), 0) THEN
        RAISE EXCEPTION 'headers array length (%) must match msgs array length (%)',
            COALESCE(array_length(headers, 1), 0), COALESCE(array_length(msgs, 1), 0);
    END IF;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
            $QUERY$
        INSERT INTO pgmq.%I (vt, message, headers)
        SELECT $2, unnest($1), unnest(coalesce($3, ARRAY[]::jsonb[]))
        RETURNING msg_id;
        $QUERY$,
            qtable
           );
    RETURN QUERY EXECUTE sql USING msgs, delay, headers;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
BEGIN
    PERFORM pgmq._validate_batch_params(msgs, headers);
    RETURN QUERY SELECT * FROM pgmq._send_batch(queue_name, msgs, headers, delay);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[]
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL::pg_catalog.jsonb[], clock_timestamp());
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[]
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, headers, clock_timestamp());
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL::pg_catalog.jsonb[], clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, NULL::pg_catalog.jsonb[], delay);
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay INTEGER
) RETURNS SETOF BIGINT AS $$
    SELECT * FROM pgmq.send_batch(queue_name, msgs, headers, clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.metrics(queue_name TEXT)
RETURNS pgmq.metrics_result AS $$
DECLARE
    result_row pgmq.metrics_result;
    query TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
    q_default_partition TEXT := qtable || '_default';
    a_default_partition TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text) || '_default';
    default_partition_length BIGINT;
BEGIN
    -- Only partitioned queues have default partitions. Messages in them have no
    -- partition of their own, which means pg_partman maintenance is failing for
    -- this queue; a non-zero value here is the signal to act on. The planner's
    -- estimate is used so that a large spill does not slow down every scrape.
    IF to_regclass(FORMAT('pgmq.%I', q_default_partition)) IS NOT NULL THEN
        SELECT COALESCE(SUM(GREATEST(c.reltuples, 0))::bigint, 0)
        INTO default_partition_length
        FROM pg_class c
        WHERE c.oid IN (
            to_regclass(FORMAT('pgmq.%I', q_default_partition)),
            to_regclass(FORMAT('pgmq.%I', a_default_partition))
        );
    END IF;

    query := FORMAT(
        $QUERY$
        WITH q_summary AS (
            SELECT
                count(*) as queue_length,
                count(CASE WHEN vt <= NOW() THEN 1 END) as queue_visible_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
                NOW() as scrape_time
            FROM pgmq.%I
        ),
        all_metrics AS (
            SELECT CASE
                WHEN is_called THEN last_value ELSE 0
                END as total_messages
            FROM pgmq.%I
        )
        SELECT
            %L as queue_name,
            q_summary.queue_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages,
            q_summary.scrape_time,
            q_summary.queue_visible_length,
            %L::bigint as default_partition_length
        FROM q_summary, all_metrics
        $QUERY$,
        qtable, qtable || '_msg_id_seq', queue_name, default_partition_length
    );
    EXECUTE query INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq."metrics_all"()
RETURNS SETOF pgmq.metrics_result AS $$
DECLARE
    row_name RECORD;
    result_row pgmq.metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
        result_row := pgmq.metrics(row_name.queue_name::pg_catalog.text);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq."list_queues"()
RETURNS SETOF pgmq.queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM pgmq.meta;
END
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq."purge_queue"(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
  deleted_count INTEGER;
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
  -- Get the row count before truncating
  EXECUTE format('SELECT count(*) FROM pgmq.%I', qtable) INTO deleted_count;

  -- Use TRUNCATE for better performance on large tables
  EXECUTE format('TRUNCATE TABLE pgmq.%I', qtable);

  -- Return the number of purged rows
  RETURN deleted_count;
END
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq."detach_archive"(queue_name TEXT)
RETURNS VOID AS $$
DECLARE
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
BEGIN
  RAISE WARNING 'detach_archive(queue_name) is deprecated and is a no-op. It will be removed in PGMQ v2.0. Archive tables are no longer member objects.';
END
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.pop(queue_name TEXT, qty INTEGER DEFAULT 1)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $QUERY$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt TIMESTAMP WITH TIME ZONE)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    result pgmq.message_record;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = $1
        WHERE msg_id = $2
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $QUERY$, 
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_id;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF pgmq.message_record AS $$
    SELECT * FROM pgmq.set_vt(queue_name, msg_id, clock_timestamp() + pg_catalog.make_interval(secs => vt));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt TIMESTAMP WITH TIME ZONE
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
BEGIN
    sql := FORMAT(
        $QUERY$
        UPDATE pgmq.%I
        SET vt = $1
        WHERE msg_id = ANY($2)
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $QUERY$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_ids;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.set_vt(
    queue_name TEXT,
    msg_ids BIGINT[],
    vt INTEGER
)
RETURNS SETOF pgmq.message_record AS $$
    SELECT * FROM pgmq.set_vt(queue_name, msg_ids, clock_timestamp() + pg_catalog.make_interval(secs => vt));
$$ LANGUAGE sql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._get_pg_partman_schema()
RETURNS TEXT AS $$
  SELECT
    extnamespace::regnamespace::text
  FROM
    pg_extension
  WHERE
    extname = 'pg_partman';
$$ LANGUAGE SQL SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
    fq_atable TEXT := 'pgmq.' || atable;
BEGIN
    RAISE WARNING 'drop_queue(queue_name, partitioned) is deprecated and will be removed in PGMQ v2.0. Use drop_queue(queue_name) instead';

    PERFORM pgmq.drop_queue(queue_name);

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.drop_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
    qtable_seq TEXT := qtable || '_msg_id_seq';
    fq_qtable TEXT := 'pgmq.' || qtable;
    atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
    fq_atable TEXT := 'pgmq.' || atable;
    partitioned BOOLEAN;
BEGIN
    PERFORM pgmq.acquire_queue_lock(queue_name);
    EXECUTE FORMAT(
        $QUERY$
        SELECT is_partitioned FROM pgmq.meta WHERE queue_name = %L
        $QUERY$,
        queue_name
    ) INTO partitioned;

    -- check if the queue exists
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = qtable and table_schema = 'pgmq'
    ) THEN
        RAISE NOTICE 'pgmq queue `%` does not exist', queue_name;
        RETURN FALSE;
    END IF;

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        qtable
    );

    EXECUTE FORMAT(
        $QUERY$
        DROP TABLE IF EXISTS pgmq.%I
        $QUERY$,
        atable
    );

     IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = 'meta' and table_schema = 'pgmq'
     ) THEN
        EXECUTE FORMAT(
            $QUERY$
            DELETE FROM pgmq.meta WHERE queue_name = %L
            $QUERY$,
            queue_name
        );
     END IF;

     IF partitioned THEN
        EXECUTE FORMAT(
          $QUERY$
          DELETE FROM %I.part_config where parent_table in (%L, %L)
          $QUERY$,
          pgmq._get_pg_partman_schema(), fq_qtable, fq_atable
        );
     END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
  IF length(queue_name) > 47 THEN
    -- complete table identifier must be <= 63
    -- https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    -- e.g. template_pgmq_q_my_queue is an identifier for my_queue when partitioned
    -- template_pgmq_q_ (16) + <a max length queue name> (47) = 63 
    RAISE EXCEPTION 'queue name is too long, maximum length is 47 characters';
  END IF;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._belongs_to_pgmq(table_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    sql TEXT;
    result BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_depend
    WHERE refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pgmq')
    AND objid = (
        SELECT oid
        FROM pg_class
        WHERE relname = table_name
    )
  ) INTO result;
  RETURN result;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  -- Let pg_monitor compute queue metrics without exposing message payloads.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);

END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);

  EXECUTE FORMAT(
    $QUERY$
    CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    )
    $QUERY$,
    qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT PRIMARY KEY,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    );
    $QUERY$,
    atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC);
    $QUERY$,
    qtable || '_vt_idx', qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, false, true)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  -- Let pg_monitor compute queue metrics without exposing message payloads.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._get_partition_col(partition_interval TEXT)
RETURNS TEXT AS $$
DECLARE
  num INTEGER;
BEGIN
    BEGIN
        num := partition_interval::INTEGER;
        RETURN 'msg_id';
    EXCEPTION
        WHEN others THEN
            RETURN 'enqueued_at';
    END;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._extension_exists(extension_name TEXT)
    RETURNS BOOLEAN
    LANGUAGE SQL
    SET search_path = pg_catalog, pg_temp
AS $$
SELECT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = extension_name
)
$$;

CREATE OR REPLACE FUNCTION pgmq._ensure_pg_partman_installed()
RETURNS void AS $$
BEGIN
  IF NOT pgmq._extension_exists('pg_partman'::pg_catalog.text) THEN
    RAISE EXCEPTION 'pg_partman is required for partitioned queues';
  END IF;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._get_pg_partman_major_version()
RETURNS INT
LANGUAGE SQL
    SET search_path = pg_catalog, pg_temp
AS $$
  SELECT split_part(extversion, '.', 1)::INT
  FROM pg_extension
  WHERE extname = 'pg_partman'
$$;

-- pg_partman.create_parent uses set_config(..., false). Keep its side effect
-- inside the implementation, then restore the caller's path in the wrapper.
CREATE OR REPLACE FUNCTION pgmq._create_partitioned(
  queue_name TEXT,
  partition_interval TEXT,
  retention_interval TEXT,
  premake INTEGER
)
RETURNS void AS $$
DECLARE
  partition_col TEXT;
  a_partition_col TEXT;
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
  qtable_seq TEXT := qtable || '_msg_id_seq';
  atable TEXT := pgmq.format_table_name(queue_name, 'a'::pg_catalog.text);
  fq_qtable TEXT := 'pgmq.' || qtable;
  fq_atable TEXT := 'pgmq.' || atable;
BEGIN
  PERFORM pgmq.validate_queue_name(queue_name);
  PERFORM pgmq.acquire_queue_lock(queue_name);
  PERFORM pgmq._ensure_pg_partman_installed();
  IF premake < 1 THEN
    RAISE EXCEPTION 'premake must be at least 1, got %', premake;
  END IF;
  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
        msg_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
    ) PARTITION BY RANGE (%I)
    $QUERY$,
    qtable, partition_col
  );

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_premake := %s,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_qtable,
    partition_col,
    partition_interval,
    premake
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (%I);
    $QUERY$,
    qtable || '_part_idx', qtable, partition_col
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || qtable
  );

  EXECUTE FORMAT(
    $QUERY$
    INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
    VALUES (%L, true, false)
    ON CONFLICT
    DO NOTHING;
    $QUERY$,
    queue_name
  );

  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE FORMAT(
    $QUERY$
    CREATE TABLE IF NOT EXISTS pgmq.%I (
      msg_id BIGINT NOT NULL,
      read_ct INT DEFAULT 0 NOT NULL,
      enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      last_read_at TIMESTAMP WITH TIME ZONE,
      archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
      vt TIMESTAMP WITH TIME ZONE NOT NULL,
      message JSONB,
      headers JSONB
    ) PARTITION BY RANGE (%I);
    $QUERY$,
    atable, a_partition_col
  );

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_premake := %s,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    fq_atable,
    a_partition_col,
    partition_interval,
    premake
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
        retention = %L,
        retention_keep_table = false,
        retention_keep_index = true,
        automatic_maintenance = 'on'
    WHERE parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    'pgmq.' || atable
  );

  EXECUTE FORMAT(
    $QUERY$
    CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at);
    $QUERY$,
    'archived_at_idx_' || queue_name, atable
  );

  -- Let pg_monitor compute queue metrics without exposing message payloads.
  EXECUTE FORMAT('GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor', qtable);
  EXECUTE FORMAT('GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor', qtable_seq);

END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_partitioned(
  queue_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000',
  premake INTEGER DEFAULT 4
)
RETURNS void AS $$
DECLARE
  caller_path pg_catalog.text := pg_catalog.current_setting('search_path');
BEGIN
  PERFORM pgmq._create_partitioned(queue_name, partition_interval, retention_interval, premake);
  IF NOT (pg_catalog.current_setting('search_path') OPERATOR(pg_catalog.=) caller_path) THEN
    PERFORM pg_catalog.set_config('search_path', caller_path, false);
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq.create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._create_fifo_index_if_not_exists(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_fifo_index(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq._create_fifo_index_if_not_exists(queue_name);
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.create_fifo_indexes_all()
RETURNS void AS $$
DECLARE
    queue_record RECORD;
BEGIN
    FOR queue_record IN SELECT queue_name FROM pgmq.meta LOOP
        PERFORM pgmq.create_fifo_index(queue_record.queue_name::pg_catalog.text);
    END LOOP;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq._convert_archive_partitioned(
  table_name TEXT,
  partition_interval TEXT,
  retention_interval TEXT,
  leading_partition INT
)
RETURNS void AS $$
DECLARE
  a_table_name TEXT := pgmq.format_table_name(table_name, 'a'::pg_catalog.text);
  a_table_name_old TEXT := pgmq.format_table_name(table_name, 'a'::pg_catalog.text) || '_old';
  qualified_a_table_name TEXT := format('pgmq.%I', a_table_name);
  partition_col TEXT;
  a_partition_col TEXT;
BEGIN

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'p';

  IF FOUND THEN
    RAISE NOTICE 'Table %s is already partitioned', a_table_name;
    RETURN;
  END IF;

  PERFORM c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = a_table_name
    AND c.relkind = 'r';

  IF NOT FOUND THEN
    RAISE NOTICE 'Table %s does not exists', a_table_name;
    RETURN;
  END IF;

  SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

  -- For archive tables, use archived_at for time-based partitioning
  IF partition_col = 'enqueued_at' THEN
    a_partition_col := 'archived_at';
  ELSE
    a_partition_col := partition_col;
  END IF;

  EXECUTE 'ALTER TABLE ' || qualified_a_table_name || ' RENAME TO ' || a_table_name_old;

  -- When partitioning by time (archived_at), we need to exclude constraints and indexes
  -- because the existing PRIMARY KEY on msg_id alone is incompatible with partitioning by archived_at.
  -- When partitioning by msg_id, we can keep all constraints including PRIMARY KEY.
  IF a_partition_col = 'archived_at' THEN
    EXECUTE format( 'CREATE TABLE pgmq.%I (LIKE pgmq.%I including defaults including generated including storage including comments) PARTITION BY RANGE (%I)', a_table_name, a_table_name_old, a_partition_col );
  ELSE
    EXECUTE format( 'CREATE TABLE pgmq.%I (LIKE pgmq.%I including all) PARTITION BY RANGE (%I)', a_table_name, a_table_name_old, a_partition_col );
  END IF;

  EXECUTE 'ALTER INDEX pgmq.archived_at_idx_' || table_name || ' RENAME TO archived_at_idx_' || table_name || '_old';
  EXECUTE 'CREATE INDEX archived_at_idx_'|| table_name || ' ON ' || qualified_a_table_name ||'(archived_at)';

  -- https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md
  -- p_parent_table - the existing parent table. MUST be schema qualified, even if in public schema.
  EXECUTE FORMAT(
    $QUERY$
    SELECT %I.create_parent(
      p_parent_table := %L,
      p_control := %L,
      p_interval := %L,
      p_type := case
        when pgmq._get_pg_partman_major_version() = 5 then 'range'
        else 'native'
      end
    )
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    qualified_a_table_name,
    a_partition_col,
    partition_interval
  );

  EXECUTE FORMAT(
    $QUERY$
    UPDATE %I.part_config
    SET
      retention = %L,
      retention_keep_table = false,
      retention_keep_index = false,
      infinite_time_partitions = true
    WHERE
      parent_table = %L;
    $QUERY$,
    pgmq._get_pg_partman_schema(),
    retention_interval,
    qualified_a_table_name
  );
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.convert_archive_partitioned(
  table_name TEXT,
  partition_interval TEXT DEFAULT '10000',
  retention_interval TEXT DEFAULT '100000',
  leading_partition INT DEFAULT 10
)
RETURNS void AS $$
DECLARE
  caller_path pg_catalog.text := pg_catalog.current_setting('search_path');
BEGIN
  PERFORM pgmq._convert_archive_partitioned(table_name, partition_interval, retention_interval, leading_partition);
  IF NOT (pg_catalog.current_setting('search_path') OPERATOR(pg_catalog.=) caller_path) THEN
    PERFORM pg_catalog.set_config('search_path', caller_path, false);
  END IF;
END;
$$ LANGUAGE plpgsql;

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
             (throttle_interval_ms OPERATOR(pg_catalog.*) INTERVAL '1 millisecond') -- Throttle interval has elapsed
    );

  -- Check how many rows were updated (will be 0 or 1)
  GET DIAGNOSTICS updated_count = ROW_COUNT;

  IF updated_count > 0 THEN
    PERFORM PG_NOTIFY('pgmq.q_' || v_queue_name || '.' || TG_OP, NULL);
  END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
          last_notified_at = pg_catalog.to_timestamp(0);

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
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.disable_notify_insert(queue_name TEXT)
RETURNS void AS $$
DECLARE
  qtable TEXT := pgmq.format_table_name(queue_name, 'q'::pg_catalog.text);
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
$$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION pgmq.list_notify_insert_throttles()
    RETURNS TABLE
            (
                queue_name           text,
                throttle_interval_ms integer,
                last_notified_at     TIMESTAMP WITH TIME ZONE
            )
    LANGUAGE sql
    STABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT queue_name, throttle_interval_ms, last_notified_at
    FROM pgmq.notify_insert_throttle
    ORDER BY queue_name;
$$;

CREATE OR REPLACE FUNCTION pgmq.update_notify_insert(queue_name text, throttle_interval_ms integer)
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    IF throttle_interval_ms < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative, got: %', throttle_interval_ms;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE meta.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist. Create the queue first using pgmq.create()', queue_name;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.notify_insert_throttle WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not have notify_insert enabled. Enable it first using pgmq.enable_notify_insert()', queue_name;
    END IF;

    UPDATE pgmq.notify_insert_throttle
    SET throttle_interval_ms = update_notify_insert.throttle_interval_ms,
        last_notified_at = pg_catalog.to_timestamp(0)
    WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.validate_routing_key(routing_key text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    -- Valid routing key examples:
    --   "logs.error"
    --   "app.user-service.auth"
    --   "system_events.db.connection_failed"
    --
    -- Invalid routing key examples:
    --   ""                     - empty
    --   ".logs.error"          - starts with dot
    --   "logs.error."          - ends with dot
    --   "logs..error"          - consecutive dots
    --   "logs.error!"          - invalid character
    --   "logs error"           - space not allowed
    --   "logs.*"               - wildcards not allowed in routing keys

    IF routing_key IS NULL OR routing_key = '' THEN
        RAISE EXCEPTION 'routing_key cannot be NULL or empty';
    END IF;

    IF length(routing_key) > 255 THEN
        RAISE EXCEPTION 'routing_key length cannot exceed 255 characters, got % characters', length(routing_key);
    END IF;

    IF routing_key !~ '^[a-zA-Z0-9._-]+$' THEN
        RAISE EXCEPTION 'routing_key contains invalid characters. Only alphanumeric, dots, hyphens, and underscores are allowed. Got: %', routing_key;
    END IF;

    IF routing_key ~ '^\.' THEN
        RAISE EXCEPTION 'routing_key cannot start with a dot. Got: %', routing_key;
    END IF;

    IF routing_key ~ '\.$' THEN
        RAISE EXCEPTION 'routing_key cannot end with a dot. Got: %', routing_key;
    END IF;

    IF routing_key ~ '\.\.' THEN
        RAISE EXCEPTION 'routing_key cannot contain consecutive dots. Got: %', routing_key;
    END IF;

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.validate_topic_pattern(pattern text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    -- Valid pattern examples:
    --   "logs.*"           - matches one segment after logs. (e.g., logs.error, logs.info)
    --   "logs.#"           - matches one or more segments after logs. (e.g., logs.error, logs.api.error)
    --   "*.error"          - matches one segment before .error (e.g., app.error, db.error)
    --   "#.error"          - matches one or more segments before .error (e.g., app.error, x.y.error)
    --   "app.*.#"          - mixed wildcards (one segment then one or more)
    --   "#"                - catch-all pattern, matches any routing key
    --
    -- Invalid pattern examples:
    --   ".logs.*"          - starts with dot
    --   "logs.*."          - ends with dot
    --   "logs..error"      - consecutive dots
    --   "logs.**"          - consecutive stars
    --   "logs.##"          - consecutive hashes
    --   "logs.*#"          - adjacent wildcards
    --   "logs.error!"      - invalid character

    IF pattern IS NULL OR pattern = '' THEN
        RAISE EXCEPTION 'pattern cannot be NULL or empty';
    END IF;

    IF length(pattern) > 255 THEN
        RAISE EXCEPTION 'pattern length cannot exceed 255 characters, got % characters', length(pattern);
    END IF;

    IF pattern !~ '^[a-zA-Z0-9._\-*#]+$' THEN
        RAISE EXCEPTION 'pattern contains invalid characters. Only alphanumeric, dots, hyphens, underscores, *, and # are allowed. Got: %', pattern;
    END IF;

    IF pattern ~ '^\.' THEN
        RAISE EXCEPTION 'pattern cannot start with a dot. Got: %', pattern;
    END IF;

    IF pattern ~ '\.$' THEN
        RAISE EXCEPTION 'pattern cannot end with a dot. Got: %', pattern;
    END IF;

    IF pattern ~ '\.\.' THEN
        RAISE EXCEPTION 'pattern cannot contain consecutive dots. Got: %', pattern;
    END IF;

    IF pattern ~ '\*\*' THEN
        RAISE EXCEPTION 'pattern cannot contain consecutive stars (**). Use # for multi-segment matching. Got: %', pattern;
    END IF;

    IF pattern ~ '##' THEN
        RAISE EXCEPTION 'pattern cannot contain consecutive hashes (##). A single # already matches zero or more segments. Got: %', pattern;
    END IF;

    IF pattern ~ '\*#' OR pattern ~ '#\*' THEN
        RAISE EXCEPTION 'pattern cannot contain adjacent wildcards (*# or #*). Separate wildcards with dots. Got: %', pattern;
    END IF;

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.bind_topic(pattern text, queue_name text)
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    PERFORM pgmq.validate_topic_pattern(pattern);
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'queue_name cannot be NULL or empty';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE meta.queue_name = bind_topic.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist. Create the queue first using pgmq.create()', queue_name;
    END IF;

    INSERT INTO pgmq.topic_bindings (pattern, queue_name)
    VALUES (pattern, queue_name)
    ON CONFLICT ON CONSTRAINT topic_bindings_unique_pattern_queue DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.unbind_topic(pattern text, queue_name text)
    RETURNS boolean
    LANGUAGE plpgsql
    SET search_path = pg_catalog, pg_temp
AS
$$
DECLARE
    rows_deleted integer;
BEGIN
    IF pattern IS NULL OR pattern = '' THEN
        RAISE EXCEPTION 'pattern cannot be NULL or empty';
    END IF;

    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'queue_name cannot be NULL or empty';
    END IF;

    DELETE
    FROM pgmq.topic_bindings
    WHERE topic_bindings.pattern = unbind_topic.pattern
      AND topic_bindings.queue_name = unbind_topic.queue_name;

    GET DIAGNOSTICS rows_deleted = ROW_COUNT;

    IF rows_deleted > 0 THEN
        RETURN true;
    ELSE
        RETURN false;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.test_routing(routing_key text)
    RETURNS TABLE
            (
                pattern        text,
                queue_name     text,
                compiled_regex text
            )
    LANGUAGE plpgsql
    STABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);
    RETURN QUERY
        SELECT b.pattern,
               b.queue_name,
               b.compiled_regex
        FROM pgmq.topic_bindings b
        WHERE routing_key ~ b.compiled_regex
        ORDER BY b.pattern;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb, headers jsonb, delay integer)
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
DECLARE
    b             RECORD;
    matched_count integer := 0;
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);

    IF msg IS NULL THEN
        RAISE EXCEPTION 'msg cannot be NULL';
    END IF;

    IF delay < 0 THEN
        RAISE EXCEPTION 'delay cannot be negative, got: %', delay;
    END IF;

    -- Filter matching patterns in SQL for better performance (uses index)
    -- Any failure will rollback the entire transaction
    FOR b IN
        SELECT DISTINCT tb.queue_name
        FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex
        ORDER BY tb.queue_name -- Deterministic ordering, deduplicated by queue_name
        LOOP
            PERFORM pgmq.send(b.queue_name, msg, headers, delay);
            matched_count := matched_count + 1;
        END LOOP;

    RETURN matched_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb)
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL::pg_catalog.jsonb, 0);
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb, delay integer)
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL::pg_catalog.jsonb, delay);
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.list_topic_bindings()
    RETURNS TABLE
            (
                pattern        text,
                queue_name     text,
                bound_at       TIMESTAMP WITH TIME ZONE,
                compiled_regex text
            )
    LANGUAGE sql
    STABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT pattern, queue_name, bound_at, compiled_regex
    FROM pgmq.topic_bindings
    ORDER BY bound_at DESC, pattern, queue_name;
$$;

CREATE OR REPLACE FUNCTION pgmq.list_topic_bindings(queue_name text)
    RETURNS TABLE
            (
                pattern        text,
                queue_name     text,
                bound_at       TIMESTAMP WITH TIME ZONE,
                compiled_regex text
            )
    LANGUAGE sql
    STABLE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT pattern, tb.queue_name, bound_at, compiled_regex
    FROM pgmq.topic_bindings tb
    WHERE tb.queue_name = list_topic_bindings.queue_name
    ORDER BY bound_at DESC, pattern;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[],
    delay TIMESTAMP WITH TIME ZONE
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE plpgsql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
DECLARE
    b RECORD;
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);

    -- Validate batch parameters once (not per queue)
    PERFORM pgmq._validate_batch_params(msgs, headers);

    -- Filter matching patterns in SQL for better performance (uses index)
    -- Any failure will rollback the entire transaction
    FOR b IN
        SELECT DISTINCT tb.queue_name
        FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex
        ORDER BY tb.queue_name -- Deterministic ordering, deduplicated by queue_name
        LOOP
            -- Use private _send_batch to avoid redundant validation
            RETURN QUERY
            SELECT b.queue_name, batch_result.msg_id
            FROM pgmq._send_batch(b.queue_name, msgs, headers, delay) AS batch_result(msg_id);
        END LOOP;

    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[]
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL::pg_catalog.jsonb[], clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[]
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    delay integer
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL::pg_catalog.jsonb[], clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    delay TIMESTAMP WITH TIME ZONE
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL::pg_catalog.jsonb[], delay);
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[],
    delay integer
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
    SET search_path = pg_catalog, pg_temp
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp() + pg_catalog.make_interval(secs => delay));
$$;

-- Queue tables created by the old functions can retain defaults bound to a
-- function in a writable schema. Replacing the functions does not replace
-- those stored expression dependencies. Rebind only PGMQ's own defaults that
-- refer to a non-catalog now() or to_timestamp(). Include partition children,
-- since their defaults can be stored separately from the parent.
DO $pgmq_rebind_defaults$
DECLARE
    def RECORD;
BEGIN
    FOR def IN
        WITH RECURSIVE extension_tables AS (
            SELECT c.oid, c.relname
              FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_depend ed
                ON ed.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
               AND ed.objid = c.oid
               AND ed.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
               AND ed.deptype = 'e'
              JOIN pg_catalog.pg_extension e
                ON e.oid = ed.refobjid AND e.extname = 'pgmq'
             WHERE c.relnamespace = 'pgmq'::pg_catalog.regnamespace
               AND c.relkind IN ('r', 'p')
        ), roots AS (
            SELECT c.oid AS relid,
                   CASE c.relname
                       WHEN 'meta' THEN 'meta'
                       WHEN 'topic_bindings' THEN 'topic_bindings'
                       ELSE 'notify_insert_throttle'
                   END AS kind
              FROM extension_tables c
             WHERE c.relname IN ('meta', 'topic_bindings', 'notify_insert_throttle')
            UNION
            SELECT c.oid, names.kind
              FROM extension_tables core
              JOIN pgmq.meta m ON core.relname = 'meta'
              CROSS JOIN LATERAL (
                  VALUES (pgmq.format_table_name(m.queue_name::pg_catalog.text, 'q'::pg_catalog.text), 'queue'),
                         (pgmq.format_table_name(m.queue_name::pg_catalog.text, 'a'::pg_catalog.text), 'archive')
              ) AS names(relname, kind)
              JOIN pg_catalog.pg_class c
                ON c.relnamespace = 'pgmq'::pg_catalog.regnamespace
               AND c.relname = names.relname
               AND c.relkind IN ('r', 'p')
        ), managed AS (
            SELECT relid, kind FROM roots
            UNION
            SELECT i.inhrelid, m.kind
              FROM managed m
              JOIN pg_catalog.pg_inherits i ON i.inhparent = m.relid
        )
        SELECT n.nspname, c.relname, a.attname,
               pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS old_expression,
               CASE WHEN m.kind = 'notify_insert_throttle'
                    THEN 'pg_catalog.to_timestamp(0)'
                    ELSE 'pg_catalog.now()'
               END AS replacement,
               EXISTS (
                   SELECT 1
                     FROM pg_catalog.pg_depend d
                     JOIN pg_catalog.pg_proc p ON p.oid = d.refobjid
                     JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
                    WHERE d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                      AND d.objid = ad.oid
                      AND d.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                      AND pn.nspname <> 'pg_catalog'
                      AND ((m.kind = 'notify_insert_throttle'
                            AND p.proname = 'to_timestamp'
                            AND p.proargtypes[0] = 'pg_catalog.int4'::pg_catalog.regtype
                            AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) =
                                pg_catalog.format('%I.to_timestamp(0)', pn.nspname))
                        OR (m.kind <> 'notify_insert_throttle'
                            AND p.proname = 'now'
                            AND p.pronargs = 0
                            AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) =
                                pg_catalog.format('%I.now()', pn.nspname)))
               ) AS known_shape
          FROM managed m
          JOIN pg_catalog.pg_class c ON c.oid = m.relid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
          JOIN pg_catalog.pg_attrdef ad
            ON ad.adrelid = c.oid AND ad.adnum = a.attnum
         WHERE ((m.kind = 'meta' AND a.attname = 'created_at')
             OR (m.kind = 'topic_bindings' AND a.attname = 'bound_at')
             OR (m.kind = 'notify_insert_throttle' AND a.attname = 'last_notified_at')
             OR (m.kind = 'queue' AND a.attname = 'enqueued_at')
             OR (m.kind = 'archive' AND a.attname IN ('enqueued_at', 'archived_at')))
           AND EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_depend d
                 JOIN pg_catalog.pg_proc p ON p.oid = d.refobjid
                 JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
                WHERE d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                  AND d.objid = ad.oid
                  AND d.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                  AND pn.nspname <> 'pg_catalog'
           )
    LOOP
        IF NOT def.known_shape THEN
            RAISE EXCEPTION 'PGMQ upgrade cannot safely rebind custom default on %.%.%: %; inspect the non-catalog function dependency before retrying',
                def.nspname, def.relname, def.attname, def.old_expression;
        END IF;
        BEGIN
            EXECUTE pg_catalog.format(
                'ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
                def.nspname, def.relname, def.attname, def.replacement
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE EXCEPTION 'PGMQ upgrade cannot rebind default on %.%.%; use a role that can update the extension and alter the table, or a superuser',
                def.nspname, def.relname, def.attname;
        END;
    END LOOP;
END
$pgmq_rebind_defaults$;

-- pg_monitor could read every table and sequence in the pgmq schema, including
-- queue and archive tables. Replace that broad access with the minimum needed
-- by pgmq.metrics(), pgmq.metrics_all(), and pgmq.list_queues().

-- 1. Remove default privileges that granted SELECT on every new table and
-- sequence. The grantor can differ from the role running this upgrade.
DO $$
DECLARE
    acl RECORD;
BEGIN
    FOR acl IN
        SELECT DISTINCT d.defaclrole::pg_catalog.regrole AS role_name
          FROM pg_catalog.pg_default_acl d
          JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) a
         WHERE n.nspname = 'pgmq'
           AND d.defaclobjtype IN ('r', 'S')
           AND a.grantee = 'pg_monitor'::pg_catalog.regrole
    LOOP
        BEGIN
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor',
                acl.role_name
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON SEQUENCES FROM pg_monitor',
                acl.role_name
            );
        EXCEPTION WHEN insufficient_privilege OR undefined_object THEN
            RAISE WARNING 'pgmq: could not remove default privileges of role % granting SELECT to pg_monitor in schema pgmq; run "ALTER DEFAULT PRIVILEGES FOR ROLE % IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor" (and the same ON SEQUENCES) as that role or a superuser', acl.role_name, acl.role_name;
        END;
    END LOOP;
END;
$$;

-- 2. Revoke whole-relation SELECT from pg_monitor on every relation in pgmq
-- except meta. Step 4 reports grants that the upgrade role cannot remove.
DO $$
DECLARE
    rel RECORD;
BEGIN
    FOR rel IN
        SELECT DISTINCT c.oid::pg_catalog.regclass AS rel_name,
               c.relkind = 'S' AS is_sequence
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
         WHERE n.nspname = 'pgmq'
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
           AND c.relname <> 'meta'
           AND a.grantee = 'pg_monitor'::pg_catalog.regrole
           AND a.privilege_type = 'SELECT'
    LOOP
        BEGIN
            EXECUTE pg_catalog.format(
                'REVOKE SELECT ON %s %s FROM pg_monitor',
                CASE WHEN rel.is_sequence THEN 'SEQUENCE' ELSE 'TABLE' END,
                rel.rel_name
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'pgmq: could not revoke SELECT on % from pg_monitor; run "REVOKE SELECT ON % FROM pg_monitor" as its owner or a superuser', rel.rel_name, rel.rel_name;
        END;
    END LOOP;
END;
$$;

-- 3. Grant the minimal metrics access on each existing queue. A grant on a
-- partitioned parent is sufficient because metrics query the parent.
DO $$
DECLARE
    q RECORD;
    qtable TEXT;
    qseq TEXT;
BEGIN
    FOR q IN SELECT queue_name FROM pgmq.meta LOOP
        qtable := pgmq.format_table_name(
            q.queue_name::pg_catalog.text,
            'q'::pg_catalog.text
        );
        qseq := qtable || '_msg_id_seq';
        IF pg_catalog.to_regclass(pg_catalog.format('pgmq.%I', qtable)) IS NOT NULL THEN
            BEGIN
                EXECUTE pg_catalog.format(
                    'GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor',
                    qtable
                );
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT (vt, enqueued_at) on pgmq.% to pg_monitor; run it as the table owner or a superuser so queue metrics work for pg_monitor', pg_catalog.quote_ident(qtable);
            END;
        END IF;
        IF pg_catalog.to_regclass(pg_catalog.format('pgmq.%I', qseq)) IS NOT NULL THEN
            BEGIN
                EXECUTE pg_catalog.format(
                    'GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor',
                    qseq
                );
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT on sequence pgmq.% to pg_monitor; run it as the sequence owner or a superuser so queue metrics work for pg_monitor', pg_catalog.quote_ident(qseq);
            END;
        END IF;
    END LOOP;
END;
$$;

-- 4. Report any heap-like relation that still exposes whole-table SELECT to
-- pg_monitor. Column grants live in pg_attribute and are not reported here.
DO $$
DECLARE
    leftover TEXT;
BEGIN
    SELECT pg_catalog.string_agg(
               c.oid::pg_catalog.regclass::pg_catalog.text,
               ', ' ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text
           )
      INTO leftover
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
     WHERE n.nspname = 'pgmq'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       AND c.relname <> 'meta'
       AND a.grantee = 'pg_monitor'::pg_catalog.regrole
       AND a.privilege_type = 'SELECT';

    IF leftover IS NOT NULL THEN
        RAISE WARNING 'pgmq: pg_monitor still has whole-table SELECT on: %. These may expose message payloads; revoke them as the object owner or a superuser.', leftover;
    END IF;
END;
$$;
