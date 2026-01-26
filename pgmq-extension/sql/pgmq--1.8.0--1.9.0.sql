-- Add advanced JSON filtering support to read functions
-- This adds support for comparison operators in the conditional parameter
-- Format: {"field": "age", "operator": ">", "value": 4}
-- Operators: >, >=, <, <=, =, !=, <>, exists
-- Also supports nested fields like "user.age"

-- Helper function to build WHERE condition from JSONB filter
CREATE OR REPLACE FUNCTION pgmq.build_message_filter(conditional JSONB)
RETURNS TEXT AS $$
DECLARE
    filter_field TEXT;
    filter_operator TEXT;
    filter_value JSONB;
    condition_sql TEXT;
    field_path TEXT;
BEGIN
    -- If empty, return always-true condition
    IF conditional IS NULL OR conditional = '{}'::jsonb THEN
        RETURN '1 = 1';
    END IF;

    -- Check if it's advanced filtering format (has "field", "operator", "value" keys)
    IF conditional ? 'field' AND conditional ? 'operator' THEN
        filter_field := conditional->>'field';
        filter_operator := conditional->>'operator';
        filter_value := conditional->'value';

        -- Validate operator
        IF filter_operator NOT IN ('>', '>=', '<', '<=', '=', '!=', '<>', 'exists') THEN
            RAISE EXCEPTION 'Invalid operator: %. Valid operators are: >, >=, <, <=, =, !=, <>, exists', filter_operator;
        END IF;

        -- Build field path (supports nested fields like "user.age")
        field_path := 'message';
        IF filter_field LIKE '%.%' THEN
            -- Nested field: convert "user.age" to "message->'user'->>'age'"
            DECLARE
                parts TEXT[];
                i INTEGER;
                path_expr TEXT := 'message';
            BEGIN
                parts := string_to_array(filter_field, '.');
                -- Build path: message->'part1'->'part2'...->>'last_part'
                FOR i IN 1..array_length(parts, 1) - 1 LOOP
                    path_expr := path_expr || '->' || quote_literal(parts[i]);
                END LOOP;
                path_expr := path_expr || '->>' || quote_literal(parts[array_length(parts, 1)]);
                field_path := path_expr;
            END;
        ELSE
            -- Simple field: message->>'field'
            field_path := 'message->>' || quote_literal(filter_field);
        END IF;

        -- Build condition based on operator
        IF filter_operator = 'exists' THEN
            -- Check if field exists using ? operator for top-level, @? for nested
            IF filter_field LIKE '%.%' THEN
                -- For nested fields, check if the path exists using @? operator
                -- Convert "user.age" to "$.user.age"
                condition_sql := 'message @? ' || quote_literal('$.' || filter_field) || '::jsonpath';
            ELSE
                condition_sql := 'message ? ' || quote_literal(filter_field);
            END IF;
        ELSIF filter_operator IN ('>', '>=', '<', '<=') THEN
            -- Numeric comparison: cast to numeric
            IF filter_value IS NULL THEN
                RAISE EXCEPTION 'Value is required for operator %', filter_operator;
            END IF;
            condition_sql := '(' || field_path || ')::numeric ' || filter_operator || ' ' || quote_literal(filter_value::text) || '::numeric';
        ELSIF filter_operator IN ('=', '!=', '<>') THEN
            -- Equality comparison
            IF filter_value IS NULL THEN
                -- NULL comparison
                IF filter_operator = '=' THEN
                    condition_sql := field_path || ' IS NULL';
                ELSE
                    condition_sql := field_path || ' IS NOT NULL';
                END IF;
            ELSE
                -- Value comparison (try numeric first, fallback to text)
                IF jsonb_typeof(filter_value) = 'number' THEN
                    condition_sql := '(' || field_path || ')::numeric ' || filter_operator || ' ' || quote_literal(filter_value::text) || '::numeric';
                ELSE
                    condition_sql := field_path || ' ' || filter_operator || ' ' || quote_literal(filter_value::text);
                END IF;
            END IF;
        ELSE
            -- This should not happen due to validation above, but handle it anyway
            RAISE EXCEPTION 'Unsupported operator: %', filter_operator;
        END IF;

        RETURN condition_sql;
    ELSE
        -- Simple containment format (backward compatible): use @> operator
        RETURN '(message @> ' || quote_literal(conditional::text) || '::jsonb)';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Update read function to use advanced filtering
CREATE OR REPLACE FUNCTION pgmq.read(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    conditional JSONB DEFAULT '{}'
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    filter_condition TEXT;
BEGIN
    -- Build filter condition using helper function
    filter_condition := pgmq.build_message_filter(conditional);
    
    sql := FORMAT(
        $QUERY$
        WITH cte AS
        (
            SELECT msg_id
            FROM pgmq.%I
            WHERE vt <= clock_timestamp() AND (%s)
            ORDER BY msg_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET
            vt = clock_timestamp() + %L,
            read_ct = read_ct + 1
        FROM cte
        WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
        $QUERY$,
        qtable, filter_condition, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

-- Update read_with_poll function to use advanced filtering
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
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    filter_condition TEXT;
BEGIN
    -- Build filter condition using helper function
    filter_condition := pgmq.build_message_filter(conditional);
    
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
              WHERE vt <= clock_timestamp() AND (%s)
              ORDER BY msg_id ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          UPDATE pgmq.%I m
          SET
              vt = clock_timestamp() + %L,
              read_ct = read_ct + 1
          FROM cte
          WHERE m.msg_id = cte.msg_id
          RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.vt, m.message, m.headers;
          $QUERY$,
          qtable, filter_condition, qtable, make_interval(secs => vt)
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

