CREATE TABLE IF NOT EXISTS pgmq.topic_bindings
(
    pattern        text NOT NULL, -- Wildcard pattern for routing key matching (* = one segment, # = zero or more segments)
    queue_name     text NOT NULL  -- Name of the queue that receives messages when pattern matches
        CONSTRAINT topic_bindings_meta_queue_name_fk
            REFERENCES pgmq.meta (queue_name)
            ON DELETE CASCADE,
    compiled_regex text GENERATED ALWAYS AS (
        -- Pre-compile the pattern to regex for faster matching
        -- This avoids runtime compilation on every send_topic call
        '^' ||
        replace(
                replace(
                        regexp_replace(pattern, '([.+?{}()|\[\]\\^$])', '\\\1', 'g'),
                        '*', '[^.]+'
                ),
                '#', '.*'
        ) || '$'
        ) STORED,                 -- Computed column: stores the compiled regex pattern
    CONSTRAINT topic_bindings_unique_pattern_queue UNIQUE (pattern, queue_name)
);

-- Create covering index for better performance when scanning patterns
-- Includes queue_name and compiled_regex to allow index-only scans (no table access needed)
CREATE INDEX IF NOT EXISTS idx_topic_bindings_covering ON pgmq.topic_bindings (pattern) INCLUDE (queue_name, compiled_regex);

CREATE OR REPLACE FUNCTION pgmq.validate_routing_key(routing_key text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
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
AS
$$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL, 0);
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb, delay integer)
    RETURNS integer
    LANGUAGE plpgsql
    VOLATILE
AS
$$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL, delay);
END;
$$;

-- send_batch_topic: Base implementation with TIMESTAMP WITH TIME ZONE delay
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[],
    delay TIMESTAMP WITH TIME ZONE
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE plpgsql
    VOLATILE
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

-- send_batch_topic: 2 args (routing_key, msgs)
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[]
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL, clock_timestamp());
$$;

-- send_batch_topic: 3 args with headers
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[]
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp());
$$;

-- send_batch_topic: 3 args with integer delay
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    delay integer
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL, clock_timestamp() + make_interval(secs => delay));
$$;

-- send_batch_topic: 3 args with timestamp delay
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    delay TIMESTAMP WITH TIME ZONE
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL, delay);
$$;

-- send_batch_topic: 4 args with integer delay
CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text,
    msgs jsonb[],
    headers jsonb[],
    delay integer
)
    RETURNS TABLE(queue_name text, msg_id bigint)
    LANGUAGE sql
    VOLATILE
AS
$$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$;

-- Fix: Add validation to send_batch to ensure headers array length matches msgs array length
-- Refactored to use private functions for better performance and code reuse

-- _validate_batch_params: Private function to validate batch parameters
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
$$ LANGUAGE plpgsql;

-- _send_batch: Private function that performs the actual batch insert without validation
CREATE OR REPLACE FUNCTION pgmq._send_batch(
    queue_name TEXT,
    msgs JSONB[],
    headers JSONB[],
    delay TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
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
$$ LANGUAGE plpgsql;

-- send_batch: Public function with validation
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
$$ LANGUAGE plpgsql;
