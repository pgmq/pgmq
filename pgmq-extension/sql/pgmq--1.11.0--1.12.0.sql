-- Expose routing key and match pattern in topic message headers (Issue #507)
-- Add x-pgmq-pattern and x-pgmq-routing-key to messages sent via send_topic and send_batch_topic.

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
        SELECT DISTINCT ON (tb.queue_name)
            tb.queue_name,
            tb.pattern
        FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex
        ORDER BY tb.queue_name -- Deterministic ordering, deduplicated by queue_name
        LOOP
            PERFORM pgmq.send(
                b.queue_name,
                msg,
                coalesce(headers, '{}'::jsonb) || jsonb_build_object(
                    'x-pgmq-pattern', b.pattern,
                    'x-pgmq-routing-key', routing_key
                ),
                delay
            );
            matched_count := matched_count + 1;
        END LOOP;

    RETURN matched_count;
END;
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
AS
$$
DECLARE
    b             RECORD;
    merged_headers jsonb[];
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);

    -- Validate batch parameters once (not per queue)
    PERFORM pgmq._validate_batch_params(msgs, headers);

    -- Filter matching patterns in SQL for better performance (uses index)
    -- Any failure will rollback the entire transaction
    FOR b IN
        SELECT DISTINCT ON (tb.queue_name)
            tb.queue_name,
            tb.pattern
        FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex
        ORDER BY tb.queue_name, tb.pattern -- Deterministic ordering, one pattern per queue
        LOOP
            -- Merge topic headers into each message's headers
            SELECT array_agg(
                       (COALESCE(headers[g], '{}'::jsonb) || jsonb_build_object(
                           'x-pgmq-pattern', b.pattern,
                           'x-pgmq-routing-key', routing_key
                       ))
                       ORDER BY g
                   )
            INTO merged_headers
            FROM generate_series(1, array_length(msgs, 1)) AS g;

            -- Use private _send_batch to avoid redundant validation
            RETURN QUERY
            SELECT b.queue_name, batch_result.msg_id
            FROM pgmq._send_batch(b.queue_name, msgs, merged_headers, delay) AS batch_result(msg_id);
        END LOOP;

    RETURN;
END;
$$;
