-- TOPIC-BASED ROUTING TESTS
-- This test file validates the topic-based routing implementation
-- Aligned with topics_test.py

-- Stabilize output and ensure clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- =============================================================================
-- Tests for validate_routing_key()
-- =============================================================================

-- test_valid_simple_routing_key
SELECT pgmq.validate_routing_key('logs.error') = true;

-- test_valid_routing_key_with_hyphens
SELECT pgmq.validate_routing_key('app.user-service.auth') = true;

-- test_valid_routing_key_with_underscores
SELECT pgmq.validate_routing_key('system_events.db.connection_failed') = true;

-- test_valid_single_segment_routing_key
SELECT pgmq.validate_routing_key('logs') = true;

-- test_invalid_empty_routing_key (expect error - using DO block to catch)
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('');
    RAISE EXCEPTION 'Should have raised an error for empty routing key';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_null_routing_key
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key(NULL);
    RAISE EXCEPTION 'Should have raised an error for NULL routing key';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_starts_with_dot
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('.logs.error');
    RAISE EXCEPTION 'Should have raised an error for routing key starting with dot';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_ends_with_dot
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs.error.');
    RAISE EXCEPTION 'Should have raised an error for routing key ending with dot';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_consecutive_dots
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs..error');
    RAISE EXCEPTION 'Should have raised an error for consecutive dots';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_with_wildcards
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs.*');
    RAISE EXCEPTION 'Should have raised an error for wildcards in routing key';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_with_special_chars
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs.error!');
    RAISE EXCEPTION 'Should have raised an error for special characters';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_with_space
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs error');
    RAISE EXCEPTION 'Should have raised an error for space in routing key';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_routing_key_too_long
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key(repeat('a', 256));
    RAISE EXCEPTION 'Should have raised an error for routing key > 255 chars';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- =============================================================================
-- Tests for validate_topic_pattern()
-- =============================================================================

-- test_valid_pattern_with_star
SELECT pgmq.validate_topic_pattern('logs.*') = true;

-- test_valid_pattern_with_hash
SELECT pgmq.validate_topic_pattern('logs.#') = true;

-- test_valid_pattern_star_at_start
SELECT pgmq.validate_topic_pattern('*.error') = true;

-- test_valid_pattern_hash_at_start
SELECT pgmq.validate_topic_pattern('#.error') = true;

-- test_valid_pattern_mixed_wildcards
SELECT pgmq.validate_topic_pattern('app.*.#') = true;

-- test_valid_pattern_exact_match
SELECT pgmq.validate_topic_pattern('logs.error.fatal') = true;

-- test_valid_pattern_only_hash
SELECT pgmq.validate_topic_pattern('#') = true;

-- test_valid_pattern_only_star
SELECT pgmq.validate_topic_pattern('*') = true;

-- test_invalid_empty_pattern
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('');
    RAISE EXCEPTION 'Should have raised an error for empty pattern';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_null_pattern
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern(NULL);
    RAISE EXCEPTION 'Should have raised an error for NULL pattern';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_starts_with_dot
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('.logs.*');
    RAISE EXCEPTION 'Should have raised an error for pattern starting with dot';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_ends_with_dot
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.*.');
    RAISE EXCEPTION 'Should have raised an error for pattern ending with dot';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_consecutive_dots
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs..error');
    RAISE EXCEPTION 'Should have raised an error for consecutive dots in pattern';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_consecutive_stars
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.**');
    RAISE EXCEPTION 'Should have raised an error for consecutive stars';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_consecutive_hashes
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.##');
    RAISE EXCEPTION 'Should have raised an error for consecutive hashes';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_adjacent_wildcards_star_hash
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.*#');
    RAISE EXCEPTION 'Should have raised an error for adjacent *#';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_adjacent_wildcards_hash_star
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.#*');
    RAISE EXCEPTION 'Should have raised an error for adjacent #*';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_special_chars
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.error!');
    RAISE EXCEPTION 'Should have raised an error for special characters in pattern';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_invalid_pattern_too_long
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern(repeat('a', 256));
    RAISE EXCEPTION 'Should have raised an error for pattern > 255 chars';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- =============================================================================
-- Tests for regex injection protection in validation
-- =============================================================================

-- test_pattern_rejects_parentheses (e.g., logs.(foo|bar))
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.(foo|bar)');
    RAISE EXCEPTION 'Should have raised an error for parentheses in pattern';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%invalid characters%' THEN
        RAISE EXCEPTION 'Expected invalid characters error, got: %', SQLERRM;
    END IF;
END $$;

-- test_pattern_rejects_square_brackets
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.[error]');
    RAISE EXCEPTION 'Should have raised an error for square brackets in pattern';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%invalid characters%' THEN
        RAISE EXCEPTION 'Expected invalid characters error, got: %', SQLERRM;
    END IF;
END $$;

-- test_pattern_rejects_dollar
DO $$
BEGIN
    PERFORM pgmq.validate_topic_pattern('logs.error$');
    RAISE EXCEPTION 'Should have raised an error for dollar sign in pattern';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%invalid characters%' THEN
        RAISE EXCEPTION 'Expected invalid characters error, got: %', SQLERRM;
    END IF;
END $$;

-- test_routing_key_rejects_regex_metacharacters
DO $$
BEGIN
    PERFORM pgmq.validate_routing_key('logs.(foo|bar)');
    RAISE EXCEPTION 'Should have raised an error for regex metacharacters in routing key';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%invalid characters%' THEN
        RAISE EXCEPTION 'Expected invalid characters error, got: %', SQLERRM;
    END IF;
END $$;

-- =============================================================================
-- Tests for bind_topic() and unbind_topic()
-- =============================================================================

-- Setup test environment
SELECT pgmq.create('topic_queue_1');
SELECT pgmq.create('topic_queue_2');
SELECT pgmq.create('topic_queue_3');

-- test_bind_topic_creates_binding
-- Bind a topic pattern to a queue
SELECT pgmq.bind_topic('orders.#', 'topic_queue_1');

-- Verify binding was created
SELECT COUNT(*) = 1 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_1' AND pattern = 'orders.#';

-- test_bind_topic_idempotent
-- Binding the same pattern multiple times should be idempotent
SELECT pgmq.bind_topic('logs.info', 'topic_queue_1');
SELECT pgmq.bind_topic('logs.info', 'topic_queue_1');
SELECT pgmq.bind_topic('logs.info', 'topic_queue_1');

-- Should have exactly one binding
SELECT COUNT(*) = 1 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_1' AND pattern = 'logs.info';

-- test_bind_topic_multiple_patterns
-- Bind multiple patterns to the same queue
SELECT pgmq.bind_topic('events.*', 'topic_queue_2');
SELECT pgmq.bind_topic('alerts.#', 'topic_queue_2');
SELECT pgmq.bind_topic('system.startup', 'topic_queue_2');

-- Should have exactly 3 bindings for topic_queue_2
SELECT COUNT(*) = 3 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_2';

-- test_bind_topic_same_pattern_different_queues
-- Bind the same pattern to different queues
SELECT pgmq.bind_topic('broadcasts.#', 'topic_queue_1');
SELECT pgmq.bind_topic('broadcasts.#', 'topic_queue_2');

-- Both queues should have the binding
SELECT COUNT(*) = 2 FROM pgmq.topic_bindings WHERE pattern = 'broadcasts.#';

-- test_unbind_topic_removes_binding
-- Unbind a topic pattern
SELECT pgmq.unbind_topic('logs.info', 'topic_queue_1');

-- Verify binding was removed
SELECT COUNT(*) = 0 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_1' AND pattern = 'logs.info';

-- test_unbind_topic_nonexistent_returns_false
-- Unbinding a non-existent pattern should return false
SELECT pgmq.unbind_topic('nonexistent.pattern', 'topic_queue_1') = false;

-- Clean up bindings for next tests
DELETE FROM pgmq.topic_bindings;

-- =============================================================================
-- Tests for test_routing() - dry-run routing tests
-- =============================================================================

-- test_routing_exact_match_dry_run
SELECT pgmq.bind_topic('logs.error', 'topic_queue_1');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.error');
DELETE FROM pgmq.topic_bindings;

-- test_routing_star_wildcard_dry_run
SELECT pgmq.bind_topic('logs.*.error', 'topic_queue_1');
-- Should match
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.api.error');
-- Should NOT match
SELECT COUNT(*) = 0 FROM pgmq.test_routing('logs.api.db.error');
DELETE FROM pgmq.topic_bindings;

-- test_routing_hash_wildcard_dry_run
SELECT pgmq.bind_topic('logs.#', 'topic_queue_1');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.error');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.api.error');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.api.db.critical');
DELETE FROM pgmq.topic_bindings;

-- test_routing_star_vs_hash_difference_dry_run
SELECT pgmq.bind_topic('logs.*', 'topic_queue_1');
SELECT pgmq.bind_topic('logs.#', 'topic_queue_2');
-- One segment: both match
SELECT COUNT(*) = 2 FROM pgmq.test_routing('logs.error');
-- Two segments: only hash matches
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.error.fatal');
DELETE FROM pgmq.topic_bindings;

-- test_routing_multiple_queues_dry_run
SELECT pgmq.bind_topic('logs.#', 'topic_queue_1');
SELECT pgmq.bind_topic('logs.*.error', 'topic_queue_2');
SELECT pgmq.bind_topic('logs.*.critical', 'topic_queue_3');
-- Info message: only topic_queue_1
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.api.info');
-- Error message: topic_queue_1 and topic_queue_2
SELECT COUNT(*) = 2 FROM pgmq.test_routing('logs.api.error');
-- Critical message: topic_queue_1 and topic_queue_3
SELECT COUNT(*) = 2 FROM pgmq.test_routing('logs.db.critical');
DELETE FROM pgmq.topic_bindings;

-- test_routing_no_matches_dry_run
SELECT pgmq.bind_topic('logs.*', 'topic_queue_1');
SELECT COUNT(*) = 0 FROM pgmq.test_routing('metrics.cpu.usage');
DELETE FROM pgmq.topic_bindings;

-- test_routing_hash_at_start_dry_run
SELECT pgmq.bind_topic('#.error', 'topic_queue_1');
SELECT COUNT(*) >= 1 FROM pgmq.test_routing('logs.error');
SELECT COUNT(*) >= 1 FROM pgmq.test_routing('logs.api.error');
DELETE FROM pgmq.topic_bindings;

-- test_routing_catch_all_dry_run
SELECT pgmq.bind_topic('#', 'topic_queue_1');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('logs.error');
SELECT COUNT(*) = 1 FROM pgmq.test_routing('anything.at.all');
DELETE FROM pgmq.topic_bindings;

-- =============================================================================
-- Tests for compiled_regex correctness (regex injection protection)
-- =============================================================================

-- test_compiled_regex_escapes_dots
-- Verify that dots in patterns are compiled to literal dot matchers, not regex wildcards
SELECT pgmq.bind_topic('logs.error', 'topic_queue_1');
SELECT compiled_regex = '^logs\.error$' AS dot_properly_escaped
FROM pgmq.topic_bindings WHERE pattern = 'logs.error' AND queue_name = 'topic_queue_1';
DELETE FROM pgmq.topic_bindings;

-- test_compiled_regex_for_star_wildcard
SELECT pgmq.bind_topic('logs.*', 'topic_queue_1');
SELECT compiled_regex = '^logs\.[^.]+$' AS star_compiled_correctly
FROM pgmq.topic_bindings WHERE pattern = 'logs.*' AND queue_name = 'topic_queue_1';
DELETE FROM pgmq.topic_bindings;

-- test_compiled_regex_for_hash_wildcard
SELECT pgmq.bind_topic('logs.#', 'topic_queue_1');
SELECT compiled_regex = '^logs\..*$' AS hash_compiled_correctly
FROM pgmq.topic_bindings WHERE pattern = 'logs.#' AND queue_name = 'topic_queue_1';
DELETE FROM pgmq.topic_bindings;

-- test_compiled_regex_for_mixed_pattern
SELECT pgmq.bind_topic('app.*.logs.#', 'topic_queue_1');
SELECT compiled_regex = '^app\.[^.]+\.logs\..*$' AS mixed_compiled_correctly
FROM pgmq.topic_bindings WHERE pattern = 'app.*.logs.#' AND queue_name = 'topic_queue_1';
DELETE FROM pgmq.topic_bindings;

-- test_dot_not_matching_arbitrary_characters
-- Pattern 'a.b' should NOT match routing key 'aXb' (dot must be literal)
SELECT pgmq.bind_topic('a.b', 'topic_queue_1');
SELECT COUNT(*) = 0 AS dot_is_literal FROM pgmq.test_routing('aXb');
SELECT COUNT(*) = 1 AS literal_dot_matches FROM pgmq.test_routing('a.b');
DELETE FROM pgmq.topic_bindings;

-- test_hyphen_in_pattern_safe
-- Hyphens should be treated literally, not as regex range operators
SELECT pgmq.bind_topic('my-app.logs.*', 'topic_queue_1');
SELECT COUNT(*) = 1 AS hyphen_pattern_works FROM pgmq.test_routing('my-app.logs.error');
SELECT COUNT(*) = 0 AS hyphen_is_literal FROM pgmq.test_routing('myXapp.logs.error');
DELETE FROM pgmq.topic_bindings;

-- =============================================================================
-- Tests for actual message routing with send_topic()
-- =============================================================================

-- test_routing_exact_match
-- Test exact routing key matching
SELECT pgmq.bind_topic('orders.created', 'topic_queue_1');
SELECT pgmq.send_topic('orders.created', '{"order_id": 123}'::jsonb, NULL, 0);

-- Should have 1 message in topic_queue_1
SELECT COUNT(*) = 1 FROM pgmq.q_topic_queue_1;
SELECT COUNT(*) = 0 FROM pgmq.q_topic_queue_2;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_routing_star_wildcard
-- Test star (*) wildcard - matches exactly one segment
SELECT pgmq.bind_topic('logs.*.error', 'topic_queue_1');

-- Should match
SELECT pgmq.send_topic('logs.app.error', '{"message": "error1"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('logs.db.error', '{"message": "error2"}'::jsonb, NULL, 0);

-- Should NOT match (wrong number of segments)
SELECT pgmq.send_topic('logs.error', '{"message": "error3"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('logs.app.system.error', '{"message": "error4"}'::jsonb, NULL, 0);

-- Should have exactly 2 messages in topic_queue_1
SELECT COUNT(*) = 2 FROM pgmq.q_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_routing_hash_wildcard
-- Test hash (#) wildcard - matches zero or more segments after the dot
SELECT pgmq.bind_topic('events.#', 'topic_queue_1');

-- Should match (has dot after events)
SELECT pgmq.send_topic('events.user', '{"event": "2"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('events.user.login', '{"event": "3"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('events.user.login.success', '{"event": "4"}'::jsonb, NULL, 0);

-- Should NOT match (no dot after events, or different prefix)
SELECT pgmq.send_topic('events', '{"event": "1"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('notifications.user', '{"event": "5"}'::jsonb, NULL, 0);

-- Should have exactly 3 messages in topic_queue_1
SELECT COUNT(*) = 3 FROM pgmq.q_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_routing_star_vs_hash_difference
-- Test the difference between * and #
SELECT pgmq.bind_topic('data.*.processed', 'topic_queue_1');
SELECT pgmq.bind_topic('data.#.processed', 'topic_queue_2');

SELECT pgmq.send_topic('data.user.processed', '{"msg": "1"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('data.order.item.processed', '{"msg": "2"}'::jsonb, NULL, 0);

-- topic_queue_1 should have 1 message (* matches exactly one segment)
SELECT COUNT(*) = 1 FROM pgmq.q_topic_queue_1;

-- topic_queue_2 should have 2 messages (# matches zero or more segments)
SELECT COUNT(*) = 2 FROM pgmq.q_topic_queue_2;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
SELECT pgmq.purge_queue('topic_queue_2');
DELETE FROM pgmq.topic_bindings;

-- test_routing_multiple_queues
-- Test routing to multiple queues
SELECT pgmq.bind_topic('alerts.#', 'topic_queue_1');
SELECT pgmq.bind_topic('alerts.critical.#', 'topic_queue_2');
SELECT pgmq.bind_topic('#', 'topic_queue_3');

SELECT pgmq.send_topic('alerts.critical.database', '{"msg": "critical alert"}'::jsonb, NULL, 0);

-- All three queues should receive the message
SELECT COUNT(*) = 1 FROM pgmq.q_topic_queue_1;
SELECT COUNT(*) = 1 FROM pgmq.q_topic_queue_2;
SELECT COUNT(*) = 1 FROM pgmq.q_topic_queue_3;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
SELECT pgmq.purge_queue('topic_queue_2');
SELECT pgmq.purge_queue('topic_queue_3');
DELETE FROM pgmq.topic_bindings;

-- test_routing_no_matches
-- Test routing with no matching patterns
SELECT pgmq.bind_topic('specific.pattern', 'topic_queue_1');

SELECT pgmq.send_topic('different.pattern', '{"msg": "no match"}'::jsonb, NULL, 0);

-- No queues should have messages
SELECT COUNT(*) = 0 FROM pgmq.q_topic_queue_1;
SELECT COUNT(*) = 0 FROM pgmq.q_topic_queue_2;

-- Clean up
DELETE FROM pgmq.topic_bindings;

-- test_routing_hash_at_start
-- Test # wildcard at the start of pattern (matches zero or more segments before .error)
SELECT pgmq.bind_topic('#.error', 'topic_queue_1');

-- Should match (have .error suffix)
SELECT pgmq.send_topic('app.error', '{"msg": "2"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('system.database.error', '{"msg": "3"}'::jsonb, NULL, 0);

-- Should NOT match (no .error suffix or missing dot before error)
SELECT pgmq.send_topic('error', '{"msg": "1"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('info', '{"msg": "4"}'::jsonb, NULL, 0);

-- Should have 2 messages (all ending with .error and having at least one segment before)
SELECT COUNT(*) = 2 FROM pgmq.q_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_routing_catch_all
-- Test catch-all pattern
SELECT pgmq.bind_topic('#', 'topic_queue_1');

SELECT pgmq.send_topic('any', '{"msg": "1"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('any.pattern', '{"msg": "2"}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('any.pattern.works', '{"msg": "3"}'::jsonb, NULL, 0);

-- Should have 3 messages (catch-all matches everything)
SELECT COUNT(*) = 3 FROM pgmq.q_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_topic_with_headers
-- Test sending topic messages with custom headers
SELECT pgmq.bind_topic('orders.created', 'topic_queue_1');

SELECT pgmq.send_topic(
    'orders.created',
    '{"order_id": 456}'::jsonb,
    '{"priority": "high", "source": "web"}'::jsonb,
    0
);

-- Verify message was delivered with headers
WITH msg AS (
    SELECT * FROM pgmq.read('topic_queue_1', 10, 1)
)
SELECT
    COUNT(*) = 1 as has_message,
    (SELECT message->>'order_id' FROM msg) = '456' as correct_body,
    (SELECT headers->>'priority' FROM msg) = 'high' as has_priority,
    (SELECT headers->>'source' FROM msg) = 'web' as has_source;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_topic_with_delay
-- Test sending topic messages with delay
SELECT pgmq.bind_topic('delayed.message', 'topic_queue_1');

SELECT pgmq.send_topic(
    'delayed.message',
    '{"msg": "delayed"}'::jsonb,
    2
);

-- Should have 0 immediately readable messages (delayed)
SELECT COUNT(*) = 0 FROM pgmq.read('topic_queue_1', 10, 1);

-- Wait for delay to expire
SELECT pg_sleep(3);

-- Should now have 1 message
SELECT COUNT(*) = 1 FROM pgmq.read('topic_queue_1', 10, 1);

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_topic_overloaded_functions
-- Test different function signatures
SELECT pgmq.bind_topic('test.routing', 'topic_queue_1');

-- Basic: routing_key, message
SELECT pgmq.send_topic('test.routing', '{"type": "basic"}'::jsonb, NULL, 0);

-- With headers: routing_key, message, headers, delay
SELECT pgmq.send_topic('test.routing', '{"type": "headers"}'::jsonb, '{"priority": "low"}'::jsonb, 0);

-- With delay: routing_key, message, delay
SELECT pgmq.send_topic('test.routing', '{"type": "delayed"}'::jsonb, 0);

-- With headers and delay: routing_key, message, headers, delay
SELECT pgmq.send_topic('test.routing', '{"type": "full"}'::jsonb, '{"priority": "high"}'::jsonb, 0);

-- Should have 4 messages total
SELECT COUNT(*) = 4 FROM pgmq.q_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_topic_invalid_routing_key
DO $$
BEGIN
    PERFORM pgmq.send_topic('invalid..key', '{"test": true}'::jsonb, NULL, 0);
    RAISE EXCEPTION 'Should have raised an error for invalid routing key';
EXCEPTION WHEN OTHERS THEN
    -- Expected error for consecutive dots
END $$;

-- test_send_topic_null_message
DO $$
BEGIN
    PERFORM pgmq.send_topic('valid.key', NULL, NULL, 0);
    RAISE EXCEPTION 'Should have raised an error for NULL message';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_send_topic_negative_delay
DO $$
BEGIN
    PERFORM pgmq.send_topic('valid.key', '{"test": true}'::jsonb, NULL, -1);
    RAISE EXCEPTION 'Should have raised an error for negative delay';
EXCEPTION WHEN OTHERS THEN
    -- Expected error
END $$;

-- test_send_topic_selective_routing
-- Test selective routing based on patterns
SELECT pgmq.bind_topic('payments.*.completed', 'topic_queue_1');
SELECT pgmq.bind_topic('payments.card.#', 'topic_queue_2');
SELECT pgmq.bind_topic('payments.#', 'topic_queue_3');

SELECT pgmq.send_topic('payments.card.completed', '{"payment_id": 1}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('payments.paypal.completed', '{"payment_id": 2}'::jsonb, NULL, 0);
SELECT pgmq.send_topic('payments.card.declined', '{"payment_id": 3}'::jsonb, NULL, 0);

-- topic_queue_1: payments.*.completed - should have 2 messages
SELECT COUNT(*) = 2 FROM pgmq.q_topic_queue_1;

-- topic_queue_2: payments.card.# - should have 2 messages
SELECT COUNT(*) = 2 FROM pgmq.q_topic_queue_2;

-- topic_queue_3: payments.# - should have 3 messages (catch-all)
SELECT COUNT(*) = 3 FROM pgmq.q_topic_queue_3;

-- Clean up
SELECT pgmq.purge_queue('topic_queue_1');
SELECT pgmq.purge_queue('topic_queue_2');
SELECT pgmq.purge_queue('topic_queue_3');
DELETE FROM pgmq.topic_bindings;

-- test_drop_queue_cascades_bindings
-- Test that dropping a queue removes its bindings
SELECT pgmq.bind_topic('test.pattern.1', 'topic_queue_1');
SELECT pgmq.bind_topic('test.pattern.2', 'topic_queue_1');

-- Verify bindings exist
SELECT COUNT(*) = 2 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_1';

-- Drop the queue
SELECT pgmq.drop_queue('topic_queue_1');

-- Bindings should be removed due to CASCADE
SELECT COUNT(*) = 0 FROM pgmq.topic_bindings WHERE queue_name = 'topic_queue_1';

-- test_validate_routing_key_valid_cases
-- Test valid routing keys
SELECT pgmq.create('validation_queue');
SELECT pgmq.bind_topic('test.#', 'validation_queue');

-- Valid routing keys
SELECT pgmq.send_topic('simple.key', '{"test": 1}'::jsonb, NULL, 0) IS NOT NULL;
SELECT pgmq.send_topic('key-with-hyphens', '{"test": 2}'::jsonb, NULL, 0) IS NOT NULL;
SELECT pgmq.send_topic('key_with_underscores', '{"test": 3}'::jsonb, NULL, 0) IS NOT NULL;
SELECT pgmq.send_topic('single', '{"test": 4}'::jsonb, NULL, 0) IS NOT NULL;

-- Clean up validation tests
SELECT pgmq.drop_queue('validation_queue');
DELETE FROM pgmq.topic_bindings;

-- =============================================================================
-- Tests for send_batch_topic()
-- =============================================================================

-- test_send_batch_topic_basic
-- Test basic batch sending to topics
SELECT pgmq.create('batch_topic_queue_1');
SELECT pgmq.create('batch_topic_queue_2');
SELECT pgmq.bind_topic('batch.test.*', 'batch_topic_queue_1');
SELECT pgmq.bind_topic('batch.#', 'batch_topic_queue_2');

-- Send a batch of 3 messages
SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
    'batch.test.messages',
    ARRAY['{"id": 1}'::jsonb, '{"id": 2}'::jsonb, '{"id": 3}'::jsonb]
) ORDER BY queue_name, msg_id;

-- Should have 3 messages in each queue
SELECT COUNT(*) = 3 FROM pgmq.q_batch_topic_queue_1;
SELECT COUNT(*) = 3 FROM pgmq.q_batch_topic_queue_2;

-- Clean up
SELECT pgmq.purge_queue('batch_topic_queue_1');
SELECT pgmq.purge_queue('batch_topic_queue_2');
DELETE FROM pgmq.topic_bindings;

-- test_send_batch_topic_with_headers
-- Test batch sending with headers
SELECT pgmq.bind_topic('headers.test', 'batch_topic_queue_1');

SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
    'headers.test',
    ARRAY['{"msg": 1}'::jsonb, '{"msg": 2}'::jsonb]::jsonb[],
    ARRAY['{"header": "A"}'::jsonb, '{"header": "B"}'::jsonb]::jsonb[]
) ORDER BY queue_name, msg_id;

-- Verify messages and headers
SELECT message, headers FROM pgmq.q_batch_topic_queue_1 ORDER BY msg_id;

-- Clean up
SELECT pgmq.purge_queue('batch_topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_batch_topic_with_delay
-- Test batch sending with delay
SELECT pgmq.bind_topic('delay.test', 'batch_topic_queue_1');

SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
    'delay.test',
    ARRAY['{"delayed": true}'::jsonb]::jsonb[],
    5
) ORDER BY queue_name, msg_id;

-- Message should be invisible due to delay
SELECT COUNT(*) = 0 FROM pgmq.read('batch_topic_queue_1', 1, 1);
SELECT COUNT(*) = 1 FROM pgmq.q_batch_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('batch_topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_batch_topic_with_timestamp_delay
-- Test batch sending with timestamp delay
SELECT pgmq.bind_topic('timestamp_delay.test', 'batch_topic_queue_1');

SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
    'timestamp_delay.test',
    ARRAY['{"delayed_timestamp": true}'::jsonb]::jsonb[],
    clock_timestamp() + interval '3 seconds'
) ORDER BY queue_name, msg_id;

-- Message should be invisible due to delay
SELECT COUNT(*) = 0 FROM pgmq.read('batch_topic_queue_1', 1, 1);
SELECT COUNT(*) = 1 FROM pgmq.q_batch_topic_queue_1;

-- Clean up
SELECT pgmq.purge_queue('batch_topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_batch_topic_no_matches
-- Test sending to routing key with no bindings
SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
    'no.matches.here',
    ARRAY['{"test": 1}'::jsonb]
);

-- Should have 0 messages in queues
SELECT COUNT(*) = 0 FROM pgmq.q_batch_topic_queue_1;
SELECT COUNT(*) = 0 FROM pgmq.q_batch_topic_queue_2;

-- test_send_batch_topic_single_queue
-- Test sending to a single queue (edge case between 0 and multiple)
SELECT pgmq.bind_topic('single.queue.test', 'batch_topic_queue_1');

SELECT queue_name, COUNT(*) as msg_count
FROM pgmq.send_batch_topic(
    'single.queue.test',
    ARRAY['{"a": 1}'::jsonb, '{"b": 2}'::jsonb]
)
GROUP BY queue_name;

-- Clean up
SELECT pgmq.purge_queue('batch_topic_queue_1');
DELETE FROM pgmq.topic_bindings;

-- test_send_batch_topic_null_validation
-- Test NULL validation
SELECT pgmq.bind_topic('validation.test', 'batch_topic_queue_1');

-- Use terse verbosity to make error output consistent across PG versions
\set VERBOSITY terse

-- Should fail: NULL messages array
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('validation.test', NULL);
\set ON_ERROR_STOP 1

-- Should fail: empty messages array
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('validation.test', ARRAY[]::jsonb[]);
\set ON_ERROR_STOP 1

-- Should fail: invalid routing key
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('invalid..key', ARRAY['{"test": 1}'::jsonb]);
\set ON_ERROR_STOP 1

-- Should fail: headers array length greater than msgs array length
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('validation.test', ARRAY['{"test": 1}'::jsonb], ARRAY['{"h": 1}'::jsonb, '{"h": 2}'::jsonb]);
\set ON_ERROR_STOP 1

-- Should fail: headers array length less than msgs array length
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('validation.test', ARRAY['{"test": 1}'::jsonb, '{"test": 2}'::jsonb], ARRAY['{"h": 1}'::jsonb]);
\set ON_ERROR_STOP 1

-- Should fail: empty headers array with non-empty msgs array
\set ON_ERROR_STOP 0
SELECT pgmq.send_batch_topic('validation.test', ARRAY['{"test": 1}'::jsonb], ARRAY[]::jsonb[]);
\set ON_ERROR_STOP 1

-- Restore default verbosity
\set VERBOSITY default

-- Clean up
DELETE FROM pgmq.topic_bindings;
SELECT pgmq.drop_queue('batch_topic_queue_1');
SELECT pgmq.drop_queue('batch_topic_queue_2');

-- =============================================================================
-- Regression test for duplicate queue prevention (issue #455)
-- =============================================================================

-- test_no_duplicate_sends_when_multiple_patterns_match_same_queue
-- Verify that when multiple patterns bind to the same queue and both match
-- a routing key, the message is only sent once to that queue
SELECT pgmq.create('dedup_test_queue');

-- Bind two different patterns to the same queue, both will match the same routing key
SELECT pgmq.bind_topic('logs.*', 'dedup_test_queue');
SELECT pgmq.bind_topic('logs.#', 'dedup_test_queue');

-- Both patterns match 'logs.error', but message should only be sent once
SELECT pgmq.send_topic('logs.error', '{"msg": "test"}'::jsonb, NULL, 0);

-- Should have exactly 1 message (not 2)
SELECT COUNT(*) = 1 FROM pgmq.q_dedup_test_queue;

-- Clean up
SELECT pgmq.purge_queue('dedup_test_queue');

-- test_no_duplicate_batch_sends_when_multiple_patterns_match_same_queue
-- Same test but for send_batch_topic
SELECT pgmq.bind_topic('batch.test.*', 'dedup_test_queue');
SELECT pgmq.bind_topic('batch.test.#', 'dedup_test_queue');

-- Both patterns match 'batch.test.msg', but batch should only be sent once
SELECT queue_name, COUNT(*) as msg_count
FROM pgmq.send_batch_topic(
    'batch.test.msg',
    ARRAY['{"id": 1}'::jsonb, '{"id": 2}'::jsonb, '{"id": 3}'::jsonb]
)
GROUP BY queue_name;

-- Should have exactly 3 messages (not 6)
SELECT COUNT(*) = 3 FROM pgmq.q_dedup_test_queue;

-- Verify matched_count is correct for send_topic with deduplication
SELECT pgmq.purge_queue('dedup_test_queue');
SELECT pgmq.send_topic('batch.test.msg', '{"msg": "count test"}'::jsonb, NULL, 0) = 1;

-- Clean up
SELECT pgmq.drop_queue('dedup_test_queue');
DELETE FROM pgmq.topic_bindings;

-- Clean up all test queues
SELECT pgmq.drop_queue('topic_queue_2');
SELECT pgmq.drop_queue('topic_queue_3');

-- Verify all queues were dropped
SELECT COUNT(*) = 0 FROM pgmq.list_queues() WHERE queue_name IN ('topic_queue_1', 'topic_queue_2', 'topic_queue_3', 'validation_queue');
