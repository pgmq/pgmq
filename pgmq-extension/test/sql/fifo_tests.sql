-- SQS-STYLE FIFO TESTS ONLY
-- This test file validates the SQS-style FIFO queue implementation

-- Stabilize output and ensure clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- Setup test environment
SELECT pgmq.create('fifo_test_queue');

-- test_fifo_sqs_style_basic_batch_filling
-- Create multiple groups with different message counts
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 1}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 2}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 3}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 1}'::jsonb, '{"x-pgmq-group": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 2}'::jsonb, '{"x-pgmq-group": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "C", "message": 1}'::jsonb, '{"x-pgmq-group": "group_C"}'::jsonb);

-- Verify we have 6 messages in queue
SELECT COUNT(*) = 6 FROM pgmq.q_fifo_test_queue;

-- SQS-style should return multiple messages from the same group (group A first)
-- Request 4 messages - should get all 3 from group A + 1 from group B
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 4)
)
SELECT
    (SELECT COUNT(*) FROM results) = 4 as count_correct,
    (SELECT ARRAY_AGG((message->>'group')::text ORDER BY msg_id) FROM results)
        = ARRAY['A', 'A', 'A', 'B']::text[] as order_correct;

-- Clean up for next SQS test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_mixed_groups
-- SQS-style with mixed groups (with and without FIFO headers)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo1"}'::jsonb, '{"x-pgmq-group": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo2"}'::jsonb, '{"x-pgmq-group": "group1"}'::jsonb);

-- SQS-style should handle mixed groups correctly
-- Should return all 4 messages, with default group messages first, then group1 messages
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 4 as count_correct,
    (SELECT ARRAY_AGG((message->>'message')::text ORDER BY msg_id) FROM results)
        = ARRAY['default1', 'default2', 'fifo1', 'fifo2']::text[] as correct_mixed_order;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_all_messages_read
-- SQS-style reading all messages from a single group
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "high"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "medium"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "notification", "priority": "low"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "low"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);

-- Should return all 4 messages in FIFO order from the orders group
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 4 as count_correct,
    (SELECT ARRAY_AGG((message->>'priority')::text ORDER BY msg_id) FROM results)
        = ARRAY['high', 'medium', 'low', 'low']::text[] as correct_fifo_order;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_visibility_timeout
-- SQS-style with visibility timeout
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout1"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout2"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout3"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);

-- Read with short visibility timeout - should get all 3 messages
SELECT COUNT(*) = 3 FROM pgmq.read_grouped('fifo_test_queue', 1, 10);

-- Should return no messages (all messages still visible)
SELECT COUNT(*) = 0 FROM pgmq.read_grouped('fifo_test_queue', 10, 10);

-- Wait for visibility timeout to expire
SELECT pg_sleep(2);

-- Should now return all messages again
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 3 as count_correct,
    (SELECT COUNT(DISTINCT msg_id) FROM results) = 3 as all_unique;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_polling
-- SQS-style polling functionality
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test1"}'::jsonb, '{"x-pgmq-group": "poll_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test2"}'::jsonb, '{"x-pgmq-group": "poll_group"}'::jsonb);

-- Test SQS-style polling with immediate availability
WITH results AS (
    SELECT * FROM pgmq.read_grouped_with_poll('fifo_test_queue', 10, 10, 1, 100)
)
SELECT
    (SELECT COUNT(*) FROM results) = 2 as count_correct,
    (SELECT COUNT(DISTINCT msg_id) FROM results) = 2 as all_unique;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_batch_sizes
-- SQS-style with different batch sizes
-- Create 5 messages in group A, 3 in group B
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 1}'::jsonb, '{"x-pgmq-group": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 2}'::jsonb, '{"x-pgmq-group": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 3}'::jsonb, '{"x-pgmq-group": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 4}'::jsonb, '{"x-pgmq-group": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 5}'::jsonb, '{"x-pgmq-group": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 1}'::jsonb, '{"x-pgmq-group": "batch_group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 2}'::jsonb, '{"x-pgmq-group": "batch_group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 3}'::jsonb, '{"x-pgmq-group": "batch_group_B"}'::jsonb);

-- Test batch size 3 - should get 3 messages from group A
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 3)
)
SELECT
    (SELECT COUNT(*) FROM results) = 3 as count_correct,
    (SELECT ARRAY_AGG((message->>'group')::text ORDER BY msg_id) FROM results)
        = ARRAY['A', 'A', 'A']::text[] as all_from_group_a;

-- Reset visibility timeout
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- Test batch size 7 - should get 5 from group A + 2 from group B
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 7)
)
SELECT
    (SELECT COUNT(*) FROM results) = 7 as count_correct,
    (SELECT ARRAY_AGG((message->>'group')::text ORDER BY msg_id) FROM results)
        = ARRAY['A', 'A', 'A', 'A', 'A', 'B', 'B']::text[] as correct_batch_order;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_edge_cases
-- SQS-style edge cases
-- Test with empty FIFO key, null key, and no header (should all work as default group)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "empty_fifo_sqs"}'::jsonb, '{"x-pgmq-group": ""}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "null_fifo_sqs"}'::jsonb, '{"x-pgmq-group": null}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "no_header"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "explicit_group"}'::jsonb, '{"x-pgmq-group": "explicit"}'::jsonb);

-- All three (empty, null, no header) should be treated as same default group
-- Should get them in order: empty_fifo_sqs, null_fifo_sqs, no_header (all default), then explicit_group
WITH results AS (
    SELECT * FROM pgmq.read_grouped('fifo_test_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 4 as count_correct,
    (SELECT ARRAY_AGG((message->>'message')::text ORDER BY msg_id) FROM results)
        = ARRAY['empty_fifo_sqs', 'null_fifo_sqs', 'no_header', 'explicit_group']::text[] as all_defaults_together;

-- Clean up
SELECT pgmq.drop_queue('fifo_test_queue');

-- Verify queue was dropped
SELECT COUNT(*) = 0 FROM pgmq.list_queues() WHERE queue_name = 'fifo_test_queue';