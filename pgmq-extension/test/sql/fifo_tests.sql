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

-- Set expected message IDs for SQS-style tests
\set sqs_msg_id1 1
\set sqs_msg_id2 2
\set sqs_msg_id3 3
\set sqs_msg_id4 4
\set sqs_msg_id5 5
\set sqs_msg_id6 6

-- Verify we have 6 messages in queue
SELECT COUNT(*) = 6 FROM pgmq.q_fifo_test_queue;

-- SQS-style should return multiple messages from the same group (group A first)
-- Request 4 messages - should get all 3 from group A + 1 from group B
SELECT COUNT(*) = 4 FROM pgmq.read_grouped('fifo_test_queue', 10, 4);

-- Verify the messages are from groups A and B in correct order
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_grouped('fifo_test_queue', 10, 4) ORDER BY msg_id
) = ARRAY['A', 'A', 'A', 'B']::text[];

-- Clean up for next SQS test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_mixed_groups
-- SQS-style with mixed groups (with and without FIFO headers)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo1"}'::jsonb, '{"x-pgmq-group": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo2"}'::jsonb, '{"x-pgmq-group": "group1"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id11 1
\set sqs_msg_id12 2
\set sqs_msg_id13 3
\set sqs_msg_id14 4

-- SQS-style should handle mixed groups correctly
SELECT COUNT(*) = 4 FROM pgmq.read_grouped('fifo_test_queue', 10, 10);

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_conditional_reads
-- SQS-style with conditional reads
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "high"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "medium"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "notification", "priority": "low"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "low"}'::jsonb, '{"x-pgmq-group": "orders"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id15 1
\set sqs_msg_id16 2
\set sqs_msg_id17 3
\set sqs_msg_id18 4

-- Should return only order messages using SQS-style
SELECT COUNT(*) = 3 FROM pgmq.read_grouped('fifo_test_queue', 10, 10, '{"type": "order"}'::jsonb);

-- Verify we got the correct order messages (skipping notification)
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_grouped('fifo_test_queue', 10, 10, '{"type": "order"}'::jsonb) ORDER BY msg_id
) = ARRAY[:sqs_msg_id15, :sqs_msg_id16, :sqs_msg_id18]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_visibility_timeout
-- SQS-style with visibility timeout
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout1"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout2"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout3"}'::jsonb, '{"x-pgmq-group": "timeout_group"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id19 1
\set sqs_msg_id20 2
\set sqs_msg_id21 3

-- Read with short visibility timeout - should get all 3 messages
SELECT COUNT(*) = 3 FROM pgmq.read_grouped('fifo_test_queue', 1, 10);

-- Should return no messages (all messages still visible)
SELECT COUNT(*) = 0 FROM pgmq.read_grouped('fifo_test_queue', 10, 10);

-- Wait for visibility timeout to expire
SELECT pg_sleep(2);

-- Should now return all messages again
SELECT COUNT(*) = 3 FROM pgmq.read_grouped('fifo_test_queue', 10, 10);
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_grouped('fifo_test_queue', 10, 10) ORDER BY msg_id
) = ARRAY[:sqs_msg_id19, :sqs_msg_id20, :sqs_msg_id21]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_polling
-- SQS-style polling functionality
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test1"}'::jsonb, '{"x-pgmq-group": "poll_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test2"}'::jsonb, '{"x-pgmq-group": "poll_group"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id22 1
\set sqs_msg_id23 2

-- Test SQS-style polling with immediate availability
SELECT COUNT(*) = 2 FROM pgmq.read_grouped_with_poll('fifo_test_queue', 10, 10, 1, 100);
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_grouped_with_poll('fifo_test_queue', 10, 10, 1, 100) ORDER BY msg_id
) = ARRAY[:sqs_msg_id22, :sqs_msg_id23]::bigint[];

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
SELECT COUNT(*) = 3 FROM pgmq.read_grouped('fifo_test_queue', 10, 3);
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_grouped('fifo_test_queue', 10, 3) ORDER BY msg_id
) = ARRAY['A', 'A', 'A']::text[];

-- Reset visibility timeout
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- Test batch size 7 - should get 5 from group A + 2 from group B
SELECT COUNT(*) = 7 FROM pgmq.read_grouped('fifo_test_queue', 10, 7);
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_grouped('fifo_test_queue', 10, 7) ORDER BY msg_id
) = ARRAY['A', 'A', 'A', 'A', 'A', 'B', 'B']::text[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_edge_cases
-- SQS-style edge cases
-- Test with empty FIFO key (should work as default group)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "empty_fifo_sqs"}'::jsonb, '{"x-pgmq-group": ""}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_grouped('fifo_test_queue', 10, 5);

-- Test with null FIFO key (should work as default group)  
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "null_fifo_sqs"}'::jsonb, '{"x-pgmq-group": null}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_grouped('fifo_test_queue', 10, 5);

-- Clean up
SELECT pgmq.drop_queue('fifo_test_queue');

-- Verify queue was dropped
SELECT COUNT(*) = 0 FROM pgmq.list_queues() WHERE queue_name = 'fifo_test_queue';

-- Test completed successfully
SELECT 'FIFO SQS-style tests completed successfully' as result;
