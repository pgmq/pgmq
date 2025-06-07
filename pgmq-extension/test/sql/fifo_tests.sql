-- Test FIFO queue functionality
-- This test file validates the FIFO queue implementation

-- CREATE pgmq extension
CREATE EXTENSION IF NOT EXISTS pgmq;

-- Setup test environment
SELECT pgmq.create('fifo_test_queue');

-- test_fifo_basic_ordering
-- Test 1: Basic FIFO ordering within a single group
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "first"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "second"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "third"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);

-- Set expected message IDs
\set msg_id1 1
\set msg_id2 2
\set msg_id3 3

-- Verify we have 3 messages in queue
SELECT COUNT(*) = 3 FROM pgmq.q_fifo_test_queue;

-- Should return only the first message
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id = :msg_id1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Should return no messages (first message still being processed)
SELECT COUNT(*) = 0 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Delete the first message to allow second to be processed
SELECT * FROM pgmq.delete('fifo_test_queue', :msg_id1);

-- Should now return the second message
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id = :msg_id2 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_multiple_groups
-- Test 2: Multiple FIFO groups processed in parallel
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group1_msg1"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group2_msg1"}'::jsonb, '{"x-pgmq-fifo": "group2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group1_msg2"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group2_msg2"}'::jsonb, '{"x-pgmq-fifo": "group2"}'::jsonb);

-- Set expected message IDs for this test
\set msg_id4 4
\set msg_id5 5
\set msg_id6 6
\set msg_id7 7

-- Verify we have 4 messages in queue
SELECT COUNT(*) = 4 FROM pgmq.q_fifo_test_queue;

-- Should return first message from both groups
SELECT COUNT(*) = 2 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id;
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id
) = ARRAY[:msg_id4, :msg_id5]::bigint[];

-- Should return no more messages (first messages still being processed)
SELECT COUNT(*) = 0 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Delete first messages
SELECT ARRAY(
    SELECT * FROM pgmq.delete('fifo_test_queue', ARRAY[:msg_id4, :msg_id5])
) = ARRAY[:msg_id4, :msg_id5]::bigint[];

-- Should now return second messages from both groups
SELECT COUNT(*) = 2 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id;
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id
) = ARRAY[:msg_id6, :msg_id7]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_mixed_headers
-- Test 3: Messages without FIFO headers (default group behavior)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "no_fifo_1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "no_fifo_2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "with_fifo"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);

-- Set expected message IDs for this test
\set msg_id8 8
\set msg_id9 9
\set msg_id10 10

-- Verify we have 3 messages in queue
SELECT COUNT(*) = 3 FROM pgmq.q_fifo_test_queue;

-- Should return first non-FIFO message and the FIFO message
SELECT COUNT(*) = 2 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id;
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id
) = ARRAY[:msg_id8, :msg_id10]::bigint[];

-- Should return no more messages (first non-FIFO message still being processed)
SELECT COUNT(*) = 0 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Delete processed messages
SELECT ARRAY(
    SELECT * FROM pgmq.delete('fifo_test_queue', ARRAY[:msg_id8, :msg_id10])
) = ARRAY[:msg_id8, :msg_id10]::bigint[];

-- Should now return second non-FIFO message
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id = :msg_id9 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_visibility_timeout
-- Test 4: Visibility timeout behavior with FIFO
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout_test"}'::jsonb, '{"x-pgmq-fifo": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "blocked_by_timeout"}'::jsonb, '{"x-pgmq-fifo": "timeout_group"}'::jsonb);

-- Set expected message IDs for this test
\set msg_id11 11
\set msg_id12 12

-- Verify we have 2 messages in queue
SELECT COUNT(*) = 2 FROM pgmq.q_fifo_test_queue;

-- Read with short visibility timeout
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 1, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 1, 5);
SELECT msg_id = :msg_id11 FROM pgmq.read_fifo('fifo_test_queue', 1, 5);

-- Should return no messages (first message still visible)
SELECT COUNT(*) = 0 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Wait for visibility timeout to expire
SELECT pg_sleep(2);

-- Should now return both messages (first message timeout expired)
SELECT COUNT(*) = 2 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id;
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo('fifo_test_queue', 10, 5) ORDER BY msg_id
) = ARRAY[:msg_id11, :msg_id12]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_conditional_reads
-- Test 5: Conditional reads with FIFO
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "high"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "notification", "priority": "low"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "medium"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);

-- Set expected message IDs for this test
\set msg_id13 13
\set msg_id14 14
\set msg_id15 15

-- Verify we have 3 messages in queue
SELECT COUNT(*) = 3 FROM pgmq.q_fifo_test_queue;

-- Should return only the first order message
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb) ORDER BY msg_id;
SELECT msg_id = :msg_id13 FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb);

-- Delete the first message
SELECT * FROM pgmq.delete('fifo_test_queue', :msg_id13);

-- Should return the third message (skipping notification)
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb);
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb) ORDER BY msg_id;
SELECT msg_id = :msg_id15 FROM pgmq.read_fifo('fifo_test_queue', 10, 5, '{"type": "order"}'::jsonb);

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_index_creation
-- Test 6: FIFO index creation
SELECT pgmq.create_fifo_index('fifo_test_queue');

-- Verify index was created (this will succeed if index exists)
SELECT COUNT(*) >= 1 FROM pg_indexes WHERE tablename LIKE '%fifo_test_queue%' AND indexname LIKE '%fifo_idx%';
SELECT indexname FROM pg_indexes WHERE tablename LIKE '%fifo_test_queue%' AND indexname LIKE '%fifo_idx%';

-- test_fifo_polling
-- Test 7: Polling functionality
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test"}'::jsonb, '{"x-pgmq-fifo": "poll_group"}'::jsonb);

-- Set expected message ID for this test
\set msg_id16 16

-- Test polling with immediate availability
SELECT COUNT(*) = 1 FROM pgmq.read_fifo_with_poll('fifo_test_queue', 10, 1, 1, 100);
SELECT msg_id, message, headers FROM pgmq.read_fifo_with_poll('fifo_test_queue', 10, 1, 1, 100);
SELECT msg_id = :msg_id16 FROM pgmq.read_fifo_with_poll('fifo_test_queue', 10, 1, 1, 100);

-- test_fifo_error_conditions
-- Test 8: Error conditions and edge cases
-- Test with empty FIFO key (should work as default group)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "empty_fifo"}'::jsonb, '{"x-pgmq-fifo": ""}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Test with null FIFO key (should work as default group)  
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "null_fifo"}'::jsonb, '{"x-pgmq-fifo": null}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_fifo('fifo_test_queue', 10, 5);

-- Clean up for SQS-style tests
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- ========================================
-- SQS-STYLE FIFO TESTS
-- ========================================

-- test_fifo_sqs_style_basic_batch_filling
-- Test 9: Basic SQS-style batch filling behavior
-- Create multiple groups with different message counts
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 1}'::jsonb, '{"x-pgmq-fifo": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 2}'::jsonb, '{"x-pgmq-fifo": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 3}'::jsonb, '{"x-pgmq-fifo": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 1}'::jsonb, '{"x-pgmq-fifo": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 2}'::jsonb, '{"x-pgmq-fifo": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "C", "message": 1}'::jsonb, '{"x-pgmq-fifo": "group_C"}'::jsonb);

-- Set expected message IDs for SQS-style tests
\set sqs_msg_id1 19
\set sqs_msg_id2 20
\set sqs_msg_id3 21
\set sqs_msg_id4 22
\set sqs_msg_id5 23
\set sqs_msg_id6 24

-- Verify we have 6 messages in queue
SELECT COUNT(*) = 6 FROM pgmq.q_fifo_test_queue;

-- SQS-style should return multiple messages from the same group (group A first)
-- Request 4 messages - should get all 3 from group A + 1 from group B
SELECT COUNT(*) = 4 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 4);
SELECT msg_id, message, headers FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 4) ORDER BY msg_id;

-- Verify the messages are from groups A and B in correct order
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 4) ORDER BY msg_id
) = ARRAY['A', 'A', 'A', 'B']::text[];

-- Clean up for next SQS test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_vs_regular_fifo
-- Test 10: Compare SQS-style vs regular FIFO behavior
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group1_msg1"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group1_msg2"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group2_msg1"}'::jsonb, '{"x-pgmq-fifo": "group2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "group2_msg2"}'::jsonb, '{"x-pgmq-fifo": "group2"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id7 25
\set sqs_msg_id8 26
\set sqs_msg_id9 27
\set sqs_msg_id10 28

-- Regular FIFO should return 1 message per group (2 total)
SELECT COUNT(*) = 2 FROM pgmq.read_fifo('fifo_test_queue', 10, 10);
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo('fifo_test_queue', 10, 10) ORDER BY msg_id
) = ARRAY[:sqs_msg_id7, :sqs_msg_id9]::bigint[];

-- Reset visibility timeout
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- SQS-style should return all messages from group1 first, then group2
SELECT COUNT(*) = 4 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10);
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10) ORDER BY msg_id
) = ARRAY[:sqs_msg_id7, :sqs_msg_id8, :sqs_msg_id9, :sqs_msg_id10]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_mixed_groups
-- Test 11: SQS-style with mixed groups (with and without FIFO headers)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "default2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo1"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "fifo2"}'::jsonb, '{"x-pgmq-fifo": "group1"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id11 29
\set sqs_msg_id12 30
\set sqs_msg_id13 31
\set sqs_msg_id14 32

-- SQS-style should handle mixed groups correctly
SELECT COUNT(*) = 4 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10);
SELECT msg_id, message, headers FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10) ORDER BY msg_id;

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_conditional_reads
-- Test 12: SQS-style with conditional reads
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "high"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "medium"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "notification", "priority": "low"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"type": "order", "priority": "low"}'::jsonb, '{"x-pgmq-fifo": "orders"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id15 33
\set sqs_msg_id16 34
\set sqs_msg_id17 35
\set sqs_msg_id18 36

-- Should return only order messages using SQS-style
SELECT COUNT(*) = 3 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10, '{"type": "order"}'::jsonb);
SELECT msg_id, message, headers FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10, '{"type": "order"}'::jsonb) ORDER BY msg_id;

-- Verify we got the correct order messages (skipping notification)
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10, '{"type": "order"}'::jsonb) ORDER BY msg_id
) = ARRAY[:sqs_msg_id15, :sqs_msg_id16, :sqs_msg_id18]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_visibility_timeout
-- Test 13: SQS-style with visibility timeout
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout1"}'::jsonb, '{"x-pgmq-fifo": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout2"}'::jsonb, '{"x-pgmq-fifo": "timeout_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "timeout3"}'::jsonb, '{"x-pgmq-fifo": "timeout_group"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id19 37
\set sqs_msg_id20 38
\set sqs_msg_id21 39

-- Read with short visibility timeout - should get all 3 messages
SELECT COUNT(*) = 3 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 1, 10);
SELECT msg_id, message, headers FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 1, 10) ORDER BY msg_id;

-- Should return no messages (all messages still visible)
SELECT COUNT(*) = 0 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10);

-- Wait for visibility timeout to expire
SELECT pg_sleep(2);

-- Should now return all messages again
SELECT COUNT(*) = 3 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10);
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 10) ORDER BY msg_id
) = ARRAY[:sqs_msg_id19, :sqs_msg_id20, :sqs_msg_id21]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_polling
-- Test 14: SQS-style polling functionality
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test1"}'::jsonb, '{"x-pgmq-fifo": "poll_group"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "poll_test2"}'::jsonb, '{"x-pgmq-fifo": "poll_group"}'::jsonb);

-- Set expected message IDs
\set sqs_msg_id22 40
\set sqs_msg_id23 41

-- Test SQS-style polling with immediate availability
SELECT COUNT(*) = 2 FROM pgmq.read_fifo_sqs_style_with_poll('fifo_test_queue', 10, 10, 1, 100);
SELECT msg_id, message, headers FROM pgmq.read_fifo_sqs_style_with_poll('fifo_test_queue', 10, 10, 1, 100) ORDER BY msg_id;
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read_fifo_sqs_style_with_poll('fifo_test_queue', 10, 10, 1, 100) ORDER BY msg_id
) = ARRAY[:sqs_msg_id22, :sqs_msg_id23]::bigint[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_batch_sizes
-- Test 15: SQS-style with different batch sizes
-- Create 5 messages in group A, 3 in group B
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 1}'::jsonb, '{"x-pgmq-fifo": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 2}'::jsonb, '{"x-pgmq-fifo": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 3}'::jsonb, '{"x-pgmq-fifo": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 4}'::jsonb, '{"x-pgmq-fifo": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "seq": 5}'::jsonb, '{"x-pgmq-fifo": "batch_group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 1}'::jsonb, '{"x-pgmq-fifo": "batch_group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 2}'::jsonb, '{"x-pgmq-fifo": "batch_group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "seq": 3}'::jsonb, '{"x-pgmq-fifo": "batch_group_B"}'::jsonb);

-- Test batch size 3 - should get 3 messages from group A
SELECT COUNT(*) = 3 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 3);
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 3) ORDER BY msg_id
) = ARRAY['A', 'A', 'A']::text[];

-- Reset visibility timeout
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- Test batch size 7 - should get 5 from group A + 2 from group B
SELECT COUNT(*) = 7 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 7);
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 7) ORDER BY msg_id
) = ARRAY['A', 'A', 'A', 'A', 'A', 'B', 'B']::text[];

-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- test_fifo_sqs_style_edge_cases
-- Test 16: SQS-style edge cases
-- Test with empty FIFO key (should work as default group)
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "empty_fifo_sqs"}'::jsonb, '{"x-pgmq-fifo": ""}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 5);

-- Test with null FIFO key (should work as default group)  
SELECT * FROM pgmq.send('fifo_test_queue', '{"message": "null_fifo_sqs"}'::jsonb, '{"x-pgmq-fifo": null}'::jsonb);
SELECT COUNT(*) = 1 FROM pgmq.read_fifo_sqs_style('fifo_test_queue', 10, 5);

-- Clean up
SELECT pgmq.drop_queue('fifo_test_queue');

-- Verify queue was dropped
SELECT COUNT(*) = 0 FROM pgmq.list_queues() WHERE queue_name = 'fifo_test_queue';

-- Test completed successfully
SELECT 'FIFO tests completed successfully' as result;
