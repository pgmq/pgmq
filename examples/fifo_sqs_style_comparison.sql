-- PGMQ FIFO SQS-Style Comparison Example
-- This example demonstrates the difference between pgmq.read_fifo() and pgmq.read_fifo_sqs_style()

-- Create a test queue
SELECT pgmq.create('fifo_comparison_test');

-- Create FIFO index for better performance
SELECT pgmq.create_fifo_index('fifo_comparison_test');

-- Setup test data: Multiple messages per group
-- Group A: 5 messages
SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "A", "message_num": 1, "data": "First message for group A"}',
    '{"x-pgmq-fifo": "group_A"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "A", "message_num": 2, "data": "Second message for group A"}',
    '{"x-pgmq-fifo": "group_A"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "A", "message_num": 3, "data": "Third message for group A"}',
    '{"x-pgmq-fifo": "group_A"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "A", "message_num": 4, "data": "Fourth message for group A"}',
    '{"x-pgmq-fifo": "group_A"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "A", "message_num": 5, "data": "Fifth message for group A"}',
    '{"x-pgmq-fifo": "group_A"}'
);

-- Group B: 3 messages
SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "B", "message_num": 1, "data": "First message for group B"}',
    '{"x-pgmq-fifo": "group_B"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "B", "message_num": 2, "data": "Second message for group B"}',
    '{"x-pgmq-fifo": "group_B"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "B", "message_num": 3, "data": "Third message for group B"}',
    '{"x-pgmq-fifo": "group_B"}'
);

-- Group C: 2 messages
SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "C", "message_num": 1, "data": "First message for group C"}',
    '{"x-pgmq-fifo": "group_C"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "C", "message_num": 2, "data": "Second message for group C"}',
    '{"x-pgmq-fifo": "group_C"}'
);

-- Test 1: Original pgmq.read_fifo() behavior
-- Should return 1 message per group (3 total messages)
SELECT 
    'Original FIFO' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    headers->>'x-pgmq-fifo' as fifo_group
FROM pgmq.read_fifo('fifo_comparison_test', 30, 10)
ORDER BY msg_id;

-- Reset visibility timeout for all messages to test again
UPDATE pgmq.q_fifo_comparison_test SET vt = clock_timestamp() - interval '1 second';

-- Test 2: New pgmq.read_fifo_sqs_style() behavior
-- Should attempt to return as many messages as possible from the same group
-- Expected: Up to 10 messages, prioritizing group A (which has 5), then B, then C
SELECT 
    'SQS-Style FIFO' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    headers->>'x-pgmq-fifo' as fifo_group
FROM pgmq.read_fifo_sqs_style('fifo_comparison_test', 30, 10)
ORDER BY msg_id;

-- Reset again for next test
UPDATE pgmq.q_fifo_comparison_test SET vt = clock_timestamp() - interval '1 second';

-- Test 3: SQS-Style with smaller batch size
-- Request only 3 messages - should get all 3 from group A (the earliest group)
SELECT 
    'SQS-Style FIFO (batch=3)' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    headers->>'x-pgmq-fifo' as fifo_group
FROM pgmq.read_fifo_sqs_style('fifo_comparison_test', 30, 3)
ORDER BY msg_id;

-- Reset again
UPDATE pgmq.q_fifo_comparison_test SET vt = clock_timestamp() - interval '1 second';

-- Test 4: SQS-Style with batch size 7
-- Should get 5 from group A + 2 from group B (next earliest)
SELECT 
    'SQS-Style FIFO (batch=7)' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    headers->>'x-pgmq-fifo' as fifo_group
FROM pgmq.read_fifo_sqs_style('fifo_comparison_test', 30, 7)
ORDER BY msg_id;

-- Test 5: Demonstrate behavior after processing some messages
-- Delete the first 2 messages from group A
DELETE FROM pgmq.q_fifo_comparison_test WHERE msg_id IN (1, 2);

-- Reset visibility for remaining messages
UPDATE pgmq.q_fifo_comparison_test SET vt = clock_timestamp() - interval '1 second';

-- Now test SQS-style again - should start from message 3 in group A
SELECT 
    'SQS-Style after processing' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    headers->>'x-pgmq-fifo' as fifo_group
FROM pgmq.read_fifo_sqs_style('fifo_comparison_test', 30, 10)
ORDER BY msg_id;

-- Test 6: Test with mixed groups (some with FIFO header, some without)
-- Add messages without FIFO header (will go to default group)
SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "default", "message_num": 1, "data": "Default group message 1"}'
);

SELECT pgmq.send('fifo_comparison_test', 
    '{"group": "default", "message_num": 2, "data": "Default group message 2"}'
);

-- Reset visibility
UPDATE pgmq.q_fifo_comparison_test SET vt = clock_timestamp() - interval '1 second';

-- Test SQS-style with mixed groups
SELECT 
    'SQS-Style with default group' as test_type,
    msg_id,
    message->>'group' as group_id,
    message->>'message_num' as message_num,
    COALESCE(headers->>'x-pgmq-fifo', 'default') as fifo_group
FROM pgmq.read_fifo_sqs_style('fifo_comparison_test', 30, 10)
ORDER BY msg_id;

-- Summary of expected behavior differences:
-- 
-- Original pgmq.read_fifo():
-- - Returns at most 1 message per FIFO group
-- - Provides "fair" distribution across groups
-- - Good for ensuring all groups get processed
--
-- New pgmq.read_fifo_sqs_style():
-- - Attempts to fill batch from earliest group first
-- - Maximizes throughput for related messages
-- - Mimics AWS SQS FIFO batch retrieval behavior
-- - Better for processing workflows where related messages should be batched

-- Clean up
SELECT pgmq.drop_queue('fifo_comparison_test');
