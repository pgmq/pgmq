-- ONE PER QUEUE FIFO TESTS ONLY
-- This test file validates the read head message per FIFO queue implementation

-- Stabilize output and ensure clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- Setup test environment
SELECT pgmq.create('fifo_test_queue');

-- Create multiple groups with different message counts
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 1}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 2}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 3}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 1}'::jsonb, '{"x-pgmq-group": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 2}'::jsonb, '{"x-pgmq-group": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "C", "message": 1}'::jsonb, '{"x-pgmq-group": "group_C"}'::jsonb);

-- Verify we have 6 messages in queue
SELECT COUNT(*) = 6 FROM pgmq.q_fifo_test_queue;

-- Request 3 messages - should get only 1st message of each group (only 3 groups)
SELECT COUNT(*) = 3 FROM pgmq.read_grouped_head('fifo_test_queue', 10, 4);

-- reset visibility
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- Verify the messages are from groups A, B, C in correct order
SELECT ARRAY(
    SELECT (message->>'group')::text FROM pgmq.read_grouped_head('fifo_test_queue', 10, 4) ORDER BY msg_id
) = ARRAY['A', 'B', 'C']::text[];

-- reset visibility
UPDATE pgmq.q_fifo_test_queue SET vt = clock_timestamp() - interval '1 second';

-- Verify first read of 2 messages returns g1-msg1 and g2-msg1, second read returns g3-msg1 and third read returns nothing
-- first read
SELECT ARRAY(
    SELECT CONCAT((message->>'group')::text, '-', message->>'message'::text) FROM pgmq.read_grouped_head('fifo_test_queue', 10, 2)
) = ARRAY['A-1', 'B-1']::text[];
-- second read
SELECT ARRAY(
    SELECT CONCAT((message->>'group')::text, '-', message->>'message'::text) FROM pgmq.read_grouped_head('fifo_test_queue', 10, 2)
) = ARRAY['C-1']::text[];
-- third read
SELECT ARRAY(
    SELECT CONCAT((message->>'group')::text, '-', message->>'message'::text) FROM pgmq.read_grouped_head('fifo_test_queue', 10, 2)
) = ARRAY[]::text[];


-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');

-- Create message with default group
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 1}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "B", "message": 1}'::jsonb, '{"x-pgmq-group": "group_B"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "A", "message": 2}'::jsonb, '{"x-pgmq-group": "group_A"}'::jsonb);
SELECT * FROM pgmq.send('fifo_test_queue', '{"group": "default", "message": 1}'::jsonb);

-- Verify read in default group
SELECT ARRAY(
    SELECT CONCAT((message->>'group')::text, '-', message->>'message'::text) FROM pgmq.read_grouped_head('fifo_test_queue', 10, 4)
) = ARRAY['A-1', 'B-1', 'default-1']::text[];


-- Clean up for next test
SELECT * FROM pgmq.purge_queue('fifo_test_queue');
-- Cleanup
SELECT pgmq.drop_queue('fifo_test_queue');