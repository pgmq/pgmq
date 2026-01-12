-- ROUND-ROBIN FIFO TESTS
-- This test file validates the round-robin FIFO queue implementation for both
-- read_grouped_rr (non-polling) and read_grouped_rr_with_poll (polling) functions

-- Stabilize messages and ensure a clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- Create a dedicated queue
SELECT pgmq.create('fifo_rr_queue');

-- test_basic_round_robin_three_groups
-- Insert heads for 3 groups
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

-- Validate we get the three heads in layered round-robin order
WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 3)
)
SELECT
    (SELECT COUNT(*) FROM results) = 3 as count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM results)
        = ARRAY['g1','g2','g3']::text[] as correct_rr_order;

-- Clean up for next test
SELECT pgmq.drop_queue('fifo_rr_queue');
SELECT pgmq.create('fifo_rr_queue');

-- test_layered_round_robin_pattern
-- Insert 5 messages per group for 3 groups, in layered order to fix priorities
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

-- Verify total messages
SELECT COUNT(*) = 15 FROM pgmq.q_fifo_rr_queue;

-- Validate layered round-robin pattern for first 10 picks
WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 10 as count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM results)
        = ARRAY['g1','g2','g3','g1','g2','g3','g1','g2','g3','g1']::text[] as correct_layered_rr;

-- Reset visibility and validate full layering by taking all 15
UPDATE pgmq.q_fifo_rr_queue SET vt = clock_timestamp() - interval '1 second';

WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 15)
)
SELECT
    (SELECT COUNT(*) FROM results) = 15 as count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM results)
        = ARRAY['g1','g2','g3','g1','g2','g3','g1','g2','g3','g1','g2','g3','g1','g2','g3']::text[] as all_15_layered;

-- Clean up for next test
SELECT pgmq.drop_queue('fifo_rr_queue');
SELECT pgmq.create('fifo_rr_queue');

-- test_round_robin_with_unequal_groups
-- Test with unequal group sizes: g1 has 5 messages, g2 has 2 messages
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);

WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 10)
)
SELECT
    (SELECT COUNT(*) FROM results) = 7 as count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM results)
        = ARRAY['g1','g2','g1','g2','g1','g1','g1']::text[] as unequal_groups_rr;

-- Clean up for next test
SELECT pgmq.drop_queue('fifo_rr_queue');
SELECT pgmq.create('fifo_rr_queue');

-- test_round_robin_respects_fifo_within_group
-- Verify that within each group, messages are processed in FIFO order
-- while round-robining between groups
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"orders","seq":1}'::jsonb, '{"x-pgmq-group":"orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"payments","seq":1}'::jsonb, '{"x-pgmq-group":"payments"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"orders","seq":2}'::jsonb, '{"x-pgmq-group":"orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"payments","seq":2}'::jsonb, '{"x-pgmq-group":"payments"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"orders","seq":3}'::jsonb, '{"x-pgmq-group":"orders"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"group":"payments","seq":3}'::jsonb, '{"x-pgmq-group":"payments"}'::jsonb);

WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 6)
)
SELECT
    (SELECT COUNT(*) FROM results) = 6 as count_correct,
    (SELECT ARRAY_AGG((message->>'group')::text ORDER BY msg_id) FROM results)
        = ARRAY['orders','payments','orders','payments','orders','payments']::text[] as correct_rr_between_groups,
    (SELECT ARRAY_AGG((message->>'seq')::int ORDER BY msg_id) FROM results)
        = ARRAY[1,1,2,2,3,3]::int[] as fifo_within_each_group;

-- Clean up for next test
SELECT pgmq.drop_queue('fifo_rr_queue');
SELECT pgmq.create('fifo_rr_queue');

-- test_round_robin_with_default_group
-- Test that messages without x-pgmq-group header use default group
SELECT * FROM pgmq.send('fifo_rr_queue', '{"msg":"no_header_1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"msg":"with_header"}'::jsonb, '{"x-pgmq-group":"explicit"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_queue', '{"msg":"no_header_2"}'::jsonb);

-- Should round-robin between default group and explicit group
-- Expected order: no_header_1 (default), with_header (explicit), no_header_2 (default)
WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_queue', 10, 3)
)
SELECT
    (SELECT COUNT(*) FROM results) = 3 as count_correct,
    (SELECT ARRAY_AGG((message->>'msg')::text ORDER BY msg_id) FROM results)
        = ARRAY['no_header_1', 'with_header', 'no_header_2']::text[] as correct_alternating_pattern;

-- Cleanup
SELECT pgmq.drop_queue('fifo_rr_queue');

-- test_round_robin_with_poll
-- Test that the polling variant works with immediate message availability
SELECT pgmq.create('fifo_rr_poll_queue');

-- Insert messages across 3 groups
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);

-- Validate polling returns messages with correct round-robin order
WITH results AS (
    SELECT * FROM pgmq.read_grouped_rr_with_poll('fifo_rr_poll_queue', 10, 5, 1, 100)
)
SELECT
    (SELECT COUNT(*) FROM results) = 5 as count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM results)
        = ARRAY['g1','g2','g3','g1','g2']::text[] as correct_poll_order;

-- Cleanup
SELECT pgmq.drop_queue('fifo_rr_poll_queue');

-- test for round robin batch reads lose message ordering
-- when messages become visible again after VT expires, they should maintain FIFO order within each group
SELECT pgmq.create('fifo_rr_order_test');

-- Send 3 messages: 2 to user123, 1 to user456
SELECT * FROM pgmq.send('fifo_rr_order_test', '{"order": 1}'::jsonb, '{"x-pgmq-group":"user123"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_order_test', '{"order": 2}'::jsonb, '{"x-pgmq-group":"user123"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_order_test', '{"order": 1}'::jsonb, '{"x-pgmq-group":"user456"}'::jsonb);

-- first read gets 2 messages (1 from each group in round-robin)
WITH first_read AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_order_test', 2, 2)
)
SELECT
    (SELECT COUNT(*) FROM first_read) = 2 as first_read_count_correct,
    (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM first_read)
        = ARRAY['user123','user456']::text[] as first_read_groups_correct;

-- second read should get nothing (messages in flight)
WITH second_read AS (
    SELECT * FROM pgmq.read_grouped_rr('fifo_rr_order_test', 2, 2)
)
SELECT (SELECT COUNT(*) FROM second_read) = 0 as second_read_empty;

-- Manually expire the visibility timeout for all messages
UPDATE pgmq.q_fifo_rr_order_test SET vt = clock_timestamp() - interval '1 second';

-- third read after VT expires, should maintain FIFO order within groups
-- expected order: user123-msg1, user456-msg1, user123-msg2
WITH third_read AS (
    SELECT
        msg_id,
        message->>'order' as msg_order,
        headers->>'x-pgmq-group' as grp,
        ROW_NUMBER() OVER () as return_order
    FROM pgmq.read_grouped_rr('fifo_rr_order_test', 2, 5)
)
SELECT
    (SELECT COUNT(*) FROM third_read) = 3 as third_read_count_correct,
    -- Verify round-robin layering (in the order returned, not by msg_id)
    (SELECT ARRAY_AGG(grp ORDER BY return_order) FROM third_read)
        = ARRAY['user123','user456','user123']::text[] as correct_rr_layering,
    -- very important, verify FIFO within user123 group (msg1 before msg2)
    -- When we look at user123 messages in return order, seq should be [1,2]
    (SELECT ARRAY_AGG(msg_order::int ORDER BY return_order) FILTER (WHERE grp = 'user123') FROM third_read)
        = ARRAY[1,2]::int[] as fifo_maintained_in_user123;

-- Cleanup
SELECT pgmq.drop_queue('fifo_rr_order_test');