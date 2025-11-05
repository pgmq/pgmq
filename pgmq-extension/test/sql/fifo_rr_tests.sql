-- Tests for read_grouped_rr_with_poll round-robin polling across FIFO groups

-- Stabilize messages and ensure a clean extension state
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pgmq CASCADE;
CREATE EXTENSION pgmq;

-- Create a dedicated queue
SELECT pgmq.create('fifo_rr_poll_queue');

-- Insert heads for 3 groups
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

-- Validate we can poll and get the three heads in layered order immediately (single evaluation)
WITH rr AS (
  SELECT * FROM pgmq.read_grouped_rr_with_poll('fifo_rr_poll_queue', 10, 3, 1, 100)
)
SELECT
  (SELECT COUNT(*) FROM rr) = 3,
  (SELECT ARRAY_AGG(headers->>'x-pgmq-group' ORDER BY msg_id) FROM rr) = ARRAY['g1','g2','g3'];

-- Drop and recreate for extended validations similar to non-poll test
SELECT pgmq.drop_queue('fifo_rr_poll_queue');
SELECT pgmq.create('fifo_rr_poll_queue');

-- Insert 5 messages per group for 3 groups, in layered order to fix priorities
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":1}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":2}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":3}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":4}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_rr_poll_queue', '{"n":5}'::jsonb, '{"x-pgmq-group":"g3"}'::jsonb);

-- Verify total messages
SELECT COUNT(*) = 15 FROM pgmq.q_fifo_rr_poll_queue;

-- Reset visibility so results are readable and comparable in one call
UPDATE pgmq.q_fifo_rr_poll_queue SET vt = clock_timestamp() - interval '1 second';

-- Validate layered round-robin pattern for first 10 picks using polling
SELECT 
  ARRAY(
    SELECT (headers->>'x-pgmq-group')
    FROM pgmq.read_grouped_rr_with_poll('fifo_rr_poll_queue', 10, 10, 1, 100)
    ORDER BY msg_id
  ) = ARRAY['g1','g2','g3','g1','g2','g3','g1','g2','g3','g1'];

-- Reset visibility and validate full layering by taking all 15 using polling
UPDATE pgmq.q_fifo_rr_poll_queue SET vt = clock_timestamp() - interval '1 second';
SELECT COUNT(*) = 15 FROM pgmq.read_grouped_rr_with_poll('fifo_rr_poll_queue', 10, 15, 1, 100);

-- Cleanup
SELECT pgmq.drop_queue('fifo_rr_poll_queue');


