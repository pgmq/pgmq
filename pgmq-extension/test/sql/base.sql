-- CREATE pgmq.
CREATE EXTENSION IF NOT EXISTS pgmq;
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- test_unlogged
-- CREATE with default retention and partition strategy
SELECT pgmq.create_unlogged('test_unlogged_queue');
SELECT * from pgmq.send('test_unlogged_queue', '{"hello": "world"}', '{"header": 1}'::jsonb);
SELECT msg_id, read_ct, enqueued_at > NOW(), vt > NOW(), message, headers
  FROM pgmq.read('test_unlogged_queue', 2, 1);

-- test_max_queue_name_size
-- CREATE with default retention and partition strategy
SELECT pgmq.create(repeat('a', 48));
SELECT pgmq.create(repeat('a', 47));
SELECT pgmq.create_partitioned(repeat('p', 48));
SELECT pgmq.create_partitioned(repeat('p', 47));

-- test_lifecycle
-- CREATE with default retention and partition strategy
SELECT pgmq.create('test_default_queue');

-- creating a queue must be idempotent
-- create with same name again, must be no error
SELECT pgmq.create('test_default_queue');

SELECT * from pgmq.send('test_default_queue', '{"hello": "world"}');

-- read message
-- vt=0, limit=1
\set msg_id 1
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 0, 1);

-- read message using conditional
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1, '{"hello": "world"}');

-- set VT to 5 seconds
SELECT vt > clock_timestamp() + '4 seconds'::interval
  FROM pgmq.set_vt('test_default_queue', :msg_id, 5);

-- read again, assert no messages because we just set VT to the future
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1);

-- read again, now using poll to block until message is ready
SELECT msg_id = :msg_id FROM pgmq.read_with_poll('test_default_queue', 10, 1, 10);

-- set VT to 5 seconds again for another read_with_poll test
SELECT vt > clock_timestamp() + '4 seconds'::interval
  FROM pgmq.set_vt('test_default_queue', :msg_id, 5);

-- read again, now using poll to block until message is ready
SELECT msg_id = :msg_id FROM pgmq.read_with_poll('test_default_queue', 10, 1, 10, 100, '{"hello": "world"}');

-- after reading it, set VT to now
SELECT msg_id = :msg_id FROM pgmq.set_vt('test_default_queue', :msg_id, 0);

-- read again, should have msg_id 1 again
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1);

SELECT pgmq.create('test_default_queue_vt');

-- send message with timestamp
SELECT * from pgmq.send('test_default_queue_vt', '{"hello": "world"}', CURRENT_TIMESTAMP + '5 seconds'::interval);

-- read, assert no messages because we set timestamp to the future
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue_vt', 2, 1);

-- read again, now using poll to block until message is ready
SELECT msg_id = :msg_id FROM pgmq.read_with_poll('test_default_queue_vt', 10, 1, 10);

-- send a batch of 2 messages
SELECT pgmq.create('batch_queue');
SELECT ARRAY( SELECT pgmq.send_batch(
    'batch_queue',
    ARRAY['{"hello": "world_0"}', '{"hello": "world_1"}']::jsonb[],
    ARRAY['{"header": 1}', '{"header": 2}']::jsonb[]
)) = ARRAY[1, 2]::BIGINT[];

SELECT msg_id, message, headers from pgmq.read('batch_queue', 0, 2);

-- Use terse verbosity for validation tests to ensure consistent error output across PG versions
\set VERBOSITY terse

-- test that send_batch validates headers array length matches msgs array length
-- should fail with mismatched array lengths
SELECT pgmq.send_batch(
    'batch_queue',
    ARRAY['{"msg": 1}', '{"msg": 2}']::jsonb[],
    ARRAY['{"header": 1}', '{"header": 2}', '{"header": 3}']::jsonb[]
);

-- test that send_batch validates msgs is not empty
-- should fail with empty msgs array
SELECT pgmq.send_batch(
    'batch_queue',
    ARRAY[]::jsonb[]
);

-- should fail with empty msgs and empty headers array
SELECT pgmq.send_batch(
    'batch_queue',
    ARRAY[]::jsonb[],
    ARRAY[]::jsonb[]
);

-- Restore default verbosity
\set VERBOSITY default

-- send a batch of 2 messages with timestamp
SELECT pgmq.create('batch_queue_vt');
SELECT ARRAY( SELECT pgmq.send_batch(
    'batch_queue_vt',
    ARRAY['{"hello": "world_0"}', '{"hello": "world_1"}']::jsonb[],
    CURRENT_TIMESTAMP + '5 seconds'::interval
)) = ARRAY[1, 2]::BIGINT[];

-- CREATE with 5 seconds per partition, 10 seconds retention
SELECT pgmq.create_partitioned('test_duration_queue', '5 seconds', '10 seconds');

-- CREATE with 10 messages per partition, 20 messages retention
SELECT pgmq.create_partitioned('test_numeric_queue', '10 seconds', '20 seconds');

-- create a queue for metrics
SELECT pgmq.create('test_metrics_queue');

-- doing some operations to get some numbers in
SELECT pgmq.send_batch('test_metrics_queue', ARRAY['1', '2', '3', '4', '5']::jsonb[]);
SELECT pgmq.send_batch('test_metrics_queue', ARRAY['6', '7']::jsonb[], 10);
SELECT pgmq.archive('test_metrics_queue', 1);

-- actually reading metrics
SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, queue_visible_length FROM pgmq.metrics('test_metrics_queue');

-- get metrics all
SELECT COUNT(1) from pgmq.metrics_all();

-- delete an existing queue returns true
select pgmq.create('exists');
select pgmq.drop_queue('exists');
-- delete a queue does not exists returns false
select pgmq.drop_queue('does_not_exist');

-- delete all the queues
-- delete partitioned queues
SELECT pgmq.drop_queue(queue, true)
  FROM unnest('{test_numeric_queue}'::text[]) AS queue;

-- test automatic partitioned status checking
SELECT pgmq.drop_queue(queue)
  FROM unnest('{test_duration_queue}'::text[]) AS queue;

-- drop the rest of the queues
SELECT pgmq.drop_queue(q.queue_name, true)
  FROM (SELECT queue_name FROM pgmq.list_queues()) AS q;

SELECT queue_name FROM pgmq.list_queues();

-- test_archive
SELECT pgmq.create('archive_queue');

-- no messages in the queue
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;

-- no messages in queue archive
SELECT COUNT(*) = 0 FROM pgmq.a_archive_queue;

-- put messages on the queue
\set msg_id1 1::bigint
\set msg_id2 2::bigint
SELECT send = :msg_id1 FROM pgmq.send('archive_queue', '0', '{"headers": 1}'::jsonb);
SELECT send = :msg_id2 FROM pgmq.send('archive_queue', '0');

-- two messages in the queue
SELECT COUNT(*) = 2 FROM pgmq.q_archive_queue;

-- archive the message. The first two exist so the id should be returned, the
-- last one doesn't
SELECT ARRAY(
    SELECT * FROM pgmq.archive('archive_queue', ARRAY[:msg_id1, :msg_id2])
) = ARRAY[:msg_id1, :msg_id2];

-- should be no messages left on the queue table
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;

-- should be two messages in archive
SELECT COUNT(*) = 2 FROM pgmq.a_archive_queue;

\set msg_id3 3::bigint
SELECT send = :msg_id3 FROM pgmq.send('archive_queue', '0');
SELECT COUNT(*) = 1 FROM pgmq.q_archive_queue;
SELECT * FROM pgmq.archive('archive_queue', :msg_id3);
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;
SELECT COUNT(*) = 3 FROM pgmq.a_archive_queue;

-- body and headers are archived
SELECT msg_id, message, headers FROM pgmq.a_archive_queue;

-- test_read_read_with_poll
-- Creating queue
SELECT pgmq.create('test_read_queue');

-- Sending 3 messages to the queue
SELECT send = :msg_id1 FROM pgmq.send('test_read_queue', '0');
SELECT send = :msg_id2 FROM pgmq.send('test_read_queue', '0');
SELECT send = :msg_id3 FROM pgmq.send('test_read_queue', '0');

-- Reading with limit respects the limit
SELECT msg_id = :msg_id1 FROM pgmq.read('test_read_queue', 5, 1);

-- Reading respects the VT
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read('test_read_queue', 10, 5)
) = ARRAY[:msg_id2, :msg_id3];

-- Read with poll will poll until the first message is available
SELECT clock_timestamp() AS start \gset
SELECT msg_id = :msg_id1 FROM pgmq.read_with_poll('test_read_queue', 10, 5, 6, 100);
SELECT clock_timestamp() - :'start' > '3 second'::interval;

-- test_purge_queue
SELECT pgmq.create('test_purge_queue');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');

SELECT * FROM pgmq.purge_queue('test_purge_queue');
SELECT COUNT(*) = 0 FROM pgmq.q_test_purge_queue;

-- test_pop
SELECT pgmq.create('test_pop_queue');
SELECT * FROM pgmq.pop('test_pop_queue');

SELECT send AS first_msg_id from pgmq.send('test_pop_queue', '0') \gset
SELECT * from pgmq.send('test_pop_queue', '0');
SELECT * from pgmq.send('test_pop_queue', '0');

SELECT msg_id = :first_msg_id FROM pgmq.pop('test_pop_queue');

SELECT msg_id from pgmq.pop('test_pop_queue', 10);
SELECT * from pgmq.send('test_pop_queue', '1');
SELECT * from pgmq.send('test_pop_queue', '2');
SELECT * from pgmq.send('test_pop_queue', '3');
SELECT msg_id from pgmq.pop('test_pop_queue', 3);

-- test_set_vt
SELECT pgmq.create('test_set_vt_queue');
SELECT * FROM pgmq.set_vt('test_set_vt_queue', 9999, 0);

SELECT send AS first_msg_id from pgmq.send('test_set_vt_queue', '0') \gset

-- set message invisible for 100 seconds
SELECT msg_id FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, 100);

-- read message, it should not be visible
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- make it visible
SELECT msg_id FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, 0);

-- set vt works if message is readable
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- set message vt to a specific timestamp in the future
SELECT clock_timestamp() + interval '65 seconds' AS future_ts \gset
SELECT msg_id, vt = (:'future_ts')::timestamptz FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, (:'future_ts')::timestamptz);

-- read message, it should not be visible
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- make it visible
SELECT clock_timestamp() AS current_ts \gset
SELECT msg_id, vt = (:'current_ts')::timestamptz FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, (:'current_ts')::timestamptz);

-- set vt works if message is readable again
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- test batch set_vt
SELECT pgmq.create('test_batch_set_vt_queue');

-- send two messages
SELECT send AS batch_msg_id1 from pgmq.send('test_batch_set_vt_queue', '0') \gset
SELECT send AS batch_msg_id2 from pgmq.send('test_batch_set_vt_queue', '0') \gset

-- set both messages invisible for 100 seconds
SELECT ARRAY(
    SELECT msg_id FROM pgmq.set_vt('test_batch_set_vt_queue', ARRAY[:batch_msg_id1, :batch_msg_id2], 100)
) = ARRAY[:batch_msg_id1, :batch_msg_id2]::bigint[];

-- read messages, none should be visible
SELECT COUNT(*) = 0 FROM pgmq.read('test_batch_set_vt_queue', 1, 5);

-- make them visible again using batch set_vt
SELECT ARRAY(
    SELECT msg_id FROM pgmq.set_vt('test_batch_set_vt_queue', ARRAY[:batch_msg_id1, :batch_msg_id2], 0)
) = ARRAY[:batch_msg_id1, :batch_msg_id2]::bigint[];

-- both messages should be visible now
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read('test_batch_set_vt_queue', 1, 5)
) = ARRAY[:batch_msg_id1, :batch_msg_id2]::bigint[];

-- set both messages vt to a specific timestamp in the future
SELECT clock_timestamp() + interval '65 seconds' AS future_ts \gset
SELECT msg_id, vt = (:'future_ts')::timestamptz FROM pgmq.set_vt('test_batch_set_vt_queue', ARRAY[:batch_msg_id1, :batch_msg_id2], (:'future_ts')::timestamptz);

-- read messages, none should be visible
SELECT COUNT(*) = 0 FROM pgmq.read('test_batch_set_vt_queue', 1, 5);

-- make them visible again using batch set_vt
SELECT clock_timestamp() AS current_ts \gset
SELECT msg_id, vt = (:'current_ts')::timestamptz FROM pgmq.set_vt('test_batch_set_vt_queue', ARRAY[:batch_msg_id1, :batch_msg_id2], (:'current_ts')::timestamptz);

-- both messages should be visible again
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read('test_batch_set_vt_queue', 1, 5)
) = ARRAY[:batch_msg_id1, :batch_msg_id2]::bigint[];

-- test_partitioned_delete
\set partition_interval 2
\set retention_interval 2

-- We first will drop pg_partman and assert that create fails without the
-- extension installed
DROP EXTENSION pg_partman;

SELECT * FROM pgmq.create_partitioned(
    'test_partitioned_queue',
    :'partition_interval',
    :'retention_interval'
);

-- With the extension existing, the queue is created successfully
CREATE EXTENSION pg_partman;
SELECT * FROM pgmq.create_partitioned(
    'test_partitioned_queue',
    :'partition_interval',
    :'retention_interval'
);

-- queue shows up in list queues
SELECT queue_name FROM pgmq.list_queues()
 WHERE queue_name = 'test_partitioned_queue';

-- Sending 3 messages to the queue
SELECT send AS msg_id1 from pgmq.send('test_partitioned_queue', '0') \gset
SELECT send AS msg_id2 from pgmq.send('test_partitioned_queue', '0') \gset
SELECT send AS msg_id3 from pgmq.send('test_partitioned_queue', '0') \gset

SELECT COUNT(*) = 3 FROM pgmq.q_test_partitioned_queue;

-- Deleting message 3
SELECT * FROM pgmq.delete('test_partitioned_queue', :msg_id3);
SELECT COUNT(*) = 2 FROM pgmq.q_test_partitioned_queue;

-- Deleting batch
SELECT ARRAY(
    SELECT archive FROM pgmq.archive(
        'test_partitioned_queue',
        ARRAY[:msg_id1, :msg_id2, :msg_id3, -3]
    )
) = ARRAY[:msg_id1, :msg_id2]::bigint[];

-- test_transaction_create
BEGIN;
SELECT pgmq.create('transaction_test_queue');
ROLLBACK;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'q_transaction_test_queue';

-- test_detach_archive
SELECT pgmq.create('detach_archive_queue');
DROP EXTENSION pgmq CASCADE;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'a_detach_archive_queue';

-- queues and archive remains
CREATE EXTENSION pgmq;
SELECT pgmq.create('queue_remains');
DROP EXTENSION pgmq CASCADE;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'q_queue_remains';
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'a_queue_remains';

--Truncated Index When queue name is max.
CREATE EXTENSION pgmq;
SELECT pgmq.create('long_queue_name_123456789012345678901234567890');
SELECT pgmq.convert_archive_partitioned('long_queue_name_123456789012345678901234567890');

--Check for archive is already partitioned
SELECT pgmq.convert_archive_partitioned('long_queue_name_123456789012345678901234567890');

--Error out due to Index duplicate index at old table.
SELECT pgmq.create('long_queue_name_1234567890123456789012345678901');
SELECT pgmq.convert_archive_partitioned('long_queue_name_1234567890123456789012345678901');

--Success
SELECT pgmq.create('long_queue_name_');
SELECT pgmq.convert_archive_partitioned('long_queue_name_');

-- test_convert_archive_partitioned_time_based
-- Test that convert_archive_partitioned works with time-based intervals
SELECT pgmq.create('time_based_convert_queue');
-- Send and archive some messages to populate the archive table
SELECT pgmq.send('time_based_convert_queue', '{"test": 1}');
SELECT pgmq.send('time_based_convert_queue', '{"test": 2}');
SELECT pgmq.archive('time_based_convert_queue', 1);
SELECT pgmq.archive('time_based_convert_queue', 2);
-- Convert archive to time-based partitioned (should not error with "invalid input syntax for type bigint")
SELECT pgmq.convert_archive_partitioned('time_based_convert_queue', '1 hour', '2 hours');
-- Verify the archive table is now partitioned
SELECT c.relkind = 'p' AS is_partitioned
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'pgmq'
  AND c.relname = 'a_time_based_convert_queue';

\set SHOW_CONTEXT never

--Failed SQL injection attack
SELECT pgmq.create('abc');
SELECT
   pgmq.delete(
     'abc where false;
     create table public.attack_vector(id int);
     delete from pgmq.q_abc',
     1
  );

--Special characters in queue name
SELECT pgmq.create('queue-hyphened');
SELECT pgmq.send('queue-hyphened', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('queue-hyphened', 1, 1);
SELECT pgmq.archive('queue-hyphened', 1);

SELECT pgmq.create('QueueCased');
SELECT pgmq.send('QueueCased', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('QueueCased', 1, 1);
SELECT pgmq.archive('QueueCased', 1);

SELECT pgmq.create_partitioned('queue-hyphened-part');
SELECT pgmq.send('queue-hyphened-part', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('queue-hyphened-part', 1, 1);
SELECT pgmq.archive('queue-hyphened-part', 1);

SELECT pgmq.create_partitioned('QueueCasedPart');
SELECT pgmq.send('QueueCasedPart', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('QueueCasedPart', 1, 1);
SELECT pgmq.archive('QueueCasedPart', 1);

-- fails with invalid queue name
SELECT pgmq.create('dollar$-signed');
SELECT pgmq.create_partitioned('dollar$-signed-part');

-- input validation success
SELECT pgmq.format_table_name('cat', 'q');
SELECT pgmq.format_table_name('cat-dog', 'a');
SELECT pgmq.format_table_name('cat_dog', 'q');

-- input validation failure
SELECT pgmq.format_table_name('dollar$fail', 'q');
SELECT pgmq.format_table_name('double--hyphen-fail', 'a');
SELECT pgmq.format_table_name('semicolon;fail', 'a');
SELECT pgmq.format_table_name($$single'quote-fail$$, 'a');

-- test null message
SELECT pgmq.create('null_message_queue');
SELECT pgmq.send('null_message_queue', NULL);
SELECT msg_id, read_ct, message FROM pgmq.read('null_message_queue', 1, 1);

-- test_last_read_at
-- Tests that last_read_at is properly set and returned by read functions

-- test_last_read_at_read
SELECT pgmq.create('test_last_read_at_read');
SELECT pgmq.send('test_last_read_at_read', '{"test": "read"}');
-- First read: last_read_at should be set
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read('test_last_read_at_read', 0, 1);

-- test_last_read_at_read_with_poll
SELECT pgmq.create('test_last_read_at_rwp');
SELECT pgmq.send('test_last_read_at_rwp', '{"test": "read_with_poll"}');
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read_with_poll('test_last_read_at_rwp', 0, 1, 1);

-- test_last_read_at_pop
SELECT pgmq.create('test_last_read_at_pop');
SELECT pgmq.send('test_last_read_at_pop', '{"test": "pop"}');
-- Pop returns the message with last_read_at (should be NULL since never read)
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NULL AS last_read_at_is_null_before_read
FROM pgmq.pop('test_last_read_at_pop');
-- Send another, read it first, then pop
SELECT pgmq.send('test_last_read_at_pop', '{"test": "pop2"}');
SELECT msg_id FROM pgmq.read('test_last_read_at_pop', 0, 1);
SELECT
    msg_id = 2 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at_after_read
FROM pgmq.pop('test_last_read_at_pop');

-- test_last_read_at_set_vt
SELECT pgmq.create('test_last_read_at_set_vt');
SELECT pgmq.send('test_last_read_at_set_vt', '{"test": "set_vt"}');
-- set_vt returns the message with last_read_at column
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NULL AS last_read_at_null_before_read
FROM pgmq.set_vt('test_last_read_at_set_vt', 1, 0);
-- Read the message to set last_read_at
SELECT msg_id FROM pgmq.read('test_last_read_at_set_vt', 0, 1);
-- Now set_vt should show last_read_at is set
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at
FROM pgmq.set_vt('test_last_read_at_set_vt', 1, 0);

-- test_last_read_at_set_vt_batch
SELECT pgmq.create('test_last_read_at_set_vt_batch');
SELECT pgmq.send('test_last_read_at_set_vt_batch', '{"test": "batch1"}');
SELECT pgmq.send('test_last_read_at_set_vt_batch', '{"test": "batch2"}');
-- Read both messages
SELECT msg_id FROM pgmq.read('test_last_read_at_set_vt_batch', 0, 2);
-- Batch set_vt should return messages with last_read_at set
SELECT
    COUNT(*) = 2 AS both_messages_returned,
    COUNT(*) FILTER (WHERE last_read_at IS NOT NULL) = 2 AS both_have_last_read_at
FROM pgmq.set_vt('test_last_read_at_set_vt_batch', ARRAY[1, 2], 0);

-- test_last_read_at_read_grouped
SELECT pgmq.create('test_last_read_at_rg');
SELECT pgmq.send('test_last_read_at_rg', '{"test": "grouped"}', '{"x-pgmq-group": "group1"}'::jsonb);
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read_grouped('test_last_read_at_rg', 0, 1);

-- test_last_read_at_read_grouped_with_poll
SELECT pgmq.create('test_last_read_at_rgwp');
SELECT pgmq.send('test_last_read_at_rgwp', '{"test": "grouped_poll"}', '{"x-pgmq-group": "group1"}'::jsonb);
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read_grouped_with_poll('test_last_read_at_rgwp', 0, 1, 1);

-- test_last_read_at_read_grouped_rr
SELECT pgmq.create('test_last_read_at_rgrr');
SELECT pgmq.send('test_last_read_at_rgrr', '{"test": "rr"}', '{"x-pgmq-group": "group1"}'::jsonb);
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read_grouped_rr('test_last_read_at_rgrr', 0, 1);

-- test_last_read_at_read_grouped_rr_with_poll
SELECT pgmq.create('test_last_read_at_rgrrwp');
SELECT pgmq.send('test_last_read_at_rgrrwp', '{"test": "rr_poll"}', '{"x-pgmq-group": "group1"}'::jsonb);
SELECT
    msg_id = 1 AS correct_msg_id,
    last_read_at IS NOT NULL AS has_last_read_at,
    last_read_at <= clock_timestamp() AS last_read_at_is_valid
FROM pgmq.read_grouped_rr_with_poll('test_last_read_at_rgrrwp', 0, 1, 1);

-- test_last_read_at_updates_on_reread
-- Verify that last_read_at updates each time a message is read
SELECT pgmq.create('test_last_read_at_reread');
SELECT pgmq.send('test_last_read_at_reread', '{"test": "reread"}');
-- First read
SELECT last_read_at AS first_read_at FROM pgmq.read('test_last_read_at_reread', 0, 1) \gset
SELECT pg_sleep(0.1);
-- Second read
SELECT last_read_at AS second_read_at FROM pgmq.read('test_last_read_at_reread', 0, 1) \gset
-- Second read should have a later timestamp
SELECT :'second_read_at'::timestamptz > :'first_read_at'::timestamptz AS last_read_at_updated;

-- =============================================================================
-- Tests for notify_insert throttle functions
-- =============================================================================

-- test_list_notify_insert_throttles_empty
-- Verify list_notify_insert_throttles returns empty when no throttles are configured
SELECT COUNT(*) = 0 FROM pgmq.list_notify_insert_throttles();

-- test_list_notify_insert_throttles_returns_all
-- Create queues and enable notify_insert with different throttle intervals
SELECT pgmq.create('notify_queue_1');
SELECT pgmq.create('notify_queue_2');
SELECT pgmq.create('notify_queue_3');

SELECT pgmq.enable_notify_insert('notify_queue_1', 100);
SELECT pgmq.enable_notify_insert('notify_queue_2', 250);
SELECT pgmq.enable_notify_insert('notify_queue_3', 500);

-- Should return 3 throttle configurations
SELECT COUNT(*) = 3 FROM pgmq.list_notify_insert_throttles();

-- Verify structure and values
SELECT
    COUNT(*) = 3 AS has_all_throttles,
    bool_and(queue_name IS NOT NULL) AS has_queue_name,
    bool_and(throttle_interval_ms IS NOT NULL) AS has_throttle_interval,
    bool_and(last_notified_at IS NOT NULL) AS has_last_notified_at
FROM pgmq.list_notify_insert_throttles();

-- Verify specific throttle values
SELECT throttle_interval_ms = 100 FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_1';
SELECT throttle_interval_ms = 250 FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_2';
SELECT throttle_interval_ms = 500 FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_3';

-- test_update_notify_insert_updates_throttle
-- Update throttle interval for a queue
SELECT pgmq.update_notify_insert('notify_queue_1', 300);

-- Verify throttle was updated
SELECT throttle_interval_ms = 300 FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_1';

-- Verify last_notified_at was reset
SELECT last_notified_at = to_timestamp(0) FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_1';

-- test_update_notify_insert_validates_non_negative
-- Verify update_notify_insert rejects negative values
DO $$
BEGIN
    PERFORM pgmq.update_notify_insert('notify_queue_1', -1);
    RAISE EXCEPTION 'Should have raised an error for negative throttle_interval_ms';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%must be non-negative%' THEN
        RAISE EXCEPTION 'Expected non-negative error, got: %', SQLERRM;
    END IF;
END $$;

-- test_update_notify_insert_requires_queue_exists
-- Verify update_notify_insert fails for non-existent queues
DO $$
BEGIN
    PERFORM pgmq.update_notify_insert('nonexistent_queue', 100);
    RAISE EXCEPTION 'Should have raised an error for non-existent queue';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%does not exist%' THEN
        RAISE EXCEPTION 'Expected queue does not exist error, got: %', SQLERRM;
    END IF;
END $$;

-- test_update_notify_insert_requires_enabled_queue
-- Verify update_notify_insert fails for queues without notify_insert enabled
SELECT pgmq.create('notify_queue_disabled');

DO $$
BEGIN
    PERFORM pgmq.update_notify_insert('notify_queue_disabled', 100);
    RAISE EXCEPTION 'Should have raised an error for queue without notify_insert enabled';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%does not have notify_insert enabled%' THEN
        RAISE EXCEPTION 'Expected notify_insert not enabled error, got: %', SQLERRM;
    END IF;
END $$;

-- test_update_notify_insert_allows_zero
-- Verify update_notify_insert accepts 0 (no throttling)
SELECT pgmq.update_notify_insert('notify_queue_2', 0);
SELECT throttle_interval_ms = 0 FROM pgmq.list_notify_insert_throttles() WHERE queue_name = 'notify_queue_2';

-- Clean up notify test queues
SELECT pgmq.drop_queue('notify_queue_1');
SELECT pgmq.drop_queue('notify_queue_2');
SELECT pgmq.drop_queue('notify_queue_3');
SELECT pgmq.drop_queue('notify_queue_disabled');

-- Verify throttles were removed after dropping queues (CASCADE should handle this)
SELECT COUNT(*) = 0 FROM pgmq.list_notify_insert_throttles();

--Cleanup tests
DROP EXTENSION pgmq CASCADE;
DROP EXTENSION pg_partman CASCADE;
