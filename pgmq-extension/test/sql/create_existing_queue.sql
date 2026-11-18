-- create() / create_unlogged() on a queue that already exists: no DDL and no catalog writes when
-- everything is in place, and anything that is missing still gets (re)created.
CREATE EXTENSION IF NOT EXISTS pgmq;
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- xmin of every catalog row create() could write for a queue: the queue/archive tables, the msg_id
-- sequence, both indexes, and the vt/enqueued_at columns that carry the pg_monitor grants.
CREATE TEMP VIEW queue_catalog_rows AS
  SELECT c.relname AS name, c.xmin::text AS row_xmin
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'pgmq'
  UNION ALL
  SELECT c.relname || '.' || a.attname, a.xmin::text
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'pgmq' AND a.attname IN ('vt', 'enqueued_at');

-- test_create_existing_queue_writes_nothing
SELECT pgmq.create('existing_q');
CREATE TEMP TABLE rows_before AS
  SELECT * FROM queue_catalog_rows WHERE name LIKE '%existing_q%';
SELECT pgmq.create('existing_q');
SELECT b.name, b.row_xmin = r.row_xmin AS unchanged
  FROM rows_before b JOIN queue_catalog_rows r USING (name)
 ORDER BY b.name;
SELECT count(*) AS indexes FROM pg_indexes WHERE schemaname = 'pgmq' AND tablename = 'q_existing_q';
SELECT count(*) AS indexes FROM pg_indexes WHERE schemaname = 'pgmq' AND tablename = 'a_existing_q';
DROP TABLE rows_before;

-- test_create_existing_queue_repairs_missing_indexes
DROP INDEX pgmq.q_existing_q_vt_idx;
DROP INDEX pgmq.archived_at_idx_existing_q;
SELECT pgmq.create('existing_q');
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = 'pgmq' AND indexname IN ('q_existing_q_vt_idx', 'archived_at_idx_existing_q')
 ORDER BY indexname;

-- test_create_existing_queue_repairs_archive_index_for_mixed_case_name
-- the archive index name keeps the queue name as given, while the tables are lowercased
SELECT pgmq.create('ExistingMixedQ');
DROP INDEX pgmq."archived_at_idx_ExistingMixedQ";
SELECT pgmq.create('ExistingMixedQ');
SELECT to_regclass('pgmq."archived_at_idx_ExistingMixedQ"') IS NOT NULL AS archive_index_restored;
SELECT count(*) AS indexes FROM pg_indexes WHERE schemaname = 'pgmq' AND tablename = 'a_existingmixedq';

-- test_create_existing_queue_restores_pg_monitor_grants
REVOKE SELECT (vt, enqueued_at) ON pgmq.q_existing_q FROM pg_monitor;
REVOKE SELECT ON SEQUENCE pgmq.q_existing_q_msg_id_seq FROM pg_monitor;
SELECT has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'vt', 'SELECT') AS vt,
       has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'enqueued_at', 'SELECT') AS enqueued_at,
       has_sequence_privilege('pg_monitor', 'pgmq.q_existing_q_msg_id_seq', 'SELECT') AS seq;
SELECT pgmq.create('existing_q');
SELECT has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'vt', 'SELECT') AS vt,
       has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'enqueued_at', 'SELECT') AS enqueued_at,
       has_sequence_privilege('pg_monitor', 'pgmq.q_existing_q_msg_id_seq', 'SELECT') AS seq;

-- a single missing column grant is restored too
REVOKE SELECT (enqueued_at) ON pgmq.q_existing_q FROM pg_monitor;
SELECT pgmq.create('existing_q');
SELECT has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'enqueued_at', 'SELECT') AS enqueued_at;

-- the grants stay column-level: pg_monitor still can't read message payloads
SELECT has_column_privilege('pg_monitor', 'pgmq.q_existing_q', 'message', 'SELECT') AS message;

-- test_create_unlogged_existing_queue_writes_nothing
SELECT pgmq.create_unlogged('existing_unlogged_q');
CREATE TEMP TABLE rows_before AS
  SELECT * FROM queue_catalog_rows WHERE name LIKE '%existing_unlogged_q%';
SELECT pgmq.create_unlogged('existing_unlogged_q');
SELECT b.name, b.row_xmin = r.row_xmin AS unchanged
  FROM rows_before b JOIN queue_catalog_rows r USING (name)
 ORDER BY b.name;
DROP TABLE rows_before;

-- test_create_unlogged_existing_queue_repairs_indexes_and_grants
DROP INDEX pgmq.q_existing_unlogged_q_vt_idx;
DROP INDEX pgmq.archived_at_idx_existing_unlogged_q;
REVOKE SELECT (vt, enqueued_at) ON pgmq.q_existing_unlogged_q FROM pg_monitor;
REVOKE SELECT ON SEQUENCE pgmq.q_existing_unlogged_q_msg_id_seq FROM pg_monitor;
SELECT pgmq.create_unlogged('existing_unlogged_q');
SELECT indexname FROM pg_indexes
 WHERE schemaname = 'pgmq' AND indexname IN ('q_existing_unlogged_q_vt_idx', 'archived_at_idx_existing_unlogged_q')
 ORDER BY indexname;
SELECT has_column_privilege('pg_monitor', 'pgmq.q_existing_unlogged_q', 'vt', 'SELECT') AS vt,
       has_column_privilege('pg_monitor', 'pgmq.q_existing_unlogged_q', 'enqueued_at', 'SELECT') AS enqueued_at,
       has_sequence_privilege('pg_monitor', 'pgmq.q_existing_unlogged_q_msg_id_seq', 'SELECT') AS seq;

-- the queues keep working after all of the above
SELECT pgmq.send('existing_q', '{"ok": true}') > 0 AS sent;
SELECT message FROM pgmq.read('existing_q', 0, 1);
SELECT pgmq.send('existing_unlogged_q', '{"ok": true}') > 0 AS sent;
SELECT message FROM pgmq.read('existing_unlogged_q', 0, 1);

-- cleanup
SELECT pgmq.drop_queue('existing_q');
SELECT pgmq.drop_queue('ExistingMixedQ');
SELECT pgmq.drop_queue('existing_unlogged_q');
DROP VIEW queue_catalog_rows;
