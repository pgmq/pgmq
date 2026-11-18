# create() / create_unlogged() on a queue that already exists must not wait for, or block,
# transactions that use the queue (https://github.com/pgmq/pgmq/issues/601).
# The creator runs with a short lock_timeout, so if create() took a conflicting lock it would
# fail with "canceling statement due to lock timeout" instead of hanging the test.

setup {
  CREATE EXTENSION IF NOT EXISTS pg_partman;
  CREATE EXTENSION IF NOT EXISTS pgmq;
  SELECT pgmq.create('create_lock_queue');
  SELECT pgmq.create_unlogged('create_lock_unlogged_queue');
  SELECT pgmq.send('create_lock_queue', jsonb_build_object());
}

teardown {
  SELECT pgmq.drop_queue('create_lock_queue');
  SELECT pgmq.drop_queue('create_lock_unlogged_queue');
}

session writer
step wbegin    { BEGIN; }
step wsend     { SELECT pgmq.send('create_lock_queue', jsonb_build_object()) > 0 AS sent; }
step wsendu    { SELECT pgmq.send('create_lock_unlogged_queue', jsonb_build_object()) > 0 AS sent; }
step wread     { SELECT count(*) AS read FROM pgmq.read('create_lock_queue', 30, 1); }
step wdelete   { SELECT pgmq.delete('create_lock_queue', (SELECT min(msg_id) FROM pgmq.q_create_lock_queue)) AS deleted; }
step warchive  { SELECT pgmq.archive('create_lock_queue', (SELECT min(msg_id) FROM pgmq.q_create_lock_queue)) AS archived; }
step wcommit   { COMMIT; }

session creator
setup          { SET lock_timeout = '1s'; }
step cbegin    { BEGIN; }
step ccreate   { SELECT pgmq.create('create_lock_queue'); }
step ccreateu  { SELECT pgmq.create_unlogged('create_lock_unlogged_queue'); }
step ccommit   { COMMIT; }

# create() while a producer, a consumer or an archiver is inside its transaction
permutation wbegin wsend ccreate wcommit
permutation wbegin wread ccreate wcommit
permutation wbegin wdelete ccreate wcommit
permutation wbegin warchive ccreate wcommit
permutation wbegin wsendu ccreateu wcommit

# a no-op create() inside an open transaction must not hold up producers either
permutation cbegin ccreate wsend ccommit
permutation cbegin ccreateu wsendu ccommit
