//! Shared SQL query strings.

// Note: the `language=PostgreSQL` comment allows JetBrains IDEs to provide inline syntax
// highlighting for the string content. See: https://www.jetbrains.com/help/idea/using-language-injections.html#use-language-injection-comments

// language=PostgreSQL
pub(crate) const CREATE: &str = "SELECT pgmq.create(queue_name=>$1::text)";

// language=PostgreSQL
pub(crate) const CREATE_UNLOGGED: &str = "SELECT pgmq.create_unlogged(queue_name=>$1::text)";

// language=PostgreSQL
pub(crate) const CREATE_PARTITIONED: &str = "SELECT pgmq.create_partitioned(queue_name=>$1::text, partition_interval=>$2::text, retention_interval=>$3::text)";

// language=PostgreSQL
pub(crate) const CONVERT_ARCHIVE_PARTITIONED: &str = "SELECT pgmq.convert_archive_partitioned(table_name=>$1::text, partition_interval=>$2::text, retention_interval=>$3::text)";

// language=PostgreSQL
pub(crate) const SEND: &str = "SELECT * FROM pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int)";

// language=PostgreSQL
pub(crate) const SEND_BATCH: &str = "SELECT * from pgmq.send_batch(queue_name=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer)";

// language=PostgreSQL
pub(crate) const READ: &str = "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers FROM pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)";

// language=PostgreSQL
pub(crate) const POP: &str = r"SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers from pgmq.pop(queue_name=>$1::text, qty=>$2::integer)";

// language=PostgreSQL
pub(crate) const ARCHIVE: &str =
    "SELECT * from pgmq.archive(queue_name=>$1::text, msg_ids=>$2::bigint[])";

// language=PostgreSQL
pub(crate) const DELETE: &str =
    "SELECT * from pgmq.delete(queue_name=>$1::text, msg_ids=>$2::bigint[])";

// language=PostgreSQL
pub(crate) const SET_VT: &str = "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers from pgmq.set_vt(queue_name=>$1::text, msg_ids=>$2::bigint[], vt=>$3::integer)";

// language=PostgreSQL
pub(crate) const CREATE_FIFO_INDEX: &str = "SELECT pgmq.create_fifo_index(queue_name=>$1::text)";

// language=PostgreSQL
pub(crate) const CREATE_FIFO_INDEXES_ALL: &str = "SELECT pgmq.create_fifo_indexes_all()";

// language=PostgreSQL
pub(crate) const READ_GROUPED: &str = "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers FROM pgmq.read_grouped(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)";

// language=PostgreSQL
pub(crate) const READ_GROUPED_HEAD: &str = "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers FROM pgmq.read_grouped_head(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)";

// language=PostgreSQL
pub(crate) const READ_GROUPED_RR: &str = "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers FROM pgmq.read_grouped_rr(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)";

// language=PostgreSQL
pub(crate) const BIND_TOPIC: &str =
    "SELECT pgmq.bind_topic(pattern=>$1::text, queue_name=>$2::text)";

// language=PostgreSQL
pub(crate) const UNBIND_TOPIC: &str =
    "SELECT pgmq.unbind_topic(pattern=>$1::text, queue_name=>$2::text)";

// language=PostgreSQL
pub(crate) const LIST_TOPIC_BINDINGS: &str = "SELECT pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings(queue_name=>$1::text)";

// language=PostgreSQL
pub(crate) const LIST_TOPIC_BINDINGS_ALL: &str =
    "SELECT pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings()";

// language=PostgreSQL
pub(crate) const SEND_TOPIC: &str = "SELECT * from pgmq.send_topic(routing_key=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int)";

// language=PostgreSQL
pub(crate) const SEND_BATCH_TOPIC: &str = "SELECT queue_name, msg_id from pgmq.send_batch_topic(routing_key=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer)";

// language=PostgreSQL
pub(crate) const ENABLE_NOTIFY_INSERT: &str =
    "SELECT pgmq.enable_notify_insert(queue_name=>$1::text, throttle_interval_ms=>$2::integer)";

// language=PostgreSQL
pub(crate) const UPDATE_NOTIFY_INSERT: &str =
    "SELECT pgmq.update_notify_insert(queue_name=>$1::text, throttle_interval_ms=>$2::integer)";

// language=PostgreSQL
pub(crate) const DISABLE_NOTIFY_INSERT: &str =
    "SELECT pgmq.disable_notify_insert(queue_name=>$1::text)";

// language=PostgreSQL
pub(crate) const LIST_NOTIFY_INSERT_THROTTLES: &str = "SELECT queue_name, throttle_interval_ms, last_notified_at FROM pgmq.list_notify_insert_throttles()";

// language=PostgreSQL
pub(crate) const LIST_QUEUES: &str =
    "SELECT queue_name, is_partitioned, is_unlogged, created_at from pgmq.list_queues()";

// language=PostgreSQL
pub(crate) const QUEUE_METADATA: &str =
    "SELECT queue_name, is_partitioned, is_unlogged, created_at FROM pgmq.meta WHERE queue_name = $1::text";

// language=PostgreSQL
pub(crate) const ACQUIRE_QUEUE_LOCK: &str = "SELECT pgmq.acquire_queue_lock(queue_name=>$1::text)";
