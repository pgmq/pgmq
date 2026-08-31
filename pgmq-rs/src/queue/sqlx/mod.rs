use crate::queue::macros::{identity_macro, impl_queue, impl_queue_transaction};
use crate::queue::sql::{
    ACQUIRE_QUEUE_LOCK, ARCHIVE, BIND_TOPIC, CONVERT_ARCHIVE_PARTITIONED, CREATE,
    CREATE_FIFO_INDEX, CREATE_FIFO_INDEXES_ALL, CREATE_PARTITIONED, CREATE_UNLOGGED, DELETE,
    DISABLE_NOTIFY_INSERT, DROP_QUEUE, ENABLE_NOTIFY_INSERT, LIST_NOTIFY_INSERT_THROTTLES,
    LIST_QUEUES, LIST_TOPIC_BINDINGS, LIST_TOPIC_BINDINGS_ALL, POP, PURGE_QUEUE, QUEUE_METADATA,
    READ, READ_GROUPED, READ_GROUPED_HEAD, READ_GROUPED_RR, SEND, SEND_BATCH, SEND_BATCH_TOPIC,
    SEND_TOPIC, SET_VT, UNBIND_TOPIC, UPDATE_NOTIFY_INSERT,
};
use crate::types::{
    InsertNotificationThrottleInterval, ListNotifyInsertThrottlesRow, ListTopicBindingsRow,
    PGMQueueMeta, QueueName, SendBatchTopicRow, VisibilityTimeoutOffset,
};
use crate::{Message, PgmqError};
use sqlx::{Executor, Postgres};
use util::handle_read_batch_result;

pub(crate) mod util;

/// Transforms a `sqlx::Transaction<'_, Postgres>` identifier by dereferencing it so that it can be
/// used as an [`Executor`].
macro_rules! transform_input_dereference_transaction {
    ($input:ident) => {
        &mut **$input
    };
}

impl_queue_transaction!(
    &mut sqlx::Transaction<'_, Postgres>,
    transform_input_dereference_transaction
);
impl_queue!(&mut sqlx::PgConnection, identity_macro);
impl_queue!(&sqlx::PgPool, identity_macro);

pub(crate) async fn create<'c, C>(executor: C, queue_name: QueueName<'_>) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE)
        .bind(*queue_name)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn create_unlogged<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE_UNLOGGED)
        .bind(*queue_name)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn create_partitioned<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    partition_interval: &str,
    retention_interval: &str,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE_PARTITIONED)
        .bind(*queue_name)
        .bind(partition_interval)
        .bind(retention_interval)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn convert_archive_partitioned<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    partition_interval: &str,
    retention_interval: &str,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CONVERT_ARCHIVE_PARTITIONED)
        .bind(*queue_name)
        .bind(partition_interval)
        .bind(retention_interval)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn send<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    message: serde_json::Value,
    headers: serde_json::Value,
    delay: VisibilityTimeoutOffset,
) -> Result<i64, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let msg_id: i64 = sqlx::query_scalar(SEND)
        .bind(*queue_name)
        .bind(message)
        .bind(headers)
        .bind(delay)
        .fetch_one(executor)
        .await?;
    Ok(msg_id)
}

pub(crate) async fn send_batch<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    messages: Vec<serde_json::Value>,
    headers: Option<Vec<serde_json::Value>>,
    delay: VisibilityTimeoutOffset,
) -> Result<Vec<i64>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let sent: Vec<i64> = sqlx::query_scalar(SEND_BATCH)
        .bind(*queue_name)
        .bind(messages)
        .bind(headers)
        .bind(delay)
        .fetch_all(executor)
        .await?;
    Ok(sent)
}

async fn read<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    read_common(executor, READ, queue_name, visibility_timeout, quantity).await
}

pub(crate) async fn pop<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    let query = sqlx::query(POP);
    let rows = query
        .bind(*queue_name)
        .bind(quantity)
        .fetch_all(executor)
        .await?;

    handle_read_batch_result(rows)
}

pub(crate) async fn archive<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    msg_ids: &[i64],
) -> Result<Vec<i64>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let archived: Vec<i64> = sqlx::query_scalar(ARCHIVE)
        .bind(*queue_name)
        .bind(msg_ids)
        .fetch_all(executor)
        .await?;
    Ok(archived)
}

pub(crate) async fn delete<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    msg_ids: &[i64],
) -> Result<Vec<i64>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let deleted: Vec<i64> = sqlx::query_scalar(DELETE)
        .bind(*queue_name)
        .bind(msg_ids)
        .fetch_all(executor)
        .await?;
    Ok(deleted)
}

pub(crate) async fn set_vt<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    msg_ids: &[i64],
    visibility_timeout: VisibilityTimeoutOffset,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    let rows = sqlx::query(SET_VT)
        .bind(*queue_name)
        .bind(msg_ids)
        .bind(visibility_timeout)
        .fetch_all(executor)
        .await?;

    handle_read_batch_result(rows)
}

pub(crate) async fn create_fifo_index<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE_FIFO_INDEX)
        .bind(*queue_name)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn create_fifo_indexes_all<'c, C>(executor: C) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE_FIFO_INDEXES_ALL)
        .execute(executor)
        .await?;

    Ok(())
}

pub(crate) async fn read_grouped<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    read_common(
        executor,
        READ_GROUPED,
        queue_name,
        visibility_timeout,
        quantity,
    )
    .await
}

pub(crate) async fn read_grouped_head<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    read_common(
        executor,
        READ_GROUPED_HEAD,
        queue_name,
        visibility_timeout,
        quantity,
    )
    .await
}

pub(crate) async fn read_grouped_rr<'c, C, T, H>(
    executor: C,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    read_common(
        executor,
        READ_GROUPED_RR,
        queue_name,
        visibility_timeout,
        quantity,
    )
    .await
}

async fn read_common<'c, C, T, H>(
    executor: C,
    query: &'static str,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T, H>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    let query = sqlx::query(query);
    let rows = query
        .bind(*queue_name)
        .bind(visibility_timeout)
        .bind(quantity)
        .fetch_all(executor)
        .await?;

    handle_read_batch_result(rows)
}

pub(crate) async fn bind_topic<'c, C>(
    executor: C,
    pattern: &str,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(BIND_TOPIC)
        .bind(pattern)
        .bind(*queue_name)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn unbind_topic<'c, C>(
    executor: C,
    pattern: &str,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(UNBIND_TOPIC)
        .bind(pattern)
        .bind(*queue_name)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn list_topic_bindings<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<Vec<ListTopicBindingsRow>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let query = sqlx::query_as(LIST_TOPIC_BINDINGS).bind(*queue_name);
    list_topic_bindings_common(executor, query).await
}

pub(crate) async fn list_topic_bindings_all<'c, C>(
    executor: C,
) -> Result<Vec<ListTopicBindingsRow>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let query = sqlx::query_as(LIST_TOPIC_BINDINGS_ALL);
    list_topic_bindings_common(executor, query).await
}

async fn list_topic_bindings_common<'q, 'c, C>(
    executor: C,
    query: sqlx::query::QueryAs<
        'q,
        Postgres,
        ListTopicBindingsRow,
        <Postgres as sqlx::Database>::Arguments,
    >,
) -> Result<Vec<ListTopicBindingsRow>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let rows = query.fetch_all(executor).await?;
    Ok(rows)
}

pub(crate) async fn send_topic<'c, C>(
    executor: C,
    routing_key: &str,
    message: serde_json::Value,
    headers: serde_json::Value,
    delay: VisibilityTimeoutOffset,
) -> Result<i32, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let matched_queue_count: i32 = sqlx::query_scalar(SEND_TOPIC)
        .bind(routing_key)
        .bind(message)
        .bind(headers)
        .bind(delay)
        .fetch_one(executor)
        .await?;
    Ok(matched_queue_count)
}

pub(crate) async fn send_batch_topic<'c, C>(
    executor: C,
    routing_key: &str,
    messages: Vec<serde_json::Value>,
    headers: Option<Vec<serde_json::Value>>,
    delay: VisibilityTimeoutOffset,
) -> Result<Vec<SendBatchTopicRow>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let sent = sqlx::query_as(SEND_BATCH_TOPIC)
        .bind(routing_key)
        .bind(messages)
        .bind(headers)
        .bind(delay)
        .fetch_all(executor)
        .await?;
    Ok(sent)
}

pub(crate) async fn enable_notify_insert<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    throttle_interval: InsertNotificationThrottleInterval,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(ENABLE_NOTIFY_INSERT)
        .bind(*queue_name)
        .bind(throttle_interval)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn update_notify_insert<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
    throttle_interval: InsertNotificationThrottleInterval,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(UPDATE_NOTIFY_INSERT)
        .bind(*queue_name)
        .bind(throttle_interval)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn disable_notify_insert<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(DISABLE_NOTIFY_INSERT)
        .bind(*queue_name)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn list_notify_insert_throttles<'c, C>(
    executor: C,
) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let rows = sqlx::query_as(LIST_NOTIFY_INSERT_THROTTLES)
        .fetch_all(executor)
        .await?;
    Ok(rows)
}

pub(crate) async fn list_queues<'c, C>(executor: C) -> Result<Vec<PGMQueueMeta>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let rows = sqlx::query_as(LIST_QUEUES).fetch_all(executor).await?;
    Ok(rows)
}

async fn queue_metadata<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<Option<PGMQueueMeta>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let metadata = sqlx::query_as(QUEUE_METADATA)
        .bind(*queue_name)
        .fetch_optional(executor)
        .await?;

    Ok(metadata)
}

pub(crate) async fn acquire_queue_lock<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(ACQUIRE_QUEUE_LOCK)
        .bind(*queue_name)
        .execute(executor)
        .await?;
    Ok(())
}

pub(crate) async fn purge_queue<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<i64, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    let num_deleted = sqlx::query_scalar(PURGE_QUEUE)
        .bind(*queue_name)
        .fetch_one(executor)
        .await?;
    Ok(num_deleted)
}

pub(crate) async fn drop_queue<'c, C>(
    executor: C,
    queue_name: QueueName<'_>,
) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(DROP_QUEUE)
        .bind(*queue_name)
        .execute(executor)
        .await?;
    Ok(())
}
