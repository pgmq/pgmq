use crate::queue::macros::{identity_macro, impl_queue};
use crate::queue::sql::{
    ARCHIVE, CREATE, CREATE_FIFO_INDEX, CREATE_FIFO_INDEXES_ALL, DELETE, POP, READ, READ_GROUPED,
    READ_GROUPED_HEAD, READ_GROUPED_RR, SEND, SEND_BATCH, SET_VT,
};
use crate::types::{QueueName, VisibilityTimeoutOffset};
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

impl_queue!(
    &mut sqlx::Transaction<'_, Postgres>,
    transform_input_dereference_transaction
);
impl_queue!(&mut sqlx::PgConnection, identity_macro);
impl_queue!(&sqlx::PgPool, identity_macro);

async fn create<'c, C>(executor: C, queue_name: QueueName<'_>) -> Result<(), PgmqError>
where
    C: Executor<'c, Database = Postgres>,
{
    sqlx::query(CREATE)
        .bind(*queue_name)
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
