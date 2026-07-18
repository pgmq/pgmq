use crate::queue::macros::{identity_macro, impl_queue};
use crate::queue::sql::{ARCHIVE, CREATE, DELETE, READ, SEND, SET_VT};
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
    let query = sqlx::query(READ);
    let rows = query
        .bind(*queue_name)
        .bind(visibility_timeout)
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
