use crate::private::util::handle_read_batch_result;
use crate::queue::macros::{identity_macro, impl_queue};
use crate::types::QueueName;
use crate::types::VisibilityTimeoutOffset;
use crate::{Message, PgmqError};
use sqlx::{Executor, Postgres};

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
    sqlx::query("SELECT pgmq.create(queue_name=>$1::text);")
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
    let msg_id: i64 = sqlx::query_scalar(
        "SELECT * from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int);",
    )
    .bind(*queue_name)
    .bind(message)
    .bind(headers)
    .bind(delay)
    .fetch_one(executor)
    .await?;
    Ok(msg_id)
}

async fn read<'c, C, T>(
    executor: C,
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> Result<Vec<Message<T>>, PgmqError>
where
    C: Executor<'c, Database = Postgres>,
    T: Send + for<'de> serde::Deserialize<'de>,
{
    let query = sqlx::query(
        r#"SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers from pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)"#,
    );
    let rows = query
        .bind(*queue_name)
        .bind(visibility_timeout)
        .bind(quantity)
        .fetch_all(executor)
        .await?;

    handle_read_batch_result(rows)
}
