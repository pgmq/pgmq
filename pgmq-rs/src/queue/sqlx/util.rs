use crate::{Message, PgmqError};
use sqlx::postgres::PgRow;

/// Helper method to convert [`PgRow`] to [`Message`] for `read*`/`read_batch*` PGMQ methods.
pub fn handle_read_batch_result<T, H>(rows: Vec<PgRow>) -> Result<Vec<Message<T, H>>, PgmqError>
where
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    use sqlx::FromRow;
    let messages = rows
        .into_iter()
        .map(|row| Message::<T, H>::from_row(&row))
        .collect::<Result<Vec<Message<T, H>>, _>>()?;
    Ok(messages)
}
