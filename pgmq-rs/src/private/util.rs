/// Helper method to convert [`PgRow`] to [`Message`] for the `read*`/`read_batch*` methods.
#[cfg(feature = "sqlx")]
pub fn handle_read_batch_result<T, H>(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<crate::types::Message<T, H>>, crate::errors::PgmqError>
where
    T: for<'de> serde::Deserialize<'de>,
    H: for<'de> serde::Deserialize<'de>,
{
    use sqlx::FromRow;
    let messages = rows
        .into_iter()
        .map(|row| crate::types::Message::<T, H>::from_row(&row))
        .collect::<Result<Vec<crate::types::Message<T, H>>, _>>()?;
    Ok(messages)
}
