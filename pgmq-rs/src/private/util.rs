/// Helper method to convert [`PgRow`] to [`Message`] for the `read*`/`read_batch*` methods.
#[cfg(feature = "sqlx")]
pub fn handle_read_batch_result<T: for<'de> serde::Deserialize<'de>>(
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<crate::types::Message<T>>, crate::errors::PgmqError> {
    use sqlx::FromRow;
    let messages = rows
        .into_iter()
        .map(|row| crate::types::Message::<T>::from_row(&row))
        .collect::<Result<Vec<crate::types::Message<T>>, _>>()?;
    Ok(messages)
}
