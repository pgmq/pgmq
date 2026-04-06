use std::fmt::Display;

use crate::{errors::PgmqError, types::Message};

use log::LevelFilter;
use serde::Deserialize;
use sqlx::error::Error;
use sqlx::postgres::PgRow;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Acquire, FromRow, Row};
use sqlx::{ConnectOptions, Transaction};
use sqlx::{Pool, Postgres};
use url::{ParseError, Url};

// Configure connection options
pub fn conn_options(url: &str) -> Result<PgConnectOptions, ParseError> {
    // Parse url
    let parsed = Url::parse(url)?;
    let options = PgConnectOptions::new()
        .host(parsed.host_str().ok_or(ParseError::EmptyHost)?)
        .port(parsed.port().ok_or(ParseError::InvalidPort)?)
        .username(parsed.username())
        .password(parsed.password().ok_or(ParseError::IdnaError)?)
        .database(parsed.path().trim_start_matches('/'))
        .log_statements(LevelFilter::Debug);
    Ok(options)
}

/// Connect to the database
pub async fn connect(url: &str, max_connections: u32) -> Result<Pool<Postgres>, PgmqError> {
    let options = conn_options(url)?;
    let pgp = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(10))
        .max_connections(max_connections)
        .connect_with(options)
        .await?;
    Ok(pgp)
}

// Executes a query and returns a single row
// If the query returns no rows, None is returned
// This function is intended for internal use.
pub async fn fetch_one_message<T: for<'de> Deserialize<'de>>(
    query: &str,
    connection: &Pool<Postgres>,
) -> Result<Option<Message<T>>, PgmqError> {
    // explore: .fetch_optional()
    let row = sqlx::query(query)
        .fetch_one(connection)
        .await
        .and_then(|row| Message::<T>::from_row(&row));
    match row {
        Ok(row) => Ok(Some(row)),
        Err(sqlx::error::Error::RowNotFound) => Ok(None),
        Err(e) => Err(e)?,
    }
}

/// A string that is known to be formed of only ASCII alphanumeric or an underscore;
#[derive(Clone, Copy)]
pub struct CheckedName<'a>(&'a str);

impl<'a> CheckedName<'a> {
    /// Accepts `input` as a CheckedName if it is a valid queue identifier
    pub fn new(input: &'a str) -> Result<Self, PgmqError> {
        check_input(input)?;

        Ok(Self(input))
    }
}

impl AsRef<str> for CheckedName<'_> {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Display for CheckedName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// panics if input is invalid. otherwise does nothing.
pub fn check_input(input: &str) -> Result<(), PgmqError> {
    // Docs:
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

    // Default value of `NAMEDATALEN`, set in `src/include/pg_config_manual.h`
    const NAMEDATALEN: usize = 64;

    // The maximum length of an identifier.
    // Longer names can be used in commands, but they'll be truncated
    const MAX_IDENTIFIER_LEN: usize = NAMEDATALEN - 1;
    const BIGGEST_CONCAT: &str = "archived_at_idx_";

    // The max length of the name of a PGMQ queue, considering that the biggest
    // postgres identifier created by PGMQ is the index on archived_at
    const MAX_PGMQ_QUEUE_LEN: usize = MAX_IDENTIFIER_LEN - BIGGEST_CONCAT.len();

    let is_short_enough = input.len() <= MAX_PGMQ_QUEUE_LEN;
    let has_valid_characters = input
        .as_bytes()
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || c == b'_');
    let valid = is_short_enough && has_valid_characters;
    match valid {
        true => Ok(()),
        false => Err(PgmqError::InvalidQueueName {
            name: input.to_owned(),
        }),
    }
}

#[cfg(feature = "install-sql-github")]
#[deprecated(
    note = "Use pgmq::install::install_sql_from_github or pgmq::install::install_sql_from_embedded instead.",
    since = "0.33.0"
)]
pub async fn install_pgmq(
    pool: &Pool<Postgres>,
    version: Option<&String>,
) -> Result<(), PgmqError> {
    // Execute the SQL file
    log::info!("Executing PGMQ installation SQL...");

    crate::install::install_sql_from_github(pool, version.map(|v| v.as_str())).await?;

    log::info!("PGMQ installation completed successfully!");
    Ok(())
}

/// Advisory lock key used to ensure only one transaction can run the `pgmq` installation process
/// at once. Select a random large negative `bigint` value to minimize the chances of conflicting
/// with another advisory lock used by the actual application.
const ADVISORY_LOCK_KEY: i64 = -9223372036854775808 + 4149;

/// Acquire an advisory lock to be sure that only one transaction can run the pgmq SQL
/// installation/upgrade process at once. Without this, it's possible for multiple transactions
/// to attempt to perform the `pgmq` SQL installation/upgrade process at the same time, and they
/// may conflict when creating the `pgmq` schema and/or `pgmq.__pgmq_migrations` table. This is
/// the case even with `IF NOT EXISTS` in the SQL query.
pub(crate) async fn init_lock<'c>(tx: &mut Transaction<'c, Postgres>) -> Result<(), PgmqError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1);")
        .bind(ADVISORY_LOCK_KEY)
        .execute(tx.acquire().await?)
        .await?;
    Ok(())
}
