#![cfg(feature = "sqlx")]

use crate::errors::PgmqError;
use log::LevelFilter;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
pub(crate) async fn init_lock<'c>(txn: &mut Transaction<'c, Postgres>) -> Result<(), PgmqError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1);")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut **txn)
        .await?;
    Ok(())
}
