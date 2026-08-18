//! Custom errors types for PGMQ
use crate::types::queue_name::QueueNameError;
use thiserror::Error;
use url::ParseError;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PgmqError {
    /// a json parsing error
    #[error("json parsing error {0}")]
    JsonParsingError(#[from] serde_json::error::Error),

    /// a url parsing error
    #[error("url parsing error {0}")]
    UrlParsingError(#[from] ParseError),

    /// a database error from the `sqlx` crate
    #[cfg(feature = "sqlx")]
    #[error("database error {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// a database error from the `tokio-postgres` crate
    #[cfg(any(feature = "rust-postgres", feature = "tokio-postgres"))]
    #[error("database error {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    /// a database error from the `diesel` crate
    #[cfg(feature = "diesel")]
    #[error("database error {0}")]
    DieselError(#[from] diesel::result::Error),

    /// a database error from a `diesel` connection pool
    #[cfg(feature = "diesel-sync-pool")]
    #[error("database pool error {0}")]
    DieselPoolError(#[from] r2d2::Error),

    /// a database error from a `diesel-async` connection pool
    #[cfg(feature = "diesel-async-pool")]
    #[error("database pool error {0}")]
    DieselAsyncPoolError(#[from] bb8::RunError<diesel_async::pooled_connection::PoolError>),

    /// the error returned when attempting to use an invalid queue name
    #[error(transparent)]
    QueueNameError(#[from] QueueNameError),

    /// a general error for installation operations
    #[cfg(feature = "install-sql")]
    #[error("installation error: {0}")]
    InstallationError(String),
}
