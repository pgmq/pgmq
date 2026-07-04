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

    /// a database error
    #[cfg(feature = "sqlx")]
    #[error("database error {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// the error returned when attempting to use an invalid queue name
    #[error(transparent)]
    QueueNameError(#[from] QueueNameError),

    /// a general error for installation operations
    #[cfg(feature = "install-sql")]
    #[error("installation error: {0}")]
    InstallationError(String),
}
