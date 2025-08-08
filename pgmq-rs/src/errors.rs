//! Custom errors types for PGMQ
use thiserror::Error;
use url::ParseError;

#[derive(Error, Debug)]
pub enum PgmqError {
    /// a json parsing error
    #[error("json parsing error {0}")]
    JsonParsingError(#[from] serde_json::error::Error),

    /// a url parsing error
    #[error("url parsing error {0}")]
    UrlParsingError(#[from] ParseError),

    /// a database error
    #[error("database error {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// a queue name error
    /// queue names must be alphanumeric and start with a letter
    #[error("invalid queue name: '{name}'")]
    InvalidQueueName { name: String },

    /// a reqwest error (only when the `cli` feature is enabled)
    #[cfg(feature = "cli")]
    #[error("http request error {0}")]
    HttpError(#[from] reqwest::Error),

    /// a general error for installation operations
    #[error("installation error: {0}")]
    InstallationError(String),
}

impl From<Box<dyn std::error::Error>> for PgmqError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        PgmqError::InstallationError(err.to_string())
    }
}

impl From<String> for PgmqError {
    fn from(err: String) -> Self {
        PgmqError::InstallationError(err)
    }
}
