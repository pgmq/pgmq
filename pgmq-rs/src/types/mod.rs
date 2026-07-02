pub mod queue_name;

use serde::Deserialize;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::FromRow;
use std::time::Duration;

pub const VT_DEFAULT: i32 = 30;
pub const READ_LIMIT_DEFAULT: i32 = 1;
pub const POLL_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);
pub const POLL_INTERVAL_DEFAULT: Duration = Duration::from_millis(250);

pub const QUEUE_PREFIX: &str = r#"q"#;
pub const ARCHIVE_PREFIX: &str = r#"a"#;
pub const PGMQ_SCHEMA: &str = "pgmq";

#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct PGMQueueMeta {
    pub queue_name: String,
    pub is_partitioned: bool,
    pub is_unlogged: bool,
    pub created_at: DateTime<Utc>,
}

/// Message struct received from the queue
///
/// It is an "envelope" for the message that is stored in the queue.
/// It contains both the message body but also metadata about the message.
#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct Message<T = serde_json::Value, H = serde_json::Value> {
    /// Unique identifier for the message.
    pub msg_id: i64,
    /// The number of times the message has been read. Increments on read.
    pub read_ct: i32,
    /// UTC timestamp that the message was sent to the queue.
    pub enqueued_at: DateTime<Utc>,
    /// UTC timestamp of the last time the message was fetched from the queue.
    pub last_read_at: Option<DateTime<Utc>>,
    /// "visibility time". The UTC timestamp at which the message will be available for reading again.
    pub vt: DateTime<Utc>,
    /// The message body.
    #[sqlx(json)]
    pub message: T,
    /// The message headers.
    #[sqlx(json(nullable))]
    pub headers: Option<H>,
}

/// A row returned by the `pgmq.send_batch_topic` SQL function(s).
#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct SendBatchTopicRow {
    pub queue_name: String,
    pub msg_id: i64,
}

/// A row returned by the `pgmq.list_topic_bindings` SQL function(s).
#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct ListTopicBindingsRow {
    pub pattern: String,
    pub queue_name: String,
    pub bound_at: DateTime<Utc>,
    pub compiled_regex: String,
}

/// A row returned by the `pgmq.list_notify_insert_throttles` SQL function.
#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct ListNotifyInsertThrottlesRow {
    pub queue_name: String,
    pub throttle_interval_ms: i32,
    pub last_notified_at: DateTime<Utc>,
}

/// Metrics for a queue. Returned for a single queue by `pgmq.metrics` and for all queues by
/// `pgmq.metrics_all`.
#[derive(Clone, Debug, Deserialize, FromRow)]
#[non_exhaustive]
pub struct QueueMetrics {
    pub queue_name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i32>,
    pub oldest_msg_age_sec: Option<i32>,
    pub total_messages: i64,
    pub scrape_time: DateTime<Utc>,
    pub queue_visible_length: i64,
}
