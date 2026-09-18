use chrono::{DateTime, Utc};
use diesel::QueryableByName;
use diesel::deserialize::FromSql;
use diesel::pg::{Pg, PgValue};
use diesel::sql_types::*;
use serde_derive::Deserialize;

pub mod query_fragment;
pub mod query_source;
pub mod sql_function;
pub mod sql_query;

#[derive(Clone, Debug, Deserialize, QueryableByName)]
#[non_exhaustive]
pub struct QueueMetricsQueryableByName {
    #[diesel(sql_type = Text)]
    pub queue_name: String,
    #[diesel(sql_type = BigInt)]
    pub queue_length: i64,
    #[diesel(sql_type = Nullable<Integer>)]
    pub newest_msg_age_sec: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    pub oldest_msg_age_sec: Option<i32>,
    #[diesel(sql_type = BigInt)]
    pub total_messages: i64,
    #[diesel(sql_type = Timestamptz)]
    pub scrape_time: DateTime<Utc>,
    #[diesel(sql_type = BigInt)]
    pub queue_visible_length: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub default_partition_length: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, diesel::FromSqlRow)]
#[non_exhaustive]
pub struct QueueMetricsFromSqlRow {
    pub queue_name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i32>,
    pub oldest_msg_age_sec: Option<i32>,
    pub total_messages: i64,
    pub scrape_time: DateTime<Utc>,
    pub queue_visible_length: i64,
    pub default_partition_length: Option<i64>,
}

#[derive(SqlType)]
#[diesel(postgres_type(name = "metrics_result", schema = "pgmq"))]
pub struct PgQueueMetrics;

// Todo: statically generate this tuple from the struct
type PgQueueMetricsTuple = (
    // queue_name
    Text,
    // queue_length
    BigInt,
    // newest_msg_age_sec
    Nullable<Integer>,
    // oldest_msg_age_sec
    Nullable<Integer>,
    // total_messages
    BigInt,
    // scrape_time
    Timestamptz,
    // queue_visible_length
    BigInt,
    // default_partition_length
    Nullable<BigInt>,
);

impl FromSql<PgQueueMetrics, Pg> for QueueMetricsFromSqlRow {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let (
            queue_name,
            queue_length,
            newest_msg_age_sec,
            oldest_msg_age_sec,
            total_messages,
            scrape_time,
            queue_visible_length,
            default_partition_length,
        ) = FromSql::<Record<PgQueueMetricsTuple>, Pg>::from_sql(bytes)?;

        Ok(Self {
            queue_name,
            queue_length,
            newest_msg_age_sec,
            oldest_msg_age_sec,
            total_messages,
            scrape_time,
            queue_visible_length,
            default_partition_length,
        })
    }
}
