use chrono::{DateTime, Utc};
use diesel::deserialize::{FromSql, FromSqlRow};
use diesel::pg::{Pg, PgValue};
use diesel::row::Row;
use diesel::sql_types::*;
use diesel::{Queryable, QueryableByName, Selectable};
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

#[derive(Clone, Debug, Deserialize)]
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

// Todo: statically generate this tuple from the struct
type QueueMetricsTuple = (
    // queue_name
    String,
    // queue_length
    i64,
    // newest_msg_age_sec
    Option<i32>,
    // oldest_msg_age_sec
    Option<i32>,
    // total_messages
    i64,
    // scrape_time
    DateTime<Utc>,
    // queue_visible_length
    i64,
    // default_partition_length
    Option<i64>,
);

impl From<QueueMetricsTuple> for QueueMetricsFromSqlRow {
    fn from(value: QueueMetricsTuple) -> Self {
        Self {
            queue_name: value.0,
            queue_length: value.1,
            newest_msg_age_sec: value.2,
            oldest_msg_age_sec: value.3,
            total_messages: value.4,
            scrape_time: value.5,
            queue_visible_length: value.6,
            default_partition_length: value.7,
        }
    }
}

#[derive(SqlType)]
#[diesel(postgres_type(name = "metrics_result", schema = "pgmq"))]
pub struct PgQueueMetrics;

// Not needed for the `QuerySource` impl
impl FromSql<PgQueueMetrics, Pg> for QueueMetricsFromSqlRow {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        let value: QueueMetricsTuple =
            FromSql::<Record<utility_types::SqlType>, Pg>::from_sql(bytes)?;

        Ok(value.into())
    }
}

// Not needed for the `QuerySource` impl
impl FromSqlRow<PgQueueMetrics, Pg> for QueueMetricsFromSqlRow {
    fn build_from_row<'a>(row: &impl Row<'a, Pg>) -> diesel::deserialize::Result<Self> {
        let value: QueueMetricsTuple =
            FromSqlRow::<Record<utility_types::SqlType>, Pg>::build_from_row(row)?;
        Ok(value.into())
    }
}

// Only needed for the `QuerySource` impl
impl Queryable<utility_types::SqlType, Pg> for QueueMetricsFromSqlRow {
    type Row = QueueMetricsTuple;

    fn build(row: Self::Row) -> diesel::deserialize::Result<Self> {
        Ok(row.into())
    }
}

// Only needed for the `QuerySource` impl
impl Selectable<Pg> for QueueMetricsFromSqlRow {
    type SelectExpression = utility_types::AllColumns;

    fn construct_selection() -> Self::SelectExpression {
        utility_types::all_columns
    }
}

// Only needed for the `QuerySource` impl
pub mod utility_types {
    use diesel::expression::ValidGrouping;
    use diesel::pg::Pg;
    use diesel::query_builder::{AstPass, QueryFragment};
    use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamptz};
    use diesel::{Expression, QueryId, QueryResult};

    #[derive(QueryId)]
    pub struct queue_name;
    #[derive(QueryId)]
    pub struct queue_length;
    #[derive(QueryId)]
    pub struct newest_msg_age_sec;
    #[derive(QueryId)]
    pub struct oldest_msg_age_sec;
    #[derive(QueryId)]
    pub struct total_messages;
    #[derive(QueryId)]
    pub struct scrape_time;
    #[derive(QueryId)]
    pub struct queue_visible_length;
    /// Estimated number of messages sitting in a partitioned queue's default partition. `None` for
    /// non-partitioned queues; a non-zero value signals that pg_partman maintenance is failing.
    #[derive(QueryId)]
    pub struct default_partition_length;

    impl Expression for queue_name {
        type SqlType = Text;
    }
    impl Expression for queue_length {
        type SqlType = BigInt;
    }
    impl Expression for newest_msg_age_sec {
        type SqlType = Nullable<Integer>;
    }
    impl Expression for oldest_msg_age_sec {
        type SqlType = Nullable<Integer>;
    }
    impl Expression for total_messages {
        type SqlType = BigInt;
    }
    impl Expression for scrape_time {
        type SqlType = Timestamptz;
    }
    impl Expression for queue_visible_length {
        type SqlType = BigInt;
    }
    impl Expression for default_partition_length {
        type SqlType = Nullable<BigInt>;
    }

    impl QueryFragment<Pg> for queue_name {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            // Todo: Writing the type to the DB vs reading?
            pass.push_sql("\"queue_name\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for queue_length {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"queue_length\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for newest_msg_age_sec {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"newest_msg_age_sec\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for oldest_msg_age_sec {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"oldest_msg_age_sec\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for total_messages {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"total_messages\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for scrape_time {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"scrape_time\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for queue_visible_length {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"queue_visible_length\"");
            Ok(())
        }
    }
    impl QueryFragment<Pg> for default_partition_length {
        fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
            pass.push_sql("\"default_partition_length\"");
            Ok(())
        }
    }

    impl ValidGrouping<()> for queue_name {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for queue_length {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for newest_msg_age_sec {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for oldest_msg_age_sec {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for total_messages {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for scrape_time {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for queue_visible_length {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }
    impl ValidGrouping<()> for default_partition_length {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }

    pub type AllColumns = (
        queue_name,
        queue_length,
        newest_msg_age_sec,
        oldest_msg_age_sec,
        total_messages,
        scrape_time,
        queue_visible_length,
        default_partition_length,
    );
    pub const all_columns: AllColumns = (
        queue_name,
        queue_length,
        newest_msg_age_sec,
        oldest_msg_age_sec,
        total_messages,
        scrape_time,
        queue_visible_length,
        default_partition_length,
    );
    pub type SqlType = <AllColumns as Expression>::SqlType;
}
