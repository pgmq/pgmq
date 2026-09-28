//! This implementation uses unnamed parameters when invoking the SQL function. This means the
//! parameter order needs to match the order in which they're defined in SQL.

use crate::metrics::{PgQueueMetrics, QueueMetricsFromSqlRow};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types::Text;

#[derive(Debug, Clone)]
pub struct Metrics<'a> {
    queue_name: &'a str,
}

impl QueryId for Metrics<'_> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> QueryFragment<Pg> for Metrics<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("SELECT (queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length, default_partition_length) FROM pgmq.metrics(");
        out.push_bind_param::<Text, _>(self.queue_name)?;
        out.push_sql(")");
        Ok(())
    }
}

impl<'a> Query for Metrics<'a> {
    type SqlType = PgQueueMetrics;
}

impl<'a, C> RunQueryDsl<C> for Metrics<'a> where C: diesel::connection::LoadConnection<Backend = Pg> {}

fn metrics_query(queue_name: &str) -> Metrics<'_> {
    Metrics { queue_name }
}

pub fn execute(conn: &mut PgConnection, queue: &str) {
    let _: QueueMetricsFromSqlRow = metrics_query(queue).get_result(conn).unwrap();
}

#[cfg(test)]
mod tests {
    use super::metrics_query;
    use diesel::debug_query;

    #[test]
    fn query() {
        assert_eq!(
            "SELECT (queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length, default_partition_length) FROM pgmq.metrics($1) -- binds: [\"queue\"]",
            debug_query(&metrics_query("queue")).to_string()
        );
    }

    #[test]
    fn query_cache_prepared() {
        use diesel::connection::statement_cache::QueryFragmentForCachedStatement;
        use diesel::pg::Pg;

        assert!(
            metrics_query("queue")
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be cached"
        );
    }
}
