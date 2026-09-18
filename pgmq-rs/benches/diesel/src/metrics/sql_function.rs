//! This implementation uses the standard approach to implementing and invoking
//! a SQL function. This approach is not resilient to changes in the SQL return type (new fields
//! or updates in the field order).

use crate::metrics::PgQueueMetrics;
use crate::metrics::QueueMetricsFromSqlRow;
use diesel::RunQueryDsl;
use diesel::dsl::select;
use diesel::sql_types::*;
use diesel::{PgConnection, declare_sql_function};

#[declare_sql_function]
extern "SQL" {
    #[sql_name = "pgmq.metrics"]
    fn pgmq_metrics(queue_name: Text) -> PgQueueMetrics;
}

#[diesel::dsl::auto_type(no_type_alias)]
fn metrics_query(queue_name: &str) -> _ {
    select(pgmq_metrics(queue_name))
}

pub fn execute(conn: &mut PgConnection, queue: &str) {
    let _: QueueMetricsFromSqlRow = metrics_query(queue).get_result(conn).unwrap();
}

#[cfg(test)]
mod tests {
    use super::metrics_query;
    use diesel::debug_query;
    use diesel::pg::Pg;

    #[test]
    fn query() {
        assert_eq!(
            "SELECT pgmq.metrics($1) -- binds: [\"queue\"]",
            debug_query::<Pg, _>(&metrics_query("queue")).to_string()
        );
    }

    #[test]
    fn query_cache_prepared() {
        use diesel::connection::statement_cache::QueryFragmentForCachedStatement;

        assert!(
            metrics_query("queue")
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be cached"
        );
    }
}
