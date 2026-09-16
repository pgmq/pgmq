//! This implementation uses `sql_query` to invoke the SQL function. This is the simplest approach
//! and most closely matches the sqlx/rust-postgres implementations (we can even share the same
//! SQL string constants). However, this approach does not use the prepared statement cache, which
//! impacts its performance.

use crate::metrics::QueueMetricsQueryableByName;
use diesel::RunQueryDsl;
use diesel::query_builder::SqlQuery;
use diesel::sql_types::Text;
use diesel::{PgConnection, sql_query};

fn metrics_query() -> SqlQuery {
    sql_query(
        "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length, default_partition_length FROM pgmq.metrics(queue_name=>$1::text)",
    )
}

pub fn execute(conn: &mut PgConnection, queue: &str) {
    let _: QueueMetricsQueryableByName = metrics_query()
        // Todo: It would be better if the `bind` was performed inside `metrics_query`, but I'm not
        //  sure what return type to use in that case
        .bind::<Text, _>(queue)
        .get_result(conn)
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::metrics_query;
    use diesel::debug_query;
    use diesel::pg::Pg;
    use diesel::sql_types::Text;

    #[test]
    fn query() {
        assert_eq!(
            "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length, default_partition_length FROM pgmq.metrics(queue_name=>$1::text) -- binds: [\"queue\"]",
            debug_query::<Pg, _>(&metrics_query().bind::<Text, _>("queue")).to_string()
        );
    }
}
