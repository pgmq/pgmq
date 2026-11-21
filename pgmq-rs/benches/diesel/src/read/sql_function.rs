//! This implementation uses the standard approach to implementing and invoking
//! a SQL function. This approach is not resilient to changes in the SQL return type (new fields
//! or updates in the field order).

use crate::read::{MessageFromSqlRow, PgMessage};
use diesel::RunQueryDsl;
use diesel::dsl::select;
use diesel::sql_types::*;
use diesel::{PgConnection, declare_sql_function};

#[declare_sql_function]
extern "SQL" {
    #[sql_name = "pgmq.read"]
    fn pgmq_read(queue_name: Text, vt: Integer, qty: Integer) -> PgMessage;
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_query(queue_name: &str, visibility_timeout: i32, quantity: i32) -> _ {
    select(pgmq_read(queue_name, visibility_timeout, quantity))
}

pub fn execute(conn: &mut PgConnection, queue: &str, quantity: i32) {
    let _: Vec<MessageFromSqlRow> = read_query(queue, 0, quantity).get_results(conn).unwrap();
}

#[cfg(test)]
mod tests {
    use super::read_query;
    use diesel::debug_query;
    use diesel::pg::Pg;

    #[test]
    fn query() {
        assert_eq!(
            "SELECT pgmq.read($1, $2, $3) -- binds: [\"queue\", 0, 1]",
            debug_query::<Pg, _>(&read_query("queue", 0, 1)).to_string()
        );
    }

    #[test]
    fn query_cache_prepared() {
        use diesel::connection::statement_cache::QueryFragmentForCachedStatement;

        assert!(
            read_query("queue", 0, 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be cached"
        );
    }
}
