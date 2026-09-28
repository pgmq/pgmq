//! This implementation returns the results as a tuple. The resulting type needs to implement
//! [`diesel::FromSqlRow`], which results in fetching the field values from the row by index.

use crate::read::{MessageFromSqlRow, PgMessage};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types::{Integer, Text};

#[derive(Debug, Clone)]
pub struct Read<'a> {
    queue_name: &'a str,
    vt: i32,
    qty: i32,
}

impl QueryId for Read<'_> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> QueryFragment<Pg> for Read<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // Todo: statically generate the list of fields to select
        out.push_sql("SELECT (msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers) FROM pgmq.read(");

        out.push_sql("queue_name=>");
        out.push_bind_param::<Text, _>(self.queue_name)?;
        out.push_sql("::text");

        out.push_sql(", ");

        out.push_sql("vt=>");
        out.push_bind_param::<Integer, _>(&self.vt)?;
        out.push_sql("::integer");

        out.push_sql(", ");

        out.push_sql("qty=>");
        out.push_bind_param::<Integer, _>(&self.qty)?;
        out.push_sql("::integer");

        out.push_sql(")");
        Ok(())
    }
}

impl<'a> Query for Read<'a> {
    type SqlType = PgMessage;
}

impl<'a, C> RunQueryDsl<C> for Read<'a> where C: diesel::connection::LoadConnection<Backend = Pg> {}

fn read_query(queue_name: &str, vt: i32, qty: i32) -> Read<'_> {
    Read {
        queue_name,
        vt,
        qty,
    }
}

pub fn execute(conn: &mut PgConnection, queue: &str, qty: i32) {
    let _: Vec<MessageFromSqlRow> = read_query(queue, 0, qty).get_results(conn).unwrap();
}

#[cfg(test)]
mod tests {
    use super::read_query;
    use diesel::debug_query;

    #[test]
    fn query() {
        assert_eq!(
            "SELECT (msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers) FROM pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer) -- binds: [\"queue\", 0, 1]",
            debug_query(&read_query("queue", 0, 1)).to_string()
        );
    }

    #[test]
    fn query_cache_prepared() {
        use diesel::connection::statement_cache::QueryFragmentForCachedStatement;
        use diesel::pg::Pg;

        assert!(
            read_query("queue", 0, 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be cached"
        );
    }
}
