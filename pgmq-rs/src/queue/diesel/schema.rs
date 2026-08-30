diesel::table! {
    /*
    Technically, diesel does not support the `pgmq.meta` table because it does not have a primary
    key defined. However, the `queue_name` column is marked `UNIQUE`, which creates an index on
    the column. So, we manually define this schema using the `queue_name` as the "primary key",
    which is sufficient for our purposes as we only need to query by the `queue_name` and we do not
    need to join with any other tables.
     */
    pgmq.meta (queue_name) {
        queue_name -> Text,
        is_partitioned -> Bool,
        is_unlogged -> Bool,
        created_at -> Timestamptz
    }
}
