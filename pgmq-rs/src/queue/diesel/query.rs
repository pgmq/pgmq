//! Extracted Diesel SQL query functions. Can be used by both diesel and diesel-async.
use crate::queue::diesel::sql::{
    pgmq_acquire_queue_lock, pgmq_archive, pgmq_bind_topic, pgmq_convert_archive_partitioned,
    pgmq_create, pgmq_create_fifo_index, pgmq_create_fifo_indexes_all, pgmq_create_partitioned,
    pgmq_create_unlogged, pgmq_delete, pgmq_disable_notify_insert, pgmq_drop_queue,
    pgmq_enable_notify_insert, pgmq_list_notify_insert_throttles, pgmq_list_queues,
    pgmq_list_topic_bindings, pgmq_list_topic_bindings_all, pgmq_metrics, pgmq_metrics_all,
    pgmq_pop, pgmq_purge_queue, pgmq_read, pgmq_read_grouped, pgmq_read_grouped_head,
    pgmq_read_grouped_head_with_poll, pgmq_read_grouped_rr, pgmq_read_grouped_rr_with_poll,
    pgmq_read_grouped_with_poll, pgmq_read_with_poll, pgmq_send, pgmq_send_batch,
    pgmq_send_batch_topic, pgmq_send_topic, pgmq_set_vt, pgmq_unbind_topic,
    pgmq_update_notify_insert,
};
use crate::types::{
    InsertNotificationThrottleInterval, PollInterval, PollTimeout, QueueName,
    VisibilityTimeoutOffset,
};
use diesel::dsl::select;
use diesel::{ExpressionMethods, QueryDsl};

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_create(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_unlogged_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_create_unlogged(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_partitioned_query<'q, 'p, 'r>(
    queue_name: QueueName<'q>,
    partition_interval: &'p str,
    retention_interval: &'r str,
) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_create_partitioned(
        queue_name,
        partition_interval,
        retention_interval,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn convert_archive_partitioned_query<'q, 'p, 'r>(
    queue_name: QueueName<'q>,
    partition_interval: &'p str,
    retention_interval: &'r str,
) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_convert_archive_partitioned(
        queue_name,
        partition_interval,
        retention_interval,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn send_query(
    queue_name: QueueName<'_>,
    message: serde_json::Value,
    headers: serde_json::Value,
    delay: VisibilityTimeoutOffset,
) -> _ {
    let queue_name: &str = *queue_name;
    let delay: i32 = *delay;
    select(pgmq_send(queue_name, message, headers, delay))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn send_batch_query(
    queue_name: QueueName<'_>,
    messages: Vec<serde_json::Value>,
    headers: Option<Vec<serde_json::Value>>,
    delay: VisibilityTimeoutOffset,
) -> _ {
    let queue_name: &str = *queue_name;
    let delay: i32 = *delay;
    select(pgmq_send_batch(queue_name, messages, headers, delay))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_read(queue_name, visibility_timeout, quantity))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn pop_query(queue_name: QueueName<'_>, quantity: i32) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_pop(queue_name, quantity))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn archive_query<'q, 'm>(queue_name: QueueName<'q>, msg_ids: &'m [i64]) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_archive(queue_name, msg_ids))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn delete_query<'q, 'm>(queue_name: QueueName<'q>, msg_ids: &'m [i64]) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_delete(queue_name, msg_ids))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn set_vt_query<'q, 'm>(
    queue_name: QueueName<'q>,
    msg_ids: &'m [i64],
    visibility_timeout: VisibilityTimeoutOffset,
) -> _ {
    let queue_name: &'q str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_set_vt(queue_name, msg_ids, visibility_timeout))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_fifo_index_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_create_fifo_index(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_fifo_indexes_all_query() -> _ {
    select(pgmq_create_fifo_indexes_all())
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_read_grouped(queue_name, visibility_timeout, quantity))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_head_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_read_grouped_head(
        queue_name,
        visibility_timeout,
        quantity,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_rr_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_read_grouped_rr(
        queue_name,
        visibility_timeout,
        quantity,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn bind_topic_query<'p, 'q>(pattern: &'p str, queue_name: QueueName<'q>) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_bind_topic(pattern, queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn unbind_topic_query<'p, 'q>(pattern: &'p str, queue_name: QueueName<'q>) -> _ {
    let queue_name: &'q str = *queue_name;
    select(pgmq_unbind_topic(pattern, queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn list_topic_bindings_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_list_topic_bindings(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn list_topic_bindings_all_query() -> _ {
    select(pgmq_list_topic_bindings_all())
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn send_topic_query(
    routing_key: &str,
    message: serde_json::Value,
    headers: serde_json::Value,
    delay: VisibilityTimeoutOffset,
) -> _ {
    let delay: i32 = *delay;
    select(pgmq_send_topic(routing_key, message, headers, delay))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn send_batch_topic_query(
    routing_key: &str,
    messages: Vec<serde_json::Value>,
    headers: Option<Vec<serde_json::Value>>,
    delay: VisibilityTimeoutOffset,
) -> _ {
    let delay: i32 = *delay;
    select(pgmq_send_batch_topic(routing_key, messages, headers, delay))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn enable_notify_insert_query(
    queue_name: QueueName<'_>,
    throttle_interval: InsertNotificationThrottleInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let throttle_interval: i32 = *throttle_interval;
    select(pgmq_enable_notify_insert(queue_name, throttle_interval))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn update_notify_insert_query(
    queue_name: QueueName<'_>,
    throttle_interval: InsertNotificationThrottleInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let throttle_interval: i32 = *throttle_interval;
    select(pgmq_update_notify_insert(queue_name, throttle_interval))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn disable_notify_insert_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_disable_notify_insert(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn list_notify_insert_throttles_query() -> _ {
    select(pgmq_list_notify_insert_throttles())
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn list_queues_query() -> _ {
    select(pgmq_list_queues())
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn queue_metadata_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    crate::queue::diesel::schema::meta::table
        .filter(crate::queue::diesel::schema::meta::queue_name.eq(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn acquire_queue_lock_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_acquire_queue_lock(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn purge_queue_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_purge_queue(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn drop_queue_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_drop_queue(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn metrics_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_metrics(queue_name))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn metrics_all_query() -> _ {
    select(pgmq_metrics_all())
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_with_poll_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
    poll_timeout: PollTimeout,
    poll_interval: PollInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    let poll_timeout: i32 = *poll_timeout;
    let poll_interval: i32 = *poll_interval;
    select(pgmq_read_with_poll(
        queue_name,
        visibility_timeout,
        quantity,
        poll_timeout,
        poll_interval,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_with_poll_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
    poll_timeout: PollTimeout,
    poll_interval: PollInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    let poll_timeout: i32 = *poll_timeout;
    let poll_interval: i32 = *poll_interval;
    select(pgmq_read_grouped_with_poll(
        queue_name,
        visibility_timeout,
        quantity,
        poll_timeout,
        poll_interval,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_rr_with_poll_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
    poll_timeout: PollTimeout,
    poll_interval: PollInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    let poll_timeout: i32 = *poll_timeout;
    let poll_interval: i32 = *poll_interval;
    select(pgmq_read_grouped_rr_with_poll(
        queue_name,
        visibility_timeout,
        quantity,
        poll_timeout,
        poll_interval,
    ))
}

#[diesel::dsl::auto_type(no_type_alias)]
pub fn read_grouped_head_with_poll_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
    poll_timeout: PollTimeout,
    poll_interval: PollInterval,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    let poll_timeout: i32 = *poll_timeout;
    let poll_interval: i32 = *poll_interval;
    select(pgmq_read_grouped_head_with_poll(
        queue_name,
        visibility_timeout,
        quantity,
        poll_timeout,
        poll_interval,
    ))
}

/// These tests detect changes in the SQL generated by each query. Changes to the generated SQL may
/// be allowed, but should be audited and explicitly accepted.
///
/// The generated SQL for each query is stored in a snapshot file. See <https://docs.rs/insta> for
/// more details.
#[cfg(test)]
mod test_sql {
    use diesel::debug_query;
    use diesel::pg::Pg;
    use insta::assert_snapshot;

    #[test]
    fn create_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::create_query("queue".try_into().unwrap())).to_string()
        );
    }

    #[test]
    fn create_unlogged_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::create_unlogged_query(
            "queue".try_into().unwrap()
        ))
        .to_string());
    }

    #[test]
    fn create_partitioned_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::create_partitioned_query(
            "queue".try_into().unwrap(),
            "1000",
            "10000"
        ))
        .to_string());
    }

    #[test]
    fn convert_archive_partitioned_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::convert_archive_partitioned_query(
                "queue".try_into().unwrap(),
                "1000",
                "10000"
            ))
            .to_string()
        );
    }

    #[test]
    fn send_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::send_query(
            "queue".try_into().unwrap(),
            serde_json::Value::Null,
            serde_json::Value::Null,
            0.into()
        ))
        .to_string());
    }

    #[test]
    fn send_batch_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::send_batch_query(
            "queue".try_into().unwrap(),
            vec![serde_json::Value::Null],
            Some(vec![serde_json::Value::Null]),
            0.into()
        ))
        .to_string());
    }

    #[test]
    fn read_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_query(
            "queue".try_into().unwrap(),
            10.into(),
            1
        ))
        .to_string());
    }

    #[test]
    fn pop_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::pop_query("queue".try_into().unwrap(), 1)).to_string()
        );
    }

    #[test]
    fn archive_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::archive_query(
            "queue".try_into().unwrap(),
            &[1]
        ))
        .to_string());
    }

    #[test]
    fn delete_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::delete_query(
            "queue".try_into().unwrap(),
            &[1]
        ))
        .to_string());
    }

    #[test]
    fn set_vt_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::set_vt_query(
            "queue".try_into().unwrap(),
            &[1],
            10.into()
        ))
        .to_string());
    }

    #[test]
    fn create_fifo_index_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::create_fifo_index_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn create_fifo_indexes_all_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::create_fifo_indexes_all_query()).to_string());
    }

    #[test]
    fn read_grouped_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_grouped_query(
            "queue".try_into().unwrap(),
            10.into(),
            1
        ))
        .to_string());
    }

    #[test]
    fn read_grouped_head_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_grouped_head_query(
            "queue".try_into().unwrap(),
            10.into(),
            1
        ))
        .to_string());
    }

    #[test]
    fn read_grouped_rr_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_grouped_rr_query(
            "queue".try_into().unwrap(),
            10.into(),
            1
        ))
        .to_string());
    }

    #[test]
    fn bind_topic_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::bind_topic_query(
            "pattern",
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn unbind_topic_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::unbind_topic_query(
            "pattern",
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn list_topic_bindings_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::list_topic_bindings_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn list_topic_bindings_all_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::list_topic_bindings_all_query()).to_string());
    }

    #[test]
    fn send_topic_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::send_topic_query(
            "pattern",
            serde_json::Value::Null,
            serde_json::Value::Null,
            0.into()
        ))
        .to_string());
    }

    #[test]
    fn send_batch_topic_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::send_batch_topic_query(
            "pattern",
            vec![serde_json::Value::Null],
            None,
            0.into()
        ))
        .to_string());
    }

    #[test]
    fn enable_notify_insert_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::enable_notify_insert_query(
            "queue".try_into().unwrap(),
            100.into()
        ))
        .to_string());
    }

    #[test]
    fn update_notify_insert_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::update_notify_insert_query(
            "queue".try_into().unwrap(),
            100.into()
        ))
        .to_string());
    }

    #[test]
    fn disable_notify_insert_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::disable_notify_insert_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn list_notify_insert_throttles_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::list_notify_insert_throttles_query()).to_string()
        );
    }

    #[test]
    fn list_queues_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::list_queues_query()).to_string());
    }

    #[test]
    fn queue_metadata_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::queue_metadata_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn acquire_queue_lock_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::acquire_queue_lock_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn purge_queue_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::purge_queue_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn drop_queue_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::drop_queue_query(
            "queue".try_into().unwrap(),
        ))
        .to_string());
    }

    #[test]
    fn metrics_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::metrics_query("queue".try_into().unwrap())).to_string()
        );
    }

    #[test]
    fn metrics_all_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::metrics_all_query()).to_string());
    }

    #[test]
    fn read_with_poll_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_with_poll_query(
            "queue".try_into().unwrap(),
            10.into(),
            1,
            1.into(),
            250.into()
        ))
        .to_string());
    }

    #[test]
    fn read_grouped_with_poll_query() {
        assert_snapshot!(debug_query::<Pg, _>(&super::read_grouped_with_poll_query(
            "queue".try_into().unwrap(),
            10.into(),
            1,
            1.into(),
            250.into()
        ))
        .to_string());
    }

    #[test]
    fn read_grouped_rr_with_poll_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::read_grouped_rr_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            ))
            .to_string()
        );
    }

    #[test]
    fn read_grouped_head_with_poll_query() {
        assert_snapshot!(
            debug_query::<Pg, _>(&super::read_grouped_head_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            ))
            .to_string()
        );
    }
}

/// These tests ensure that each query is safe to store in the prepared statement cache, which
/// is important for performance.
#[cfg(test)]
mod cached_tests {
    use diesel::connection::statement_cache::QueryFragmentForCachedStatement;
    use diesel::pg::Pg;

    #[test]
    fn create_query() {
        assert!(
            super::create_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn create_unlogged_query() {
        assert!(
            super::create_unlogged_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn create_partitioned_query() {
        assert!(
            super::create_partitioned_query("queue".try_into().unwrap(), "1000", "10000")
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn convert_archive_partitioned_query() {
        assert!(
            super::convert_archive_partitioned_query("queue".try_into().unwrap(), "1000", "10000")
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn send_query() {
        assert!(
            super::send_query(
                "queue".try_into().unwrap(),
                serde_json::Value::Null,
                serde_json::Value::Null,
                0.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn send_batch_query() {
        assert!(
            super::send_batch_query(
                "queue".try_into().unwrap(),
                vec![serde_json::Value::Null],
                Some(vec![serde_json::Value::Null]),
                0.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_query() {
        assert!(
            super::read_query("queue".try_into().unwrap(), 10.into(), 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn pop_query() {
        assert!(
            super::pop_query("queue".try_into().unwrap(), 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn archive_query() {
        assert!(
            super::archive_query("queue".try_into().unwrap(), &[1])
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn delete_query() {
        assert!(
            super::delete_query("queue".try_into().unwrap(), &[1])
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn set_vt_query() {
        assert!(
            super::set_vt_query("queue".try_into().unwrap(), &[1], 10.into())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn create_fifo_index_query() {
        assert!(
            super::create_fifo_index_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn create_fifo_indexes_all_query() {
        assert!(
            super::create_fifo_indexes_all_query()
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_query() {
        assert!(
            super::read_grouped_query("queue".try_into().unwrap(), 10.into(), 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_head_query() {
        assert!(
            super::read_grouped_head_query("queue".try_into().unwrap(), 10.into(), 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_rr_query() {
        assert!(
            super::read_grouped_rr_query("queue".try_into().unwrap(), 10.into(), 1)
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn bind_topic_query() {
        assert!(
            super::bind_topic_query("pattern", "queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn unbind_topic_query() {
        assert!(
            super::unbind_topic_query("pattern", "queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn list_topic_bindings_query() {
        assert!(
            super::list_topic_bindings_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn list_topic_bindings_all_query() {
        assert!(
            super::list_topic_bindings_all_query()
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn send_topic_query() {
        assert!(
            super::send_topic_query(
                "pattern",
                serde_json::Value::Null,
                serde_json::Value::Null,
                0.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn send_batch_topic_query() {
        assert!(
            super::send_batch_topic_query("pattern", vec![serde_json::Value::Null], None, 0.into())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn enable_notify_insert_query() {
        assert!(
            super::enable_notify_insert_query("queue".try_into().unwrap(), 100.into())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn update_notify_insert_query() {
        assert!(
            super::update_notify_insert_query("queue".try_into().unwrap(), 100.into())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn disable_notify_insert_query() {
        assert!(
            super::disable_notify_insert_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn list_notify_insert_throttles_query() {
        assert!(
            super::list_notify_insert_throttles_query()
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn list_queues_query() {
        assert!(
            super::list_queues_query()
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn queue_metadata_query() {
        assert!(
            super::queue_metadata_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn acquire_queue_lock_query() {
        assert!(
            super::acquire_queue_lock_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn purge_queue_query() {
        assert!(
            super::purge_queue_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn drop_queue_query() {
        assert!(
            super::drop_queue_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn metrics_query() {
        assert!(
            super::metrics_query("queue".try_into().unwrap())
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn metrics_all_query() {
        assert!(
            super::metrics_all_query()
                .is_safe_to_cache_prepared(&Pg)
                .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_with_poll_query() {
        assert!(
            super::read_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_with_poll_query() {
        assert!(
            super::read_grouped_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_rr_with_poll_query() {
        assert!(
            super::read_grouped_rr_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }

    #[test]
    fn read_grouped_head_with_poll_query() {
        assert!(
            super::read_grouped_head_with_poll_query(
                "queue".try_into().unwrap(),
                10.into(),
                1,
                1.into(),
                250.into()
            )
            .is_safe_to_cache_prepared(&Pg)
            .unwrap(),
            "Should be safe to cache"
        );
    }
}
