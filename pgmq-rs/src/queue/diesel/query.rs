//! Extracted Diesel SQL query functions. Can be used by both diesel and diesel-async.
use crate::queue::diesel::sql::{
    pgmq_archive, pgmq_bind_topic, pgmq_create, pgmq_create_fifo_index,
    pgmq_create_fifo_indexes_all, pgmq_delete, pgmq_disable_notify_insert,
    pgmq_enable_notify_insert, pgmq_list_notify_insert_throttles, pgmq_list_topic_bindings,
    pgmq_list_topic_bindings_all, pgmq_pop, pgmq_read, pgmq_read_grouped, pgmq_read_grouped_head,
    pgmq_read_grouped_rr, pgmq_send, pgmq_send_batch, pgmq_send_batch_topic, pgmq_send_topic,
    pgmq_set_vt, pgmq_unbind_topic, pgmq_update_notify_insert,
};
use crate::types::{InsertNotificationThrottleInterval, QueueName, VisibilityTimeoutOffset};
use diesel::dsl::select;

#[diesel::dsl::auto_type(no_type_alias)]
pub fn create_queue_query(queue_name: QueueName<'_>) -> _ {
    let queue_name: &str = *queue_name;
    select(pgmq_create(queue_name))
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
