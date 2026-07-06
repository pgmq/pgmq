//! Extracted Diesel SQL query functions. Can be used by both diesel and diesel-async.
use crate::queue::diesel::sql::{pgmq_create, pgmq_read, pgmq_send};
use crate::types::{QueueName, VisibilityTimeoutOffset};
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
pub fn read_query(
    queue_name: QueueName<'_>,
    visibility_timeout: VisibilityTimeoutOffset,
    quantity: i32,
) -> _ {
    let queue_name: &str = *queue_name;
    let visibility_timeout: i32 = *visibility_timeout;
    select(pgmq_read(queue_name, visibility_timeout, quantity))
}
