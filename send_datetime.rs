// Native support for send with target datetime
use chrono::{DateTime, Utc};

pub fn send_at(queue: &str, msg: serde_json::Value, execute_at: DateTime<Utc>) -> String {
    format!("INSERT INTO pgmq_{} (message, vt) VALUES ('{}', '{}')", queue, msg, execute_at.to_rfc3339())
}
