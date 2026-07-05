//! Shared SQL query strings.

// Note: the `language=PostgreSQL` comment allows JetBrains IDEs to provide inline syntax
// highlighting for the string content. See: https://www.jetbrains.com/help/idea/using-language-injections.html#use-language-injection-comments

// language=PostgreSQL
pub const CREATE: &str = "SELECT pgmq.create(queue_name=>$1::text)";

// language=PostgreSQL
pub const SEND: &str = "SELECT * FROM pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int)";

// language=PostgreSQL
pub const READ: &str = r"SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers FROM pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)";
