use crate::errors::PgmqError;
use crate::types::{Message, QUEUE_PREFIX};
#[cfg(feature = "cli")]
use crate::util::install_pgmq;
use crate::util::{check_input, connect};
use log::info;
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::Utc;
use sqlx::{Pool, Postgres, Row};

const DEFAULT_POLL_TIMEOUT_S: i32 = 5;
const DEFAULT_POLL_INTERVAL_MS: i32 = 250;

/// Main controller for interacting with a managed by the PGMQ Postgres extension.
#[derive(Clone, Debug)]
pub struct PGMQueueExt {
    pub url: String,
    pub connection: Pool<Postgres>,
}

pub struct PGMQueueMeta {
    pub queue_name: String,
    pub created_at: chrono::DateTime<Utc>,
    pub is_unlogged: bool,
    pub is_partitioned: bool,
}
impl PGMQueueExt {
    /// Initialize a connection to PGMQ/Postgres
    pub async fn new(url: String, max_connections: u32) -> Result<Self, PgmqError> {
        Ok(Self {
            connection: connect(&url, max_connections).await?,
            url,
        })
    }

    /// BYOP  - bring your own pool
    /// initialize a PGMQ connection with your own SQLx Postgres connection pool
    pub async fn new_with_pool(pool: Pool<Postgres>) -> Self {
        Self {
            url: "".to_owned(),
            connection: pool,
        }
    }

    #[cfg(feature = "cli")]
    pub async fn install_sql_with_cxn(
        &self,
        pool: &Pool<Postgres>,
        version: Option<&String>,
    ) -> Result<(), PgmqError> {
        install_pgmq(pool, version).await
    }

    #[cfg(feature = "cli")]
    pub async fn install_sql(&self, version: Option<&String>) -> Result<(), PgmqError> {
        self.install_sql_with_cxn(&self.connection, version).await
    }

    pub async fn init_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<bool, PgmqError> {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
            .execute(executor)
            .await
            .map(|_| true)
            .map_err(PgmqError::from)
    }

    pub async fn init(&self) -> Result<bool, PgmqError> {
        self.init_with_cxn(&self.connection).await
    }

    pub async fn create_with_cxn<'c, E>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError>
    where
        E: sqlx::Acquire<'c, Database = Postgres>,
    {
        check_input(queue_name)?;
        let mut conn = executor.acquire().await?;

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pgmq.meta WHERE queue_name = $1::text);",
        )
        .bind(queue_name)
        .fetch_one(&mut *conn)
        .await?;

        if exists {
            return Ok(false);
        }

        sqlx::query("SELECT * from pgmq.create(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(&mut *conn)
            .await?;
        Ok(true)
    }
    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_with_cxn(queue_name, &self.connection).await
    }

    pub async fn create_unlogged_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        sqlx::query("SELECT * from pgmq.create_unlogged(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(executor)
            .await?;
        Ok(true)
    }

    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create_unlogged(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_unlogged_with_cxn(queue_name, &self.connection)
            .await?;
        Ok(true)
    }

    pub async fn create_partitioned_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres> + std::marker::Copy,
    >(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let queue_table = format!("pgmq.{QUEUE_PREFIX}_{queue_name}");
        // we need to check whether the queue exists first
        // pg_partman create operations are currently unable to be idempotent
        let exists_stmt = "SELECT EXISTS(SELECT * from part_config where parent_table = $1);";
        let exists = sqlx::query_scalar(exists_stmt)
            .bind(queue_table)
            .fetch_one(executor)
            .await?;
        if exists {
            info!("queue: {queue_name} already exists",);
            Ok(false)
        } else {
            sqlx::query("SELECT * from pgmq.create_partitioned(queue_name=>$1::text);")
                .bind(queue_name)
                .execute(executor)
                .await?;
            Ok(true)
        }
    }

    /// Create a new partitioned queue.
    /// Errors when there is any database error and Ok(false) when the queue already exists.
    pub async fn create_partitioned(&self, queue_name: &str) -> Result<bool, PgmqError> {
        self.create_partitioned_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn drop_queue_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        executor
            .execute(
                sqlx::query("SELECT * from pgmq.drop_queue(queue_name=>$1::text);")
                    .bind(queue_name),
            )
            .await?;

        Ok(())
    }

    /// Drop an existing queue table.
    pub async fn drop_queue(&self, queue_name: &str) -> Result<(), PgmqError> {
        self.drop_queue_with_cxn(queue_name, &self.connection).await
    }

    /// Drop an existing queue table.
    pub async fn purge_queue_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let purged = sqlx::query("SELECT * from pgmq.purge_queue(queue_name=>$1::text);")
            .bind(queue_name)
            .fetch_one(executor)
            .await?;
        Ok(purged.try_get("purge_queue")?)
    }

    /// Drop an existing queue table.
    pub async fn purge_queue(&self, queue_name: &str) -> Result<i64, PgmqError> {
        self.purge_queue_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn list_queues_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        let queues = sqlx::query(r#"SELECT queue_name, is_partitioned, is_unlogged, created_at from pgmq.list_queues();"#)
            .fetch_all(executor)
            .await?;
        if queues.is_empty() {
            Ok(None)
        } else {
            let queues = queues
                .into_iter()
                .map(|q| {
                    Ok(PGMQueueMeta {
                        queue_name: q.try_get("queue_name")?,
                        created_at: q.try_get("created_at")?,
                        is_unlogged: q.try_get("is_unlogged")?,
                        is_partitioned: q.try_get("is_partitioned")?,
                    })
                })
                .collect::<Result<_, sqlx::Error>>()?;
            Ok(Some(queues))
        }
    }

    /// List all queues in the Postgres instance.
    pub async fn list_queues(&self) -> Result<Option<Vec<PGMQueueMeta>>, PgmqError> {
        self.list_queues_with_cxn(&self.connection).await
    }

    pub async fn set_vt_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: i32,
        executor: E,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        // queue_name, created_at as "created_at: chrono::DateTime<Utc>", is_partitioned, is_unlogged
        let updated = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.set_vt(queue_name=>$1::text, msg_id=>$2::bigint, vt=>$3::integer);"#
        )
        .bind(queue_name)
        .bind(msg_id)
        .bind(vt)
        .fetch_one(executor)
        .await?;
        let raw_msg = updated.try_get("message")?;
        let parsed_msg = serde_json::from_value::<T>(raw_msg)?;

        Ok(Message {
            msg_id: updated.try_get("msg_id")?,
            vt: updated.try_get("vt")?,
            read_ct: updated.try_get("read_ct")?,
            enqueued_at: updated.try_get("enqueued_at")?,
            message: parsed_msg,
        })
    }
    // Set the visibility time on an existing message.
    pub async fn set_vt<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: i32,
    ) -> Result<Message<T>, PgmqError> {
        self.set_vt_with_cxn(queue_name, msg_id, vt, &self.connection)
            .await
    }

    pub async fn send_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>, T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let prepared = sqlx::query(
            "SELECT send as msg_id from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>0::integer);",
        )
        .bind(queue_name)
        .bind(msg);
        let sent = prepared.fetch_one(executor).await?;
        Ok(sent.try_get("msg_id")?)
    }

    pub async fn send<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
    ) -> Result<i64, PgmqError> {
        self.send_with_cxn(queue_name, message, &self.connection)
            .await
    }

    pub async fn send_delay_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
    >(
        &self,
        queue_name: &str,
        message: &T,
        delay: u32,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let msg = serde_json::json!(&message);
        let sent = sqlx::query(
            "SELECT send as msg_id from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>$3::int);",
        )
        .bind(queue_name)
        .bind(msg)
        .bind(delay as i32)
        .fetch_one(executor)
        .await?;
        Ok(sent.try_get("msg_id")?)
    }

    pub async fn send_delay<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        delay: u32,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_cxn(queue_name, message, delay, &self.connection)
            .await
    }

    pub async fn read_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: i32,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)"#,
        )
        .bind(queue_name)
        .bind(vt)
        .bind(1)
        .fetch_optional(executor)
        .await?;
        match row {
            Some(row) => {
                // happy path - successfully read a message
                let raw_msg = row.try_get("message")?;
                let parsed_msg = serde_json::from_value::<T>(raw_msg)?;
                Ok(Some(Message {
                    msg_id: row.try_get("msg_id")?,
                    vt: row.try_get("vt")?,
                    read_ct: row.try_get("read_ct")?,
                    enqueued_at: row.try_get("enqueued_at")?,
                    message: parsed_msg,
                }))
            }
            None => {
                // no message found
                Ok(None)
            }
        }
    }
    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_with_cxn(queue_name, vt, &self.connection).await
    }

    pub async fn read_batch_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: i32,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        check_input(queue_name)?;
        let poll_timeout_s = poll_timeout.map_or(DEFAULT_POLL_TIMEOUT_S, |t| t.as_secs() as i32);
        let poll_interval_ms =
            poll_interval.map_or(DEFAULT_POLL_INTERVAL_MS, |i| i.as_millis() as i32);
        let result = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_with_poll(
                queue_name=>$1::text,
                vt=>$2::integer,
                qty=>$3::integer,
                max_poll_seconds=>$4::integer,
                poll_interval_ms=>$5::integer
            )"#,
        )
        .bind(queue_name)
        .bind(vt)
        .bind(max_batch_size)
        .bind(poll_timeout_s)
        .bind(poll_interval_ms)
        .fetch_all(executor)
        .await;

        match result {
            Err(sqlx::error::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e)?,
            Ok(rows) => {
                // happy path - successfully read messages
                let mut messages: Vec<Message<T>> = Vec::new();
                for row in rows.iter() {
                    let raw_msg = row.try_get("message")?;
                    let parsed_msg = serde_json::from_value::<T>(raw_msg);
                    if let Err(e) = parsed_msg {
                        return Err(PgmqError::JsonParsingError(e));
                    } else if let Ok(parsed_msg) = parsed_msg {
                        messages.push(Message {
                            msg_id: row.try_get("msg_id")?,
                            vt: row.try_get("vt")?,
                            read_ct: row.try_get("read_ct")?,
                            enqueued_at: row.try_get("enqueued_at")?,
                            message: parsed_msg,
                        })
                    }
                }
                Ok(Some(messages))
            }
        }
    }

    pub async fn read_batch_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: i32,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        self.read_batch_with_poll_with_cxn(
            queue_name,
            vt,
            max_batch_size,
            poll_timeout,
            poll_interval,
            &self.connection,
        )
        .await
    }

    pub async fn archive_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        executor: E,
    ) -> Result<bool, PgmqError> {
        check_input(queue_name)?;
        let arch =
            sqlx::query("SELECT * from pgmq.archive(queue_name=>$1::text, msg_id=>$2::bigint)")
                .bind(queue_name)
                .bind(msg_id)
                .fetch_one(executor)
                .await?;
        Ok(arch.try_get("archive")?)
    }
    /// Move a message to the archive table.
    pub async fn archive(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        self.archive_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }

    /// Move a slice of messages to the archive table.
    pub async fn archive_batch_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
        executor: E,
    ) -> Result<usize, PgmqError> {
        check_input(queue_name)?;
        let qty =
            sqlx::query("SELECT * from pgmq.archive(queue_name=>$1::text, msg_ids=>$2::bigint[])")
                .bind(queue_name)
                .bind(msg_ids)
                .fetch_all(executor)
                .await?
                .len();

        Ok(qty)
    }

    /// Move a slice of messages to the archive table.
    pub async fn archive_batch(
        &self,
        queue_name: &str,
        msg_ids: &[i64],
    ) -> Result<usize, PgmqError> {
        self.archive_batch_with_cxn(queue_name, msg_ids, &self.connection)
            .await
    }

    pub async fn pop_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let row = sqlx::query(r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.pop(queue_name=>$1::text)"#)
            .bind(queue_name)
            .fetch_optional(executor)
            .await?;
        match row {
            Some(row) => {
                // happy path - successfully read a message
                let raw_msg = row.try_get("message")?;
                let parsed_msg = serde_json::from_value::<T>(raw_msg)?;
                Ok(Some(Message {
                    msg_id: row.try_get("msg_id")?,
                    vt: row.try_get("vt")?,
                    read_ct: row.try_get("read_ct")?,
                    enqueued_at: row.try_get("enqueued_at")?,
                    message: parsed_msg,
                }))
            }
            None => {
                // no message found
                Ok(None)
            }
        }
    }
    // Read and message and immediately delete it.
    pub async fn pop<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.pop_with_cxn(queue_name, &self.connection).await
    }

    pub async fn delete_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        executor: E,
    ) -> Result<bool, PgmqError> {
        let row =
            sqlx::query("SELECT * from pgmq.delete(queue_name=>$1::text, msg_id=>$2::bigint)")
                .bind(queue_name)
                .bind(msg_id)
                .fetch_one(executor)
                .await?;
        Ok(row.try_get("delete")?)
    }

    // Delete a message by message id.
    pub async fn delete(&self, queue_name: &str, msg_id: i64) -> Result<bool, PgmqError> {
        self.delete_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }

    pub async fn delete_batch_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        msg_id: &[i64],
        executor: E,
    ) -> Result<usize, PgmqError> {
        let qty =
            sqlx::query("SELECT * from pgmq.delete(queue_name=>$1::text, msg_ids=>$2::bigint[])")
                .bind(queue_name)
                .bind(msg_id)
                .fetch_all(executor)
                .await?
                .len();

        // FIXME: change function signature to Vec<i64> and return rows
        Ok(qty)
    }

    // Delete with a slice of message ids
    pub async fn delete_batch(&self, queue_name: &str, msg_id: &[i64]) -> Result<usize, PgmqError> {
        self.delete_batch_with_cxn(queue_name, msg_id, &self.connection)
            .await
    }
}
