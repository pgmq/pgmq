mod visibility_timeout_offest;

use crate::errors::PgmqError;
use crate::types::{
    ListNotifyInsertThrottlesRow, ListTopicBindingsRow, Message, QueueMetrics, SendBatchTopicRow,
    QUEUE_PREFIX,
};
use crate::util::{check_input, connect};
use log::info;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::types::chrono::Utc;
use sqlx::{FromRow, Pool, Postgres, Row};
pub use visibility_timeout_offest::VisibilityTimeoutOffset;

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

    #[cfg(feature = "install-sql-github")]
    #[deprecated(
        note = "Use install_sql_from_github_with_cxn/install_sql_from_github or install_sql_embedded_with_cxn/install_sql_embedded instead.",
        since = "0.33.0"
    )]
    pub async fn install_sql_with_cxn(
        &self,
        pool: &Pool<Postgres>,
        version: Option<&String>,
    ) -> Result<(), PgmqError> {
        self.install_sql_from_github_with_cxn(pool, version.map(|v| v.as_str()))
            .await
    }

    #[cfg(feature = "install-sql-github")]
    #[deprecated(
        note = "Use install_sql_from_github_with_cxn/install_sql_from_github or install_sql_embedded_with_cxn/install_sql_embedded instead.",
        since = "0.33.0"
    )]
    pub async fn install_sql(&self, version: Option<&String>) -> Result<(), PgmqError> {
        self.install_sql_from_github(version.map(|v| v.as_str()))
            .await
    }

    #[cfg(feature = "install-sql")]
    #[doc = include_str!("../install/init_migrations_table.md")]
    pub async fn init_migrations_table_with_cxn(
        &self,
        pool: &Pool<Postgres>,
        version: &str,
    ) -> Result<(), PgmqError> {
        use std::str::FromStr;
        crate::install::init_migrations_table(pool, crate::install::Version::from_str(version)?)
            .await
    }

    #[cfg(feature = "install-sql")]
    #[doc = include_str!("../install/init_migrations_table.md")]
    pub async fn init_migrations_table(&self, version: &str) -> Result<(), PgmqError> {
        self.init_migrations_table_with_cxn(&self.connection, version)
            .await
    }

    #[cfg(feature = "install-sql")]
    #[doc = include_str!("../install/installed_version.md")]
    pub async fn installed_version_with_cxn(
        &self,
        pool: &Pool<Postgres>,
    ) -> Result<Option<crate::install::Version>, PgmqError> {
        crate::install::installed_version(pool).await
    }

    #[cfg(feature = "install-sql")]
    #[doc = include_str!("../install/installed_version.md")]
    pub async fn installed_version(&self) -> Result<Option<crate::install::Version>, PgmqError> {
        self.installed_version_with_cxn(&self.connection).await
    }

    #[cfg(feature = "install-sql-github")]
    #[doc = include_str!("../install/github/install_sql_github.md")]
    pub async fn install_sql_from_github_with_cxn(
        &self,
        pool: &Pool<Postgres>,
        version: Option<&str>,
    ) -> Result<(), PgmqError> {
        crate::install::install_sql_from_github(pool, version).await
    }

    #[cfg(feature = "install-sql-github")]
    #[doc = include_str!("../install/github/install_sql_github.md")]
    pub async fn install_sql_from_github(&self, version: Option<&str>) -> Result<(), PgmqError> {
        self.install_sql_from_github_with_cxn(&self.connection, version)
            .await
    }

    #[cfg(feature = "install-sql-embedded")]
    #[doc = include_str!("../install/embedded/install_sql_embedded.md")]
    pub async fn install_sql_from_embedded_with_cxn(
        &self,
        pool: &Pool<Postgres>,
    ) -> Result<(), PgmqError> {
        crate::install::install_sql_from_embedded(pool).await
    }

    #[cfg(feature = "install-sql-embedded")]
    #[doc = include_str!("../install/embedded/install_sql_embedded.md")]
    pub async fn install_sql_from_embedded(&self) -> Result<(), PgmqError> {
        self.install_sql_from_embedded_with_cxn(&self.connection)
            .await
    }

    pub async fn init_with_cxn<'c, E: sqlx::Acquire<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<bool, PgmqError> {
        let mut txn = executor.begin().await?;
        crate::util::init_lock(&mut txn).await?;
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
            .execute(&mut *txn)
            .await
            .map(|_| true)?;
        txn.commit().await?;
        Ok(true)
    }

    pub async fn init(&self) -> Result<bool, PgmqError> {
        self.init_with_cxn(&self.connection).await
    }

    /// Acquire a transaction-level advisory lock specific to the provided queue. Useful to prevent
    /// race conditions when performing queue/table-level operations, such as creating an index
    /// for the queue (e.g., with [`Self::create_fifo_index`].
    pub async fn acquire_queue_lock_with_txn<'c>(
        &self,
        queue_name: &str,
        txn: &mut sqlx::Transaction<'c, Postgres>,
    ) -> Result<(), PgmqError> {
        sqlx::query("SELECT pgmq.acquire_queue_lock(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(&mut **txn)
            .await?;
        Ok(())
    }

    /// Acquire a transaction-level advisory lock specific to the provided queue. Useful to prevent
    /// race conditions when performing queue/table-level operations, such as creating an index
    /// for the queue (e.g., with [`Self::create_fifo_index`].
    ///
    /// Returns the [`sqlx::Transaction`] that should be used to perform the queue/table-level
    /// operations. Remember to call [`sqlx::Transaction::commit`] after performing the desired
    /// operations.
    pub async fn acquire_queue_lock_with_cxn<'c, E: sqlx::Acquire<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<sqlx::Transaction<'c, Postgres>, PgmqError> {
        let mut txn = executor.begin().await?;

        self.acquire_queue_lock_with_txn(queue_name, &mut txn)
            .await?;

        Ok(txn)
    }

    /// Acquire a transaction-level advisory lock specific to the provided queue. Useful to prevent
    /// race conditions when performing queue/table-level operations, such as creating an index
    /// for the queue (e.g., with [`Self::create_fifo_index_with_cxn`]).
    ///
    /// Returns the [`sqlx::Transaction`] that should be used to perform the queue/table-level
    /// operations. Remember to call [`sqlx::Transaction::commit`] after performing the desired
    /// operations.
    pub async fn acquire_queue_lock<'c>(
        &self,
        queue_name: &str,
    ) -> Result<sqlx::Transaction<'c, Postgres>, PgmqError> {
        let txn = self
            .acquire_queue_lock_with_cxn(queue_name, &self.connection)
            .await?;
        Ok(txn)
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
        let mut txn = self
            .acquire_queue_lock_with_cxn(queue_name, executor)
            .await?;

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pgmq.meta WHERE queue_name = $1::text);",
        )
        .bind(queue_name)
        .fetch_one(&mut *txn)
        .await?;

        if exists {
            return Ok(false);
        }

        sqlx::query("SELECT pgmq.create(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(&mut *txn)
            .await?;

        txn.commit().await?;

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
        sqlx::query("SELECT pgmq.create_unlogged(queue_name=>$1::text);")
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
        let exists =
            sqlx::query_scalar("SELECT EXISTS(SELECT * from part_config where parent_table = $1);")
                .bind(queue_table)
                .fetch_one(executor)
                .await?;
        if exists {
            info!("queue: {queue_name} already exists",);
            Ok(false)
        } else {
            sqlx::query("SELECT pgmq.create_partitioned(queue_name=>$1::text);")
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

    pub async fn convert_archive_partitioned_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres> + std::marker::Copy,
    >(
        &self,
        queue_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
        executor: E,
    ) -> Result<(), PgmqError> {
        let mut query: sqlx::QueryBuilder<Postgres> =
            sqlx::QueryBuilder::new("SELECT pgmq.convert_archive_partitioned(");

        {
            let mut separated = query.separated(", ");
            separated
                .push("table_name=>")
                .push_bind_unseparated(queue_name);

            if let Some(partition_interval) = partition_interval {
                separated
                    .push("partition_interval=>")
                    .push_bind_unseparated(partition_interval);
            }

            if let Some(retention_interval) = retention_interval {
                separated
                    .push("retention_interval=>")
                    .push_bind_unseparated(retention_interval);
            }
        }

        query.push(")").build().execute(executor).await?;

        Ok(())
    }

    pub async fn convert_archive_partitioned(
        &self,
        table_name: &str,
        partition_interval: Option<&str>,
        retention_interval: Option<&str>,
    ) -> Result<(), PgmqError> {
        self.convert_archive_partitioned_with_cxn(
            table_name,
            partition_interval,
            retention_interval,
            &self.connection,
        )
        .await
    }

    pub async fn drop_queue_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sqlx::query("SELECT pgmq.drop_queue(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(executor)
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
        vt: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<Message<T>, PgmqError> {
        check_input(queue_name)?;
        let vt: VisibilityTimeoutOffset = vt.into();
        // queue_name, created_at as "created_at: chrono::DateTime<Utc>", is_partitioned, is_unlogged
        let updated = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.set_vt(queue_name=>$1::text, msg_id=>$2::bigint, vt=>$3::integer);"#
        )
            .bind(queue_name)
            .bind(msg_id)
            .bind(vt)
            .fetch_one(executor)
            .await
            .and_then(|row| Message::<T>::from_row(&row))?;

        Ok(updated)
    }
    // Set the visibility time on an existing message.
    pub async fn set_vt<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        msg_id: i64,
        vt: impl Into<VisibilityTimeoutOffset>,
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
        self.send_delay_with_cxn(queue_name, message, 0, executor)
            .await
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
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_headers_with_cxn(
            queue_name,
            message,
            Option::<&()>::None,
            delay,
            executor,
        )
        .await
    }

    pub async fn send_delay<T: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_cxn(queue_name, message, delay, &self.connection)
            .await
    }

    pub async fn send_delay_with_headers_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
        H: Serialize,
    >(
        &self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<i64, PgmqError> {
        check_input(queue_name)?;
        let delay: VisibilityTimeoutOffset = delay.into();
        let message = serde_json::to_value(message)?;
        let headers = serde_json::to_value(headers)?;
        let msg_id: i64 = sqlx::query_scalar(
            "SELECT * from pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int);",
        )
        .bind(queue_name)
        .bind(message)
        .bind(headers)
        .bind(delay)
        .fetch_one(executor)
        .await?;
        Ok(msg_id)
    }

    pub async fn send_delay_with_headers<T: Serialize, H: Serialize>(
        &self,
        queue_name: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<i64, PgmqError> {
        self.send_delay_with_headers_with_cxn(queue_name, message, headers, delay, &self.connection)
            .await
    }

    pub async fn send_batch_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
    >(
        &self,
        queue_name: &str,
        messages: &[T],
        executor: E,
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay_with_cxn(queue_name, messages, 0, executor)
            .await
    }

    pub async fn send_batch<T: Serialize>(
        &self,
        queue_name: &str,
        messages: &[T],
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_cxn(queue_name, messages, &self.connection)
            .await
    }

    pub async fn send_batch_with_delay_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
    >(
        &self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay_with_headers_with_cxn(
            queue_name,
            messages,
            Option::<&[()]>::None,
            delay,
            executor,
        )
        .await
    }

    pub async fn send_batch_with_delay<T: Serialize>(
        &self,
        queue_name: &str,
        messages: &[T],
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay_with_cxn(queue_name, messages, delay, &self.connection)
            .await
    }

    pub async fn send_batch_with_delay_with_headers_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
        H: Serialize,
    >(
        &self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<Vec<i64>, PgmqError> {
        check_input(queue_name)?;
        let delay: VisibilityTimeoutOffset = delay.into();
        let messages = Self::serialize_list(messages)?;
        let headers = Self::serialize_optional_list(headers)?;
        let sent: Vec<i64> = sqlx::query_scalar(
            "SELECT * from pgmq.send_batch(queue_name=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer);",
        )
            .bind(queue_name)
            .bind(messages)
            .bind(headers)
            .bind(delay)
            .fetch_all(executor)
            .await?;
        Ok(sent)
    }

    pub async fn send_batch_with_delay_with_headers<T: Serialize, H: Serialize>(
        &self,
        queue_name: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<Vec<i64>, PgmqError> {
        self.send_batch_with_delay_with_headers_with_cxn(
            queue_name,
            messages,
            headers,
            delay,
            &self.connection,
        )
        .await
    }

    pub async fn read_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_batch_with_cxn(queue_name, vt, 1, executor)
            .await
            .map(|result| result.into_iter().next())
    }

    pub async fn read<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_with_cxn(queue_name, vt, &self.connection).await
    }

    pub async fn read_batch_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer)"#,
        );

        Self::read_batch_common(query, queue_name, vt, qty, executor).await
    }

    pub async fn read_batch<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_batch_with_cxn(queue_name, vt, qty, &self.connection)
            .await
    }

    pub async fn read_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_batch_with_poll_with_cxn(queue_name, vt, 1, poll_timeout, poll_interval, executor)
            .await
            .map(|result| result.and_then(|result| result.into_iter().next()))
    }

    pub async fn read_with_poll<'c, T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Option<Message<T>>, PgmqError> {
        self.read_with_poll_with_cxn(
            queue_name,
            vt,
            poll_timeout,
            poll_interval,
            &self.connection,
        )
        .await
    }

    // Todo: In a future SemVer-breaking release, we can update this to return
    //  `Result<Vec<Message<T>>, PgmqError>` to match `read_batch`/`read_batch_with_cxn`.
    pub async fn read_batch_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Option<Vec<Message<T>>>, PgmqError> {
        let query = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_with_poll(
                queue_name=>$1::text,
                vt=>$2::integer,
                qty=>$3::integer,
                max_poll_seconds=>$4::integer,
                poll_interval_ms=>$5::integer
            )"#,
        );

        Self::read_batch_with_poll_common(
            query,
            queue_name,
            vt,
            max_batch_size,
            poll_timeout,
            poll_interval,
            executor,
        )
        .await
        .map(Some)
    }

    pub async fn read_batch_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
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

    pub async fn read_grouped_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query("SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_grouped(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer);");

        Self::read_batch_common(query, queue_name, vt, qty, executor).await
    }

    pub async fn read_grouped<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_grouped_with_cxn(queue_name, vt, qty, &self.connection)
            .await
    }

    pub async fn read_grouped_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_grouped_with_poll(
                queue_name=>$1::text,
                vt=>$2::integer,
                qty=>$3::integer,
                max_poll_seconds=>$4::integer,
                poll_interval_ms=>$5::integer
            )"#,
        );

        Self::read_batch_with_poll_common(
            query,
            queue_name,
            vt,
            qty,
            poll_timeout,
            poll_interval,
            executor,
        )
        .await
    }

    pub async fn read_grouped_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_grouped_with_poll_with_cxn(
            queue_name,
            vt,
            qty,
            poll_timeout,
            poll_interval,
            &self.connection,
        )
        .await
    }

    pub async fn read_grouped_head_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query("SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_grouped_head(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer);");

        Self::read_batch_common(query, queue_name, vt, qty, executor).await
    }

    pub async fn read_grouped_head<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_grouped_head_with_cxn(queue_name, vt, qty, &self.connection)
            .await
    }

    pub async fn read_grouped_rr_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query("SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_grouped_rr(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer);");

        Self::read_batch_common(query, queue_name, vt, qty, executor).await
    }

    pub async fn read_grouped_rr<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_grouped_rr_with_cxn(queue_name, vt, qty, &self.connection)
            .await
    }

    pub async fn read_grouped_rr_with_poll_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let query = sqlx::query(
            r#"SELECT msg_id, read_ct, enqueued_at, vt, message from pgmq.read_grouped_rr_with_poll(
                queue_name=>$1::text,
                vt=>$2::integer,
                qty=>$3::integer,
                max_poll_seconds=>$4::integer,
                poll_interval_ms=>$5::integer
            )"#,
        );

        Self::read_batch_with_poll_common(
            query,
            queue_name,
            vt,
            qty,
            poll_timeout,
            poll_interval,
            executor,
        )
        .await
    }

    pub async fn read_grouped_rr_with_poll<T: for<'de> Deserialize<'de>>(
        &self,
        queue_name: &str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        self.read_grouped_rr_with_poll_with_cxn(
            queue_name,
            vt,
            qty,
            poll_timeout,
            poll_interval,
            &self.connection,
        )
        .await
    }

    async fn read_batch_common<
        'c,
        'q,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        query: sqlx::query::Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>,
        queue_name: &'q str,
        vt: impl Into<VisibilityTimeoutOffset>,
        qty: i32,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt: VisibilityTimeoutOffset = vt.into();
        let rows = query
            .bind(queue_name)
            .bind(vt)
            .bind(qty)
            .fetch_all(executor)
            .await?;

        Self::handle_read_batch_result(rows)
    }

    async fn read_batch_with_poll_common<
        'c,
        'q,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: for<'de> Deserialize<'de>,
    >(
        query: sqlx::query::Query<'q, Postgres, <Postgres as sqlx::Database>::Arguments>,
        queue_name: &'q str,
        vt: impl Into<VisibilityTimeoutOffset>,
        max_batch_size: i32,
        poll_timeout: Option<std::time::Duration>,
        poll_interval: Option<std::time::Duration>,
        executor: E,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        check_input(queue_name)?;
        let vt: VisibilityTimeoutOffset = vt.into();
        let poll_timeout_s = poll_timeout.map_or(DEFAULT_POLL_TIMEOUT_S, |t| t.as_secs() as i32);
        let poll_interval_ms =
            poll_interval.map_or(DEFAULT_POLL_INTERVAL_MS, |i| i.as_millis() as i32);
        let rows = query
            .bind(queue_name)
            .bind(vt)
            .bind(max_batch_size)
            .bind(poll_timeout_s)
            .bind(poll_interval_ms)
            .fetch_all(executor)
            .await?;

        Self::handle_read_batch_result(rows)
    }

    /// Helper method to convert [`PgRow`] to [`Message`] for the `read*`/`read_batch*` methods.
    /// This is needed, vs using [`sqlx::query_as`] to directly convert the result to
    /// [`Message`], because in order to use [`sqlx::query_as`] we need to add trait constraints
    /// to the `T` type parameter used in the `read*`/`read_batch*` methods, which
    /// would be a breaking change.
    // Todo: In a future SemVer-breaking release, replace this method using `query_as`
    //  to directly parse the SQL query rows to `Vec<Message<T>`.
    fn handle_read_batch_result<T: for<'de> Deserialize<'de>>(
        rows: Vec<PgRow>,
    ) -> Result<Vec<Message<T>>, PgmqError> {
        let messages = rows
            .into_iter()
            .map(|row| Message::<T>::from_row(&row))
            .collect::<Result<Vec<Message<T>>, _>>()?;
        Ok(messages)
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
                Ok(Some(Message::<T>::from_row(&row)?))
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

    pub async fn create_fifo_index_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        sqlx::query("SELECT pgmq.create_fifo_index(queue_name=>$1::text);")
            .bind(queue_name)
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn create_fifo_index(&self, queue_name: &str) -> Result<(), PgmqError> {
        self.create_fifo_index_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn create_fifo_indexes_all_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
    >(
        &self,
        executor: E,
    ) -> Result<(), PgmqError> {
        sqlx::query("SELECT pgmq.create_fifo_indexes_all();")
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn create_fifo_indexes_all(&self) -> Result<(), PgmqError> {
        self.create_fifo_indexes_all_with_cxn(&self.connection)
            .await
    }

    pub async fn bind_topic_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        pattern: &str,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sqlx::query("SELECT pgmq.bind_topic(pattern=>$1::text, queue_name=>$2::text)")
            .bind(pattern)
            .bind(queue_name)
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn bind_topic(&self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        self.bind_topic_with_cxn(pattern, queue_name, &self.connection)
            .await
    }

    pub async fn unbind_topic_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        pattern: &str,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sqlx::query("SELECT pgmq.unbind_topic(pattern=>$1::text, queue_name=>$2::text)")
            .bind(pattern)
            .bind(queue_name)
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn unbind_topic(&self, pattern: &str, queue_name: &str) -> Result<(), PgmqError> {
        self.unbind_topic_with_cxn(pattern, queue_name, &self.connection)
            .await
    }

    pub async fn list_topic_bindings_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let rows = sqlx::query(
            "SELECT pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings(queue_name=>$1::text);",
        )
            .bind(queue_name)
            .fetch_all(executor)
            .await?;

        let rows = rows
            .into_iter()
            .map(|row| ListTopicBindingsRow::from_row(&row))
            .collect::<Result<Vec<ListTopicBindingsRow>, _>>()?;
        Ok(rows)
    }

    pub async fn list_topic_bindings(
        &self,
        queue_name: &str,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        self.list_topic_bindings_with_cxn(queue_name, &self.connection)
            .await
    }

    pub async fn list_topic_bindings_all_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
    >(
        &self,
        executor: E,
    ) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        let rows = sqlx::query(
            "SELECT pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings();",
        )
        .fetch_all(executor)
        .await?;

        let rows = rows
            .into_iter()
            .map(|row| ListTopicBindingsRow::from_row(&row))
            .collect::<Result<Vec<ListTopicBindingsRow>, _>>()?;
        Ok(rows)
    }

    pub async fn list_topic_bindings_all(&self) -> Result<Vec<ListTopicBindingsRow>, PgmqError> {
        self.list_topic_bindings_all_with_cxn(&self.connection)
            .await
    }

    /// Send a message using topic-based routing. Will send the message to every queue that has
    /// a topic binding that matches the given `routing_key`. Returns the number of queues that
    /// the message was sent to.
    pub async fn send_topic_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
        H: Serialize,
    >(
        &self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<i32, PgmqError> {
        let delay: VisibilityTimeoutOffset = delay.into();
        let message = serde_json::to_value(message)?;
        let headers = serde_json::to_value(headers)?;
        let matched_queue_count = sqlx::query_scalar("SELECT * from pgmq.send_topic(routing_key=>$1::text, msg=>$2::jsonb, headers=>$3::jsonb, delay=>$4::int)")
            .bind(routing_key)
            .bind(message)
            .bind(headers)
            .bind(delay)
            .fetch_one(executor)
            .await?;

        Ok(matched_queue_count)
    }

    pub async fn send_topic<T: Serialize, H: Serialize>(
        &self,
        routing_key: &str,
        message: &T,
        headers: Option<&H>,
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<i32, PgmqError> {
        self.send_topic_with_cxn(routing_key, message, headers, delay, &self.connection)
            .await
    }

    /// Send messages using topic-based routing. Will send the messages to every queue that has
    /// a topic binding that matches the given `routing_key`.
    pub async fn send_batch_topic_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
        T: Serialize,
        H: Serialize,
    >(
        &self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset>,
        executor: E,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        let delay: VisibilityTimeoutOffset = delay.into();
        let messages = Self::serialize_list(messages)?;
        let headers = Self::serialize_optional_list(headers)?;
        let sent = sqlx::query(
            "SELECT queue_name, msg_id from pgmq.send_batch_topic(routing_key=>$1::text, msgs=>$2::jsonb[], headers=>$3::jsonb[], delay=>$4::integer);",
        )
            .bind(routing_key)
            .bind(messages)
            .bind(headers)
            .bind(delay)
            .fetch_all(executor)
            .await?;

        let sent = sent
            .into_iter()
            .map(|row| SendBatchTopicRow::from_row(&row))
            .collect::<Result<Vec<SendBatchTopicRow>, _>>()?;
        Ok(sent)
    }

    pub async fn send_batch_topic<T: Serialize, H: Serialize>(
        &self,
        routing_key: &str,
        messages: &[T],
        headers: Option<&[H]>,
        delay: impl Into<VisibilityTimeoutOffset>,
    ) -> Result<Vec<SendBatchTopicRow>, PgmqError> {
        self.send_batch_topic_with_cxn(routing_key, messages, headers, delay, &self.connection)
            .await
    }

    fn serialize_list<T: serde::Serialize>(
        list: &[T],
    ) -> Result<Vec<serde_json::Value>, serde_json::Error> {
        list.iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()
    }

    fn serialize_optional_list<H: serde::Serialize>(
        list: Option<&[H]>,
    ) -> Result<Option<Vec<serde_json::Value>>, serde_json::Error> {
        let headers = if let Some(list) = list {
            Some(Self::serialize_list(list)?)
        } else {
            None
        };
        Ok(headers)
    }

    /// Enable sending a Postgres notification when an item is inserted into the specified queue.
    /// Provide a non-zero throttle interval to specify how often a notification can be sent.
    ///
    /// To actually receive the notification when an item is inserted, use a
    /// [`sqlx::postgres::PgListener`]. This can be created using [`Self::queue_insert_listener`]
    /// (or one of the other similarly-named methods).
    ///
    /// Postgres notifications can be useful for queues that must be acted upon immediately
    /// but rarely have items. However, in most cases, it's recommended to use a polling mechanism
    /// to fetch items from the queue. In fact, because Postgres notifications are transient and
    /// may be missed, it's recommended to also use a polling mechanism as a fallback instead of
    /// relying entirely on notifications.
    pub async fn enable_notify_insert_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let throttle_interval_ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
        sqlx::query("SELECT pgmq.enable_notify_insert(queue_name=>$1::text, throttle_interval_ms=>$2::integer)")
            .bind(queue_name)
            .bind(throttle_interval_ms)
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn enable_notify_insert(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        self.enable_notify_insert_with_cxn(queue_name, throttle_interval, &self.connection)
            .await
    }

    /// Disable sending insert notifications for the specified queue.
    pub async fn disable_notify_insert_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        sqlx::query("SELECT pgmq.disable_notify_insert(queue_name=>$1::text)")
            .bind(queue_name)
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn disable_notify_insert(&self, queue_name: &str) -> Result<(), PgmqError> {
        self.disable_notify_insert_with_cxn(queue_name, &self.connection)
            .await
    }

    /// Update the throttle interval for Postgres notifications sent for the specified queue.
    pub async fn update_notify_insert_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
        executor: E,
    ) -> Result<(), PgmqError> {
        check_input(queue_name)?;
        let throttle_interval_ms = i32::try_from(throttle_interval.as_millis()).unwrap_or(i32::MAX);
        sqlx::query("SELECT pgmq.update_notify_insert(queue_name=>$1::text, throttle_interval_ms=>$2::integer)")
            .bind(queue_name)
            .bind(throttle_interval_ms)
            .execute(executor)
            .await?;
        Ok(())
    }

    pub async fn update_notify_insert(
        &self,
        queue_name: &str,
        throttle_interval: std::time::Duration,
    ) -> Result<(), PgmqError> {
        self.update_notify_insert_with_cxn(queue_name, throttle_interval, &self.connection)
            .await
    }

    pub async fn list_notify_insert_throttles_with_cxn<
        'c,
        E: sqlx::Executor<'c, Database = Postgres>,
    >(
        &self,
        executor: E,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        let rows: Vec<ListNotifyInsertThrottlesRow> = sqlx::query_as("SELECT queue_name, throttle_interval_ms, last_notified_at FROM pgmq.list_notify_insert_throttles()")
            .fetch_all(executor)
            .await?;
        Ok(rows)
    }

    pub async fn list_notify_insert_throttles(
        &self,
    ) -> Result<Vec<ListNotifyInsertThrottlesRow>, PgmqError> {
        self.list_notify_insert_throttles_with_cxn(&self.connection)
            .await
    }

    /// Create a [`sqlx::postgres::PgListener`] that will listen to insert notifications for the
    /// specified queue. The listener can be configured to listen to more queues by calling
    /// [`sqlx::postgres::PgListener::listen`] with the channel name returned by
    /// [`queue_name_to_insert_notification_channel_name`].
    ///
    /// Note: The listener will hold a connection from the pool for as long as it is in scope.
    pub async fn queue_insert_listener_with_pool(
        &self,
        queue_name: &str,
        pool: &Pool<Postgres>,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        listener
            .listen(&queue_name_to_insert_notification_channel_name(queue_name))
            .await?;
        Ok(listener)
    }

    pub async fn queue_insert_listener(
        &self,
        queue_name: &str,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        self.queue_insert_listener_with_pool(queue_name, &self.connection)
            .await
    }

    /// Create a [`sqlx::postgres::PgListener`] that will listen to insert notifications for all the
    /// specified queues. The listener can be configured to listen to more queues by calling
    /// [`sqlx::postgres::PgListener::listen`] with the channel name returned by
    /// [`queue_name_to_insert_notification_channel_name`].
    ///
    /// Note: The listener will hold a connection from the pool for as long as it is in scope.
    pub async fn queue_insert_listener_all_with_pool(
        &self,
        queue_names: impl IntoIterator<Item = &str>,
        pool: &Pool<Postgres>,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        let channel_names = queue_names
            .into_iter()
            .map(queue_name_to_insert_notification_channel_name)
            .collect::<Vec<_>>();
        listener
            .listen_all(channel_names.iter().map(|s| s.as_str()))
            .await?;
        Ok(listener)
    }

    pub async fn queue_insert_listener_all(
        &self,
        queue_names: impl IntoIterator<Item = &str>,
    ) -> Result<sqlx::postgres::PgListener, PgmqError> {
        self.queue_insert_listener_all_with_pool(queue_names, &self.connection)
            .await
    }

    pub async fn metrics_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        queue_name: &str,
        executor: E,
    ) -> Result<QueueMetrics, PgmqError> {
        check_input(queue_name)?;
        let metrics: QueueMetrics = sqlx::query_as("SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length FROM pgmq.metrics(queue_name=>$1::text)")
            .bind(queue_name)
            .fetch_one(executor)
            .await?;
        Ok(metrics)
    }

    pub async fn metrics(&self, queue_name: &str) -> Result<QueueMetrics, PgmqError> {
        self.metrics_with_cxn(queue_name, &self.connection).await
    }

    pub async fn metrics_all_with_cxn<'c, E: sqlx::Executor<'c, Database = Postgres>>(
        &self,
        executor: E,
    ) -> Result<Vec<QueueMetrics>, PgmqError> {
        let metrics: Vec<QueueMetrics> = sqlx::query_as("SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time, queue_visible_length FROM pgmq.metrics_all()")
            .fetch_all(executor)
            .await?;
        Ok(metrics)
    }

    pub async fn metrics_all(&self) -> Result<Vec<QueueMetrics>, PgmqError> {
        self.metrics_all_with_cxn(&self.connection).await
    }
}

/// Translate the given queue name into the name of the Postgres notification channel that will
/// be triggered when using the [`PGMQueueExt::enable_notify_insert`] functionality. This method
/// is called internally by the `PGMQueueExt::queue_insert_listener*` methods.
///
/// This method is useful in order to tell a [`sqlx::postgres::PgListener`] to stop listening
/// to notifications for a specific queue using [`sqlx::postgres::PgListener::unlisten`].
///
/// # Examples
/// ```
/// # use pgmq::pg_ext::queue_name_to_insert_notification_channel_name;
/// let channel_name = queue_name_to_insert_notification_channel_name("test");
/// assert_eq!("pgmq.q_test.INSERT", channel_name);
/// ```
pub fn queue_name_to_insert_notification_channel_name(queue_name: &str) -> String {
    format!("pgmq.q_{queue_name}.INSERT")
}
