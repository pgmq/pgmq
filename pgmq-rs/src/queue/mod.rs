//! (Unstable) Common interface shared between various SQL client implementations (sqlx, diesel, rust-postgres).
//! This interface is considered unstable -- breaking changes may be released without a corresponding
//! SemVer bump.

#[cfg(feature = "diesel")]
pub mod diesel;
mod macros;
#[cfg(any(feature = "rust-postgres", feature = "tokio-postgres"))]
pub mod rust_postgres;
#[cfg(any(
    feature = "sqlx",
    feature = "rust-postgres",
    feature = "tokio-postgres"
))]
pub(crate) mod sql;
#[cfg(feature = "sqlx")]
pub mod sqlx;

use crate::types::{QueueName, VisibilityTimeoutOffset};
use crate::{Message, PgmqError};

/// Interface that provides methods for invoking PGMQ SQL functions.
///
/// For variadic functions, only a single method with all available parameters is provided.
/// For example, only `pgmq.send(queue_name, msg, headers, delay)` is supported instead of all
/// the other variations of the `pgmq.send` function.
// Sealed so we can add methods without breaking semver compatibility.
// See: <https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed>
#[async_trait::async_trait]
#[allow(private_bounds)]
pub trait Queue: crate::private::Sealed {
    /// Create the SQL tables for the specified queue.
    ///
    /// Invokes the `pgmq.create` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// queue.create("my_queue").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create<'q, Q, QE>(self, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Enqueue a message for the specified queue.
    ///
    /// Invokes the `pgmq.send` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// // Send an owned message with no headers and no delay
    /// let msg = serde_json::json!({"a": 1234});
    /// queue.send("my_queue", msg, (), 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// // Send a message reference with headers and a delay
    /// let msg = serde_json::json!({"a": 1234});
    /// queue.send("my_queue", &msg, serde_json::json!({"headerA": 5678}), 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn send<'q, T, H, Q, QE, D>(
        self,
        queue_name: Q,
        message: T,
        headers: H,
        delay: D,
    ) -> Result<i64, PgmqError>
    where
        T: Send + serde::Serialize,
        H: Send + serde::Serialize,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        D: Send + Into<VisibilityTimeoutOffset>;

    /// Enqueue several messages for the specified queue.
    ///
    /// Invokes the `pgmq.send_batch` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::types::EMPTY_HEADERS;
    /// // Send owned messages with no headers and no delay
    /// let msgs = [serde_json::json!({"a": 1}), serde_json::json!({"a": 2})];
    /// queue.send_batch("my_queue", msgs, EMPTY_HEADERS, 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// // Send a slice of messages with headers and a delay
    /// let msgs = [serde_json::json!({"a": 1}), serde_json::json!({"a": 2})];
    /// let headers = [serde_json::json!({"headerA": 3}), serde_json::json!({"headerA": 4})];
    /// queue.send_batch("my_queue", &msgs, Some(&headers), Duration::from_secs(10)).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn send_batch<'q, T, H, TI, HI, Q, QE, D>(
        self,
        queue_name: Q,
        messages: TI,
        headers: Option<HI>,
        delay: D,
    ) -> Result<Vec<i64>, PgmqError>
    where
        T: serde::Serialize,
        H: serde::Serialize,
        TI: Send + IntoIterator<Item = T>,
        HI: Send + IntoIterator<Item = H>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        D: Send + Into<VisibilityTimeoutOffset>;

    /// Read at most `quantity` messages from the queue with the provided `queue_name`. If no
    /// messages are available, an empty [`Vec`] will be returned.
    ///
    /// Invokes the `pgmq.read` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// // Read a message, deserializing as a `serde_json::Value`, using an integer to update
    /// // the visibility timeout (`vt`)
    /// let msgs: Vec<Message> = queue.read("my_queue", 10, 1).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use serde_derive::Deserialize;
    /// # use pgmq::{Message, PgmqError};
    /// #[derive(Deserialize)]
    /// struct MyMessage {
    ///     a: String
    /// }
    /// // Read multiple messages, deserializing as a custom message struct, `MyMessage`, using
    /// // a `Duration` to update the visibility timeout (`vt`)
    /// let msgs: Vec<Message<MyMessage>> = queue.read("my_queue", Duration::from_secs(10), 2).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn read<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        VT: Send + Into<VisibilityTimeoutOffset>;

    /// Pop at most `quantity` messages from the queue with the provided `queue_name`. If no
    /// messages are available, an empty [`Vec`] will be returned. The popped messages are deleted
    /// from the queue -- this is the equivalent of [`Self::read`] + [`Self::delete`] within a
    /// single command.
    ///
    /// Invokes the `pgmq.pop` SQL function.
    ///
    /// Take care when using this function -- if your application crashes while processing a
    /// message and does not add the message back to the queue, the message may never be processed
    /// because it was deleted.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// // Pop a message, deserializing as a `serde_json::Value`
    /// let msgs: Vec<Message> = queue.pop("my_queue", 1).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use serde_derive::Deserialize;
    /// # use pgmq::{Message, PgmqError};
    /// #[derive(Deserialize)]
    /// struct MyMessage {
    ///     a: String
    /// }
    /// // Pop multiple messages, deserializing as a custom message struct, `MyMessage`.
    /// let msgs: Vec<Message<MyMessage>> = queue.pop("my_queue", 2).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn pop<'q, T, H, Q, QE>(
        self,
        queue_name: Q,
        quantity: i32,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Mark the specified messages as archived. Moves the messages to the archive table for the
    /// specified queue. Returns the IDs of the messages that were successfully archived. This may
    /// differ from the IDs provided to this method, e.g., if a message was previously archived or
    /// deleted, it won't be present in the returned list.
    ///
    /// Invokes the `pgmq.archive` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::types::EMPTY_HEADERS;
    /// let msg_ids = [1234];
    /// queue.archive("my_queue", &msg_ids).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn archive<'q, Q, QE>(
        self,
        queue_name: Q,
        msg_ids: &[i64],
    ) -> Result<Vec<i64>, PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Delete the specified messages from the database. Returns the IDs of the messages that were
    /// successfully deleted. This may differ from the IDs provided to this method, e.g., if a
    /// message was previously archived or deleted, it won't be present in the returned list.
    ///
    /// Invokes the `pgmq.delete` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::types::EMPTY_HEADERS;
    /// let msg_ids = [1234];
    /// queue.delete("my_queue", &msg_ids).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn delete<'q, Q, QE>(self, queue_name: Q, msg_ids: &[i64]) -> Result<Vec<i64>, PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Update the visibility timeout (`vt`) of the specified messages. Returns the messages
    /// that were successfully updated. This may differ from the IDs provided to this method,
    /// e.g., if a message was previously archived or deleted, it won't be present in the returned
    /// list.
    ///
    /// The provided `vt` value is a duration offset with a unit of seconds. The offset will be
    /// added to the current timestamp when the command reaches the DB.
    ///
    /// Invokes the `pgmq.set_vt` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// # use pgmq::types::EMPTY_HEADERS;
    /// let msg_ids = [1234];
    /// // Update the visibility timeout (`vt`) using an integer offset
    /// let _: Vec<Message> = queue.set_vt("my_queue", &msg_ids, 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use pgmq::{Message, PgmqError};
    /// # use pgmq::types::EMPTY_HEADERS;
    /// let msg_ids = [1234];
    /// // Update the visibility timeout (`vt`) using a `Duration` offset
    /// let _: Vec<Message> = queue.set_vt("my_queue", &msg_ids, Duration::from_secs(10)).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn set_vt<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        msg_ids: &[i64],
        visibility_timeout: VT,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        VT: Send + Into<VisibilityTimeoutOffset>;

    /// Create an index on the `headers` column of the queue to improve FIFO read performance.
    ///
    /// Invokes the `pgmq.create_fifo_index` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// queue.create_fifo_index("my_queue").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_fifo_index<'q, Q, QE>(self, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Create an index on the `headers` column of all queues to improve FIFO read performance.
    ///
    /// Invokes the `pgmq.create_fifo_indexes_all` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// queue.create_fifo_indexes_all().await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn create_fifo_indexes_all(self) -> Result<(), PgmqError>;

    /// Reads messages with AWS SQS FIFO-style batch retrieval behavior. Returns at most `quantity`
    /// messages from the same FIFO group from the queue with the provided `queue_name`. If no
    /// messages are available, an empty [`Vec`] will be returned.
    ///
    /// Invokes the `pgmq.read_grouped` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// // Read a message, deserializing as a `serde_json::Value`, using an integer to update
    /// // the visibility timeout (`vt`)
    /// let msgs: Vec<Message> = queue.read_grouped("my_queue", 10, 1).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use serde_derive::Deserialize;
    /// # use pgmq::{Message, PgmqError};
    /// #[derive(Deserialize)]
    /// struct MyMessage {
    ///     a: String
    /// }
    /// // Read multiple messages, deserializing as a custom message struct, `MyMessage`, using
    /// // a `Duration` to update the visibility timeout (`vt`)
    /// let msgs: Vec<Message<MyMessage>> = queue.read_grouped("my_queue", Duration::from_secs(10), 2).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn read_grouped<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        VT: Send + Into<VisibilityTimeoutOffset>;

    /// Read the head of at most `quantity` FIFO groups from the queue with the provided
    /// `queue_name`. This supports horizontal scaling by processing groups in parallel while
    /// ensuring message ordering is preserved per group. If no messages are available, an
    /// empty [`Vec`] will be returned.
    ///
    /// Invokes the `pgmq.read_grouped_head` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// // Read a message, deserializing as a `serde_json::Value`, using an integer to update
    /// // the visibility timeout (`vt`)
    /// let msgs: Vec<Message> = queue.read_grouped_head("my_queue", 10, 1).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use serde_derive::Deserialize;
    /// # use pgmq::{Message, PgmqError};
    /// #[derive(Deserialize)]
    /// struct MyMessage {
    ///     a: String
    /// }
    /// // Read multiple messages, deserializing as a custom message struct, `MyMessage`, using
    /// // a `Duration` to update the visibility timeout (`vt`)
    /// let msgs: Vec<Message<MyMessage>> = queue.read_grouped_head("my_queue", Duration::from_secs(10), 2).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn read_grouped_head<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        VT: Send + Into<VisibilityTimeoutOffset>;

    /// Read at most `quantity` messages from the queue with the provided `queue_name`. Preserves
    /// FIFO order within groups and interleaves across groups (layered round-robin). If no
    /// messages are available, an empty [`Vec`] will be returned.
    ///
    /// Invokes the `pgmq.read_grouped_rr` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::{Message, PgmqError};
    /// // Read a message, deserializing as a `serde_json::Value`, using an integer to update
    /// // the visibility timeout (`vt`)
    /// let msgs: Vec<Message> = queue.read_grouped_rr("my_queue", 10, 1).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// # use serde_derive::Deserialize;
    /// # use pgmq::{Message, PgmqError};
    /// #[derive(Deserialize)]
    /// struct MyMessage {
    ///     a: String
    /// }
    /// // Read multiple messages, deserializing as a custom message struct, `MyMessage`, using
    /// // a `Duration` to update the visibility timeout (`vt`)
    /// let msgs: Vec<Message<MyMessage>> = queue.read_grouped_rr("my_queue", Duration::from_secs(10), 2).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn read_grouped_rr<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>,
        VT: Send + Into<VisibilityTimeoutOffset>;

    /// Bind a topic pattern to a queue. Messages matching the pattern will be routed to this queue when
    /// they're sent with [`Self::send_topic`] or [`Self::send_batch_topic`].
    ///
    /// Invokes the `pgmq.bind_topic` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// queue.bind_topic("topic.*", "my_queue").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn bind_topic<'q, Q, QE>(self, pattern: &str, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Remove the topic pattern binding from the queue.
    ///
    /// Invokes the `pgmq.unbind_topic` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// queue.unbind_topic("topic.*", "my_queue").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn unbind_topic<'q, Q, QE>(self, pattern: &str, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Returns all topic bindings for the specified `queue_name`.
    ///
    /// Invokes the `pgmq.list_topic_bindings` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// let topic_bindings = queue.list_topic_bindings("my_queue").await?;
    /// println!("{topic_bindings:?}");
    /// # Ok(())
    /// # }
    /// ```
    async fn list_topic_bindings<'q, Q, QE>(
        self,
        queue_name: Q,
    ) -> Result<Vec<crate::types::ListTopicBindingsRow>, PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;

    /// Returns all topic bindings across all queues.
    ///
    /// Invokes the `pgmq.list_topic_bindings` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// let topic_bindings = queue.list_topic_bindings_all().await?;
    /// println!("{topic_bindings:?}");
    /// # Ok(())
    /// # }
    /// ```
    async fn list_topic_bindings_all(
        self,
    ) -> Result<Vec<crate::types::ListTopicBindingsRow>, PgmqError>;

    /// Send a message using topic-based routing. Will send the message to every queue that has
    /// a topic binding that matches the given `routing_key`. Returns the number of queues that
    /// the message was sent to. If the message ID and/or queues the message was sent to are needed,
    /// use [`Self::send_batch_topic`] instead.
    ///
    /// Invokes the `pgmq.send_topic` SQL function.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// // Send an owned message with no headers and no delay
    /// let msg = serde_json::json!({"a": 1234});
    /// queue.send_topic("topic", msg, (), 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// // Send a message reference with headers and a delay
    /// let msg = serde_json::json!({"a": 1234});
    /// queue.send_topic("topic", &msg, serde_json::json!({"headerA": 5678}), 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn send_topic<T, H, D>(
        self,
        routing_key: &str,
        message: T,
        headers: H,
        delay: D,
    ) -> Result<i32, PgmqError>
    where
        T: Send + serde::Serialize,
        H: Send + serde::Serialize,
        D: Send + Into<VisibilityTimeoutOffset>;

    /// Send multiple messages using topic-based routing. Will send the messages to every queue
    /// that has a topic binding that matches the given `routing_key`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use pgmq::types::EMPTY_HEADERS;
    /// // Send owned messages with no headers and no delay
    /// let msgs = [serde_json::json!({"a": 1}), serde_json::json!({"a": 2})];
    /// queue.send_batch_topic("topic", msgs, EMPTY_HEADERS, 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(queue: impl pgmq::queue::Queue) -> Result<(), pgmq::PgmqError> {
    /// # use std::time::Duration;
    /// // Send a slice of messages with headers and a delay
    /// let msgs = [serde_json::json!({"a": 1}), serde_json::json!({"a": 2})];
    /// let headers = [serde_json::json!({"headerA": 3}), serde_json::json!({"headerA": 4})];
    /// queue.send_batch_topic("topic", &msgs, Some(&headers), Duration::from_secs(10)).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn send_batch_topic<'q, T, H, TI, HI, D>(
        self,
        routing_key: &str,
        messages: TI,
        headers: Option<HI>,
        delay: D,
    ) -> Result<Vec<crate::types::SendBatchTopicRow>, PgmqError>
    where
        T: serde::Serialize,
        H: serde::Serialize,
        TI: Send + IntoIterator<Item = T>,
        HI: Send + IntoIterator<Item = H>,
        D: Send + Into<VisibilityTimeoutOffset>;
}
