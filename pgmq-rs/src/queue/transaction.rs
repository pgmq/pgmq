use crate::types::QueueName;
use crate::PgmqError;

/// Interface that provides methods for invoking PGMQ SQL functions that only make sense in the
/// context of a transaction. For example, `pgmq.acquire_queue_lock` acquires a transaction-level
/// advisory lock, so if it's not called inside an active transaction, it will have no effect (the
/// lock will be released before the subsequent statements are executed).
#[async_trait::async_trait]
#[allow(private_bounds)]
pub trait QueueTransaction: crate::queue::Queue {
    /// Acquire a transaction-level advisory lock specific to the provided queue. Useful to prevent
    /// race conditions when performing queue/table-level operations, such as creating an index
    /// for the queue (e.g., with [`crate::queue::Queue::create_fifo_index`]).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "queue-experimental")]
    /// # async fn example(transaction: impl pgmq::queue::QueueTransaction) -> Result<(), pgmq::PgmqError> {
    /// transaction.acquire_queue_lock("my_queue").await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn acquire_queue_lock<'q, Q, QE>(self, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: Into<crate::types::queue_name::QueueNameError>;
}
