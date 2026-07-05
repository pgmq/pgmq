//! Macros to help with implementing the [`crate::queue::Queue`] trait.

/// "Identity" transformation macro -- simply returns the provided input.
macro_rules! identity_macro {
    ($input:tt) => {
        $input
    };
}
// Re-export the macro for use within this crate
pub(crate) use identity_macro;

/// Helper macro to implement the [`crate::queue::Queue`] trait for a type. Assumes that async
/// functions exist in scope with the same names as the trait's methods. The functions' parameters
/// should match the trait's parameters as well, with the following exceptions:
///
/// 1. Concrete [`crate::types::QueueName`] and
///    [`crate::types::VisibilityTimeoutOffset`] instances are passed
///    instead of a generic parameter (the generic parameter has already been converted)
/// 2. Serializable parameters (e.g., message and headers) are passed as pre-serialized
///    [`serde_json::Value`] instances.
/*
Note: Ideally we would be able to implement `Queue` using blanket implementations, e.g.:

```rust
impl<T> Queue for T where T: for<'c> sqlx::Executor<'c, Database = sqlx::Postgres> {
    ...
}

impl<T> Queue for T where T: diesel_async::AsyncConnection<Backend = diesel::pg::Pg> {
    ...
}
```

This works if we only have a single blanket implementation. However, since we want to support
multiple external traits' executor types, we would need multiple blanket implementations, which
results in a "conflicting implementations" compilation error. This is the case even if we add a
"sealed" trait for each external crate's blanket implementation, e.g., something like this:

```rust
mod sqlx_impl {
    trait SqlxSealed {}
    impl SqlxSealed for &mut sqlx::PgConnection {}
    impl<T> Queue for T where T: SqlxSealed + for<'c> sqlx::Executor<'c, Database = sqlx::Postgres> {
        ...
    }
}

mod diesel_impl {
    trait DieselSealed {}
    impl DieselSealed for &mut diesel::PgConnection {}
    impl<T> Queue for T where
        T: DieselSealed + diesel::connection::LoadConnection<Backend = diesel::pg::Pg>
    {
        ...
    }
}
```

So, instead of using a blanket implementation, the `impl_queue` macro can be used to reduce the
boilerplate / code duplication required to implement the `Queue` trait for all the external
types we want to support.
 */
macro_rules! impl_queue {
    (
        /// The type to implement the `Queue` trait for.
        $for_type:ty,
        /// Expression used to transform `self` into an implementation-specific executor type.
        $transform_self:tt
    ) => {
        impl crate::private::Sealed for $for_type {}

        #[async_trait::async_trait]
        impl crate::queue::Queue for $for_type {
            async fn create<'q, Q, QE>(self, queue_name: Q) -> Result<(), crate::errors::PgmqError>
            where
                Q: Send + TryInto<crate::types::QueueName<'q>, Error = QE>,
                QE: ToString,
            {
                let queue_name = queue_name
                    .try_into()
                    .map_err(crate::types::queue_name::QueueNameError::other)?;
                create($transform_self!(self), queue_name).await
            }

            async fn send<'q, T, H, Q, QE, D>(
                self,
                queue_name: Q,
                message: T,
                headers: H,
                delay: D,
            ) -> Result<i64, crate::errors::PgmqError>
            where
                T: Send + serde::Serialize,
                H: Send + serde::Serialize,
                Q: Send + TryInto<crate::types::QueueName<'q>, Error = QE>,
                QE: ToString,
                D: Send + Into<crate::types::VisibilityTimeoutOffset>,
            {
                let queue_name = queue_name
                    .try_into()
                    .map_err(crate::types::queue_name::QueueNameError::other)?;
                let delay: crate::types::VisibilityTimeoutOffset = delay.into();
                let message = serde_json::to_value(message)?;
                let headers = serde_json::to_value(headers)?;
                send($transform_self!(self), queue_name, message, headers, delay).await
            }

            async fn read<'q, T, Q, QE, VT>(
                self,
                queue_name: Q,
                visibility_timeout: VT,
                quantity: i32,
            ) -> Result<Vec<crate::types::Message<T>>, crate::errors::PgmqError>
            where
                T: 'static + Send + for<'de> serde::Deserialize<'de>,
                Q: Send + TryInto<crate::types::QueueName<'q>, Error = QE>,
                QE: ToString,
                VT: Send + Into<crate::types::VisibilityTimeoutOffset>,
            {
                let queue_name = queue_name
                    .try_into()
                    .map_err(crate::types::queue_name::QueueNameError::other)?;
                let visibility_timeout: crate::types::VisibilityTimeoutOffset =
                    visibility_timeout.into();
                read(
                    $transform_self!(self),
                    queue_name,
                    visibility_timeout,
                    quantity,
                )
                .await
            }
        }
    };
}
// Re-export the macro for use within this crate
pub(crate) use impl_queue;
