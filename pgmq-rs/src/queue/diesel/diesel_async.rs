use crate::queue::diesel::diesel_functions;
use crate::queue::macros::{
    identity_macro, impl_queue, impl_queue_transaction, transform_result_async,
};

diesel_functions!(
    diesel_async::AsyncConnection<Backend = diesel::pg::Pg>,
    diesel_async::RunQueryDsl,
    transform_result_async
);

// `diesel`/`diesel-async` don't have a type specific to transactions. Instead, transactions
// are performed in a callback provided to a `transaction` method, where the callback gets a
// reference to the connection. So, we need to implement `QueueTransaction` for the connection type.
impl_queue_transaction!(&mut diesel_async::AsyncPgConnection, identity_macro);

#[cfg(feature = "diesel-async-pool")]
macro_rules! transform_self_acquire_connection_async {
    ($self:ident) => {
        &mut ($self.get().await?)
    };
}

#[cfg(feature = "diesel-async-pool")]
impl_queue!(
    &mut bb8::PooledConnection<
        '_,
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    identity_macro
);

#[cfg(feature = "diesel-async-pool")]
impl_queue!(
    &diesel_async::pooled_connection::bb8::Pool<diesel_async::AsyncPgConnection>,
    transform_self_acquire_connection_async
);
