use crate::queue::diesel::diesel_functions;
use crate::queue::macros::{identity_macro, impl_queue, impl_queue_transaction};

diesel_functions!(
    diesel::connection::LoadConnection<Backend = diesel::pg::Pg>,
    diesel::RunQueryDsl,
    identity_macro
);

// `diesel`/`diesel-async` don't have a type specific to transactions. Instead, transactions
// are performed in a callback provided to a `transaction` method, where the callback gets a
// reference to the connection. So, we need to implement `QueueTransaction` for the connection type.
impl_queue_transaction!(&mut diesel::PgConnection, identity_macro);

#[cfg(feature = "diesel-sync-pool")]
macro_rules! transform_self_acquire_connection {
    ($self:ident) => {
        &mut ($self.get()?)
    };
}

#[cfg(feature = "diesel-sync-pool")]
impl_queue!(
    &mut r2d2::PooledConnection<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    identity_macro
);

#[cfg(feature = "diesel-sync-pool")]
impl_queue!(
    &r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>>,
    transform_self_acquire_connection
);
