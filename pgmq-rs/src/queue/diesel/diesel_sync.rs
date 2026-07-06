use crate::queue::diesel::diesel_functions;
use crate::queue::macros::{identity_macro, impl_queue};

diesel_functions!(
    diesel::connection::LoadConnection<Backend = diesel::pg::Pg>,
    diesel::RunQueryDsl,
    identity_macro
);

impl_queue!(&mut diesel::PgConnection, identity_macro);

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
