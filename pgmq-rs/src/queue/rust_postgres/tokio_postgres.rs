use crate::queue::macros::{identity_macro, impl_queue, transform_result_async};
use crate::queue::rust_postgres::rust_postgres_functions;

impl_queue!(&tokio_postgres::Client, identity_macro);
impl_queue!(&tokio_postgres::Transaction<'_>, identity_macro);

macro_rules! immutable_ref {
    ($type_:ty) => {
        &$type_
    };
}

rust_postgres_functions!(
    tokio_postgres::GenericClient,
    immutable_ref,
    transform_result_async
);
