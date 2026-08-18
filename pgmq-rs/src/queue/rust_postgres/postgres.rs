use crate::queue::macros::{identity_macro, impl_queue};
use crate::queue::rust_postgres::rust_postgres_functions;

impl_queue!(&mut postgres::Client, identity_macro);
impl_queue!(&mut postgres::Transaction<'_>, identity_macro);

macro_rules! mutable_ref {
    ($type_:ty) => {
        &mut $type_
    };
}

rust_postgres_functions!(postgres::GenericClient, mutable_ref, identity_macro);
