//! (Unstable) Common interface shared between various SQL client implementations (sqlx, diesel, rust-postgres).
//! This interface is considered unstable -- breaking changes may be released without a corresponding
//! SemVer bump.

mod macros;
#[cfg(feature = "sqlx")]
pub mod sqlx;

use crate::types::{QueueName, VisibilityTimeoutOffset};
use crate::PgmqError;

/// Sealed so we can add methods without breaking semver compatibility.
/// See: <https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed>
#[async_trait::async_trait]
#[allow(private_bounds)]
pub trait Queue: crate::private::Sealed {
    async fn create<'q, Q, QE>(self, queue_name: Q) -> Result<(), PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: ToString;

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
        QE: ToString,
        D: Send + Into<VisibilityTimeoutOffset>;

    async fn read<'q, T, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<crate::types::Message<T>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: ToString,
        VT: Send + Into<VisibilityTimeoutOffset>;
}
