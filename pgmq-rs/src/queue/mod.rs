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

    async fn read<'q, T, H, Q, QE, VT>(
        self,
        queue_name: Q,
        visibility_timeout: VT,
        quantity: i32,
    ) -> Result<Vec<crate::types::Message<T, H>>, PgmqError>
    where
        T: 'static + Send + for<'de> serde::Deserialize<'de>,
        H: 'static + Send + for<'de> serde::Deserialize<'de>,
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: ToString,
        VT: Send + Into<VisibilityTimeoutOffset>;

    async fn archive<'q, Q, QE>(
        self,
        queue_name: Q,
        msg_ids: &[i64],
    ) -> Result<Vec<i64>, PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: ToString;

    async fn delete<'q, Q, QE>(self, queue_name: Q, msg_ids: &[i64]) -> Result<Vec<i64>, PgmqError>
    where
        Q: Send + TryInto<QueueName<'q>, Error = QE>,
        QE: ToString;
}
