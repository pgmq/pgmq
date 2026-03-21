use sqlx::{Database, Encode, Type};
use std::ops::Deref;

/// Type to represent an offset that will be applied to the current timestamp in order to update
/// the `vt` (visibility timeout) timestamp column of a queue message. Used by various
/// methods in [`crate::pg_ext::PGMQueueExt`] to set the visibility timeout of a job. Supports
/// converting from [`chrono::Duration`], [`std::time::Duration`], and various integer types
/// (assumed to be a duration in seconds).
///
/// Note: The offset has 1 second precision and is stored as an [`i32`]. This limits the possible
/// range of values compared to what's technically supported by Postgres. However, this ensures
/// that the value provided to Postgres will not overflow, and the maximum [`i32`] value in seconds
/// is roughly 68 years, which should be plenty large for virtually any use case. If any conversion
/// to [`i32`] would result in an overflow, the value is instead capped to
/// [`i32::MIN`]/[`i32::MAX`].
///
/// # Examples
///
/// ## Convert from `i32`
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(10i32, *VisibilityTimeoutOffset::from(10i32));
/// ```
///
/// ## Convert from `u32`
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(10i32, *VisibilityTimeoutOffset::from(10u32));
/// ```
///
/// ## Convert from `u32` capped
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(i32::MAX, *VisibilityTimeoutOffset::from(u32::MAX));
/// ```
///
/// ## Convert from `i64`
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(10i32, *VisibilityTimeoutOffset::from(10i64));
/// ```
///
/// ## Convert from `i64` capped max
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(i32::MAX, *VisibilityTimeoutOffset::from(i64::MAX));
/// ```
///
/// ## Convert from `i64` capped min
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(i32::MIN, *VisibilityTimeoutOffset::from(i64::MIN));
/// ```
///
/// ## Convert from [`chrono::Duration`]`
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(10i32, *VisibilityTimeoutOffset::from(chrono::Duration::seconds(10)));
/// ```
///
/// ## Convert from [`chrono::Duration`]` capped max
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(VisibilityTimeoutOffset::MAX, VisibilityTimeoutOffset::from(chrono::Duration::MAX));
/// ```
///
/// ## Convert from [`chrono::Duration`]` capped min
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(VisibilityTimeoutOffset::MAX, VisibilityTimeoutOffset::from(chrono::Duration::MAX));
/// ```
///
/// ## Convert from [`std::time::Duration`]`
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(10i32, *VisibilityTimeoutOffset::from(std::time::Duration::from_secs(10)));
/// ```
///
/// ## Convert from [`std::time::Duration`]` capped max
/// ```
/// # use pgmq::pg_ext::VisibilityTimeoutOffset;
/// assert_eq!(VisibilityTimeoutOffset::MAX, VisibilityTimeoutOffset::from(std::time::Duration::MAX));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Encode)]
pub struct VisibilityTimeoutOffset(i32);

impl VisibilityTimeoutOffset {
    pub const MIN: Self = Self(i32::MIN);
    pub const MAX: Self = Self(i32::MAX);

    pub fn seconds(seconds: i32) -> Self {
        Self(seconds)
    }

    pub fn as_seconds(&self) -> i32 {
        self.0
    }
}

impl AsRef<i32> for VisibilityTimeoutOffset {
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

impl Deref for VisibilityTimeoutOffset {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<i32> for VisibilityTimeoutOffset {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<u32> for VisibilityTimeoutOffset {
    fn from(value: u32) -> Self {
        Self(i32::try_from(value).unwrap_or(i32::MAX))
    }
}

impl From<i64> for VisibilityTimeoutOffset {
    fn from(value: i64) -> Self {
        Self(i32::try_from(value).unwrap_or_else(|_| {
            if value.is_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        }))
    }
}

impl From<u64> for VisibilityTimeoutOffset {
    fn from(value: u64) -> Self {
        Self(i32::try_from(value).unwrap_or(i32::MAX))
    }
}

impl From<chrono::Duration> for VisibilityTimeoutOffset {
    fn from(value: chrono::Duration) -> Self {
        value.num_seconds().into()
    }
}

impl From<std::time::Duration> for VisibilityTimeoutOffset {
    fn from(value: std::time::Duration) -> Self {
        value.as_secs().into()
    }
}

/*
Manually implement `sqlx::Type` because the derive macro automatically implements both
`sqlx::Encode` and `sqlx::Decode`, but we only need `sqlx::Encode` for this type.
However, `sqlx::Encode` is implemented via a derive macro.
*/
impl<DB: Database> Type<DB> for VisibilityTimeoutOffset
where
    i32: Type<DB>,
    std::time::Duration: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <i32 as sqlx::Type<DB>>::type_info()
    }
}
