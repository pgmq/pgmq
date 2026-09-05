use crate::private::Sealed;
use std::marker::PhantomData;
use std::ops::Deref;

/// Custom type to represent a duration with the specified [`Unit`]. Used to allow providing
/// duration values as plain integers ([`i32`], [`i64`], etc) or as [`std::time::Duration`]
/// or [`chrono::Duration`] values. When converting from a plain integer, it's assumed that the
/// integer is in the [`Unit`] specified by the [`Duration`]. When converting from
/// [`std::time::Duration`]/[`chrono::Duration`], the value is automatically converted to the
/// correct [`Unit`].
///
/// Provided values are mapped to an [`i32`] value because the PGMQ SQL functions expect integer
/// values for their duration parameters, which corresponds to [`i32`] in Rust.
///
/// Note how overflows: If converting to [`i32`] (e.g., from an [`i64`]) would result in an overflow,
/// the value is capped at [`i32::MAX`] ([`Duration::MAX`]) or [`i32::MIN`] ([`Duration::MIN`]). The
/// maximum [`i32`] value should be plenty large for virtually any PGMQ use case (68 years for
/// seconds, 24 days for milliseconds).
///
/// Negative values are allowed; however, negative values are not particularly useful for any
/// PGMQ use case.
///
/// Because the value contained in the [`Duration`] has already been converted to the correct units,
/// it can be directly used in SQL queries by dereferencing the [`Duration`] (via the [`Deref`]
/// implementation).
///
/// # Examples
///
/// ## Convert from `i32`
/// ```
/// # use pgmq::types::duration::{Duration, Seconds, Milliseconds};
/// assert_eq!(10i32, *Duration::<Seconds>::from(10i32));
/// assert_eq!(10i32, *Duration::<Milliseconds>::from(10i32));
/// ```
///
/// ## Convert from a negative `i32`
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(-10, *Duration::<Seconds>::from(-10i32));
/// assert_eq!(-10, *Duration::<Milliseconds>::from(-10i32));
/// ```
///
/// ## Convert from `u32`
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(10i32, *Duration::<Seconds>::from(10u32));
/// assert_eq!(10i32, *Duration::<Milliseconds>::from(10u32));
/// ```
///
/// ## Convert from `u32` -- capped
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(Duration::MAX, Duration::<Seconds>::from(u32::MAX));
/// assert_eq!(Duration::MAX, Duration::<Milliseconds>::from(u32::MAX));
/// ```
///
/// ## Convert from `i64`
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(10i32, *Duration::<Seconds>::from(10i64));
/// assert_eq!(10i32, *Duration::<Milliseconds>::from(10i64));
/// ```
///
/// ## Convert from `i64` -- capped max
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(Duration::MAX, Duration::<Seconds>::from(i64::MAX));
/// assert_eq!(Duration::MAX, Duration::<Milliseconds>::from(i64::MAX));
/// ```
///
/// ## Convert from `i64` -- capped min
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(Duration::MIN, Duration::<Seconds>::from(i64::MIN));
/// assert_eq!(Duration::MIN, Duration::<Milliseconds>::from(i64::MIN));
/// ```
///
/// ## Convert from [`chrono::Duration`]
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(10i32, *Duration::<Seconds>::from(chrono::Duration::seconds(10)));
/// assert_eq!(10_000i32, *Duration::<Milliseconds>::from(chrono::Duration::seconds(10)));
/// ```
///
/// ## Convert from [`chrono::Duration`] -- capped max
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(i32::MAX, *Duration::<Seconds>::from(chrono::Duration::MAX));
/// assert_eq!(Duration::MAX, Duration::<Milliseconds>::from(chrono::Duration::MAX));
/// ```
///
/// ## Convert from [`chrono::Duration`] -- capped min
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(Duration::MIN, Duration::<Seconds>::from(chrono::Duration::MIN));
/// assert_eq!(Duration::MIN, Duration::<Milliseconds>::from(chrono::Duration::MIN));
/// ```
///
/// ## Convert from [`std::time::Duration`]
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(10i32, *Duration::<Seconds>::from(std::time::Duration::from_secs(10)));
/// assert_eq!(10_000i32, *Duration::<Milliseconds>::from(std::time::Duration::from_secs(10)));
/// ```
///
/// ## Convert from [`std::time::Duration`] -- capped max
/// ```
/// # use pgmq::types::duration::{Duration, Milliseconds, Seconds};
/// assert_eq!(i32::MAX, *Duration::<Seconds>::from(std::time::Duration::MAX));
/// assert_eq!(Duration::MAX, Duration::<Milliseconds>::from(std::time::Duration::MAX));
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Duration<U: Unit> {
    value: i32,
    unit: PhantomData<U>,
}

/// Marker trait for a duration unit. For example, [`Seconds`] and [`Milliseconds`]. Only the units
/// relevant for PGMQ SQL functions are supported.
#[allow(private_bounds)]
pub trait Unit: Sealed {}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Milliseconds;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Seconds;

impl Sealed for Milliseconds {}
impl Sealed for Seconds {}
impl Unit for Milliseconds {}
impl Unit for Seconds {}

impl<U: Unit> Duration<U> {
    /// The minimum allowed [`Duration`] value.
    pub const MIN: Self = Self::new(i32::MIN);

    /// The maximum allowed [`Duration`] value.
    pub const MAX: Self = Self::new(i32::MAX);

    pub const fn new(value: i32) -> Self {
        Self {
            value,
            unit: PhantomData::<U>,
        }
    }

    pub fn value(&self) -> i32 {
        self.value
    }
}

impl Duration<Seconds> {
    pub fn seconds(value: i32) -> Self {
        Self::new(value)
    }

    pub fn as_seconds(&self) -> i32 {
        self.value
    }
}

impl Duration<Milliseconds> {
    pub fn milliseconds(value: i32) -> Self {
        Self::new(value)
    }

    pub fn as_milliseconds(&self) -> i32 {
        self.value
    }
}

impl<U: Unit> AsRef<i32> for Duration<U> {
    fn as_ref(&self) -> &i32 {
        &self.value
    }
}

impl<U: Unit> Deref for Duration<U> {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<U: Unit> From<i32> for Duration<U> {
    fn from(value: i32) -> Self {
        Self::new(value)
    }
}

impl<U: Unit> From<u32> for Duration<U> {
    fn from(value: u32) -> Self {
        Self::new(i32::try_from(value).unwrap_or(*Self::MAX))
    }
}

impl<U: Unit> From<i64> for Duration<U> {
    fn from(value: i64) -> Self {
        let value = i32::try_from(std::cmp::max(value, *Self::MIN as i64)).unwrap_or(*Self::MAX);
        Self::new(value)
    }
}

impl<U: Unit> From<u64> for Duration<U> {
    fn from(value: u64) -> Self {
        Self::new(i32::try_from(value).unwrap_or(*Self::MAX))
    }
}

impl<U: Unit> From<i128> for Duration<U> {
    fn from(value: i128) -> Self {
        let value = i32::try_from(std::cmp::max(value, *Self::MIN as i128)).unwrap_or(*Self::MAX);
        Self::new(value)
    }
}

impl<U: Unit> From<u128> for Duration<U> {
    fn from(value: u128) -> Self {
        Self::new(i32::try_from(value).unwrap_or(*Self::MAX))
    }
}

impl From<chrono::Duration> for Duration<Seconds> {
    fn from(value: chrono::Duration) -> Self {
        value.num_seconds().into()
    }
}

impl From<chrono::Duration> for Duration<Milliseconds> {
    fn from(value: chrono::Duration) -> Self {
        value.num_milliseconds().into()
    }
}

impl From<std::time::Duration> for Duration<Seconds> {
    fn from(value: std::time::Duration) -> Self {
        value.as_secs().into()
    }
}

impl From<std::time::Duration> for Duration<Milliseconds> {
    fn from(value: std::time::Duration) -> Self {
        value.as_millis().into()
    }
}

#[cfg(feature = "sqlx")]
impl<DB: sqlx::Database, U: Unit> sqlx::Type<DB> for Duration<U>
where
    i32: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <i32 as sqlx::Type<DB>>::type_info()
    }
}

#[cfg(feature = "sqlx")]
impl<'q, DB: sqlx::Database, U: Unit> sqlx::Encode<'q, DB> for Duration<U>
where
    i32: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::Database>::ArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i32 as sqlx::Encode<'q, DB>>::encode_by_ref(&self.value, buf)
    }
}
