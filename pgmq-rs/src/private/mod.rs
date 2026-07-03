pub mod util;

/// Private marker trait that can be used to mark another trait as "sealed", which prevents external
/// consumers from implementing the trait. This allows adding new methods to the sealed trait
/// in a semver compatible way. See: https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
pub(crate) trait Sealed {}
