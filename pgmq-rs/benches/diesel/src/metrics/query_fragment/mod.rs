//! Implement queries using `QueryFragment`. This approach is more verbose than any of the other
//! options, but has similar performance to the standard approach of invoking SQL functions. We
//! could probably create a macro to reduce the verbosity of this approach.
//!
//! There are a few minor variations of the `QueryFragment` implementation available. They are
//! implemented in the submodules so we can compare their performance.

pub mod standard;
pub mod tuple;
pub mod unnamed_params;
pub mod untyped;
