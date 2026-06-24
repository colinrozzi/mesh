//! Thin re-export of the State and MemberInfo types now defined in `dag.rs`.
//! Kept as its own module for backwards-compatible imports; the real
//! definitions and state-derivation logic live in dag.rs.

pub use crate::dag::{hex, MemberInfo, State};
