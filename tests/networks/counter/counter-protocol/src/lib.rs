//! `counter-protocol` — the counter SM's payload, as a TYPED value.
//!
//! The middle layer of the reference stack (executor / **protocol** / SM / node): the
//! payload `Cmd` kinds, as a `#[derive(GraphValue)]` enum that marshals through the
//! Graph ABI. `encode`/`decode` are one-liners — no hand-rolled cursor. `counter-sm` and
//! the counter executor share `Cmd`, so the wire can never drift. No SM logic here.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

/// A counter command. `Inc` moves the count by a signed delta; `Reset` zeroes it.
#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Cmd {
    Inc(i64),
    Reset,
}

/// Encode a command via the Graph ABI.
pub fn encode(cmd: &Cmd) -> Vec<u8> {
    abi_encode(&Value::from(cmd.clone())).unwrap_or_default()
}

/// Decode a payload. `None` on anything that is not a well-formed `Cmd`.
pub fn decode(payload: &[u8]) -> Option<Cmd> {
    abi_decode(payload).ok().and_then(|v| Cmd::try_from(v).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for cmd in [Cmd::Inc(1), Cmd::Inc(-7), Cmd::Inc(1_000_000), Cmd::Reset] {
            assert_eq!(decode(&encode(&cmd)), Some(cmd));
        }
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[1, 2, 3, 4, 5]), None);
    }
}
