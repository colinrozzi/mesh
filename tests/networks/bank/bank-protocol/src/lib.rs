//! `bank-protocol` — the currency payload, as a TYPED value.
//!
//! The payload kinds are a `#[derive(GraphValue)]` enum, so they marshal through the
//! Graph ABI automatically — no hand-rolled cursor, no `[version][kind][content]` byte
//! math. `encode`/`decode` are one-liners over `packr_guest::{encode,decode}`. This is
//! the payload analog of typed *state*: the SM and the executor share `Cmd` and let the
//! ABI do the parsing. A malformed or wrong-shaped payload fails `try_from` → `None`, so
//! the SM still rejects it as undecodable.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Cmd {
    Mint { to: String, amount: u64 },
    Transfer { from: String, to: String, amount: u64 },
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
    use alloc::string::ToString;

    #[test]
    fn round_trips() {
        for cmd in [
            Cmd::Mint { to: "alice".to_string(), amount: 100 },
            Cmd::Transfer { from: "alice".to_string(), to: "bob".to_string(), amount: 30 },
        ] {
            assert_eq!(decode(&encode(&cmd)), Some(cmd));
        }
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[1, 2, 3, 4, 5]), None);
    }
}
