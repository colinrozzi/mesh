//! `bank-protocol` — the currency schema: the TYPED payload AND the TYPED state.
//!
//! Both the payload kinds (`Cmd`) and the state (`BankState`) are `#[derive(GraphValue)]`
//! types, so they marshal through the Graph ABI automatically — no hand-rolled cursor, no
//! serde. The SM and the executor share these types and let the ABI do the parsing.
//!
//! `BankState.wallets` is a `BTreeMap<String, i64>`, which the ABI carries as a canonical
//! (key-sorted) `list<tuple<string, s64>>` — the wire shape of packr's `map<string, s64>`
//! (packr >= 0.15). The BTreeMap gives that canonical order for free, so every node's
//! folded state serializes byte-identically → convergence holds. This is why bank can now
//! be a TYPED-state SM (the second, after counter) even though its state is a dynamic map:
//! counter shows typed state for a flat record, bank shows it for a map.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Cmd {
    Mint { to: String, amount: u64 },
    Transfer { from: String, to: String, amount: u64 },
}

/// The bank's typed state: wallet name → balance (i64 so an overdraw double-spend shows
/// as negative). Carried on the wire as a key-sorted `list<tuple<string, s64>>`.
#[derive(Debug, Clone, Default, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct BankState {
    pub wallets: BTreeMap<String, i64>,
}

impl BankState {
    /// Balance of a wallet (0 if it has never been touched).
    pub fn balance(&self, w: &str) -> i64 {
        self.wallets.get(w).copied().unwrap_or(0)
    }
}

/// Encode a command via the Graph ABI.
pub fn encode(cmd: &Cmd) -> Vec<u8> {
    abi_encode(&Value::from(cmd.clone())).unwrap_or_default()
}

/// Decode a payload. `None` on anything that is not a well-formed `Cmd`.
pub fn decode(payload: &[u8]) -> Option<Cmd> {
    abi_decode(payload).ok().and_then(|v| Cmd::try_from(v).ok())
}

/// Encode the bank state via the Graph ABI (the wire form the node's `current-state` returns).
pub fn encode_state(state: &BankState) -> Vec<u8> {
    abi_encode(&Value::from(state.clone())).unwrap_or_default()
}

/// Decode bank state; `None` on anything not a well-formed `BankState`.
pub fn decode_state(bytes: &[u8]) -> Option<BankState> {
    abi_decode(bytes).ok().and_then(|v| BankState::try_from(v).ok())
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

    #[test]
    fn state_round_trips_and_is_canonical() {
        let mut s = BankState::default();
        s.wallets.insert("alice".to_string(), -20);
        s.wallets.insert("bob".to_string(), 60);
        s.wallets.insert("carol".to_string(), 60);
        assert_eq!(decode_state(&encode_state(&s)), Some(s.clone()));
        // key-sorted canonical form: insertion order must NOT affect the bytes.
        let mut s2 = BankState::default();
        s2.wallets.insert("carol".to_string(), 60);
        s2.wallets.insert("alice".to_string(), -20);
        s2.wallets.insert("bob".to_string(), 60);
        assert_eq!(encode_state(&s), encode_state(&s2), "map wire form must be key-canonical");
        assert_eq!(s.balance("alice"), -20);
        assert_eq!(s.balance("dave"), 0);
    }
}
