//! `bank-sm` — a currency as an RSM `state-machine` component.
//!
//! Wallets → balances. `Mint` creates money (always valid); `Transfer{from,to,amount}`
//! is valid **iff** `from` holds `amount` — the first **conflict-prone** SM (two transfers
//! from one wallet, each valid against its own ancestry, double-spend when both apply; see
//! the `double_spend_*` tests). Balances are `i64` so an overdraw shows as negative.
//!
//! The payload arrives **typed** as a `bank_protocol::Cmd` (the node is generic over the
//! payload `p`) — no `decode` in the SM. State is opaque bytes (serde).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use bank_protocol::Cmd;
use packr_guest::export;
use serde::{Deserialize, Serialize};

#[cfg(not(test))]
packr_guest::setup_guest!();

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct BankState {
    /// wallet name → balance (i64 so an overdraw double-spend shows as negative).
    wallets: BTreeMap<String, i64>,
}

impl BankState {
    fn decode(bytes: &[u8]) -> BankState {
        if bytes.is_empty() {
            return BankState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn balance(&self, w: &str) -> i64 {
        self.wallets.get(w).copied().unwrap_or(0)
    }
}

fn do_validate(cmd: &Cmd, state: &[u8]) -> Result<bool, String> {
    let s = BankState::decode(state);
    match cmd {
        Cmd::Mint { .. } => Ok(true),
        Cmd::Transfer { from, amount, .. } => {
            if s.balance(from) >= *amount as i64 {
                Ok(true)
            } else {
                Err("insufficient balance".to_string())
            }
        }
    }
}

fn do_apply(cmd: Cmd, state: &[u8]) -> Vec<u8> {
    let mut s = BankState::decode(state);
    match cmd {
        Cmd::Mint { to, amount } => {
            *s.wallets.entry(to).or_insert(0) += amount as i64;
        }
        Cmd::Transfer { from, to, amount } => {
            *s.wallets.entry(from).or_insert(0) -= amount as i64;
            *s.wallets.entry(to).or_insert(0) += amount as i64;
        }
    }
    s.encode()
}

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    BankState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&payload, &state)
}

#[export]
fn apply(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, state: Vec<u8>) -> Vec<u8> {
    do_apply(payload, &state)
}

#[export]
fn members(_state: Vec<u8>) -> Vec<Vec<u8>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mint(to: &str, amount: u64) -> Cmd {
        Cmd::Mint { to: to.to_string(), amount }
    }
    fn transfer(from: &str, to: &str, amount: u64) -> Cmd {
        Cmd::Transfer { from: from.to_string(), to: to.to_string(), amount }
    }
    /// Fold honest events, applying only what validates (the node's rule).
    fn fold(seed: &[u8], evs: &[Cmd]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for c in evs {
            if do_validate(c, &s).is_ok() {
                s = do_apply(c.clone(), &s);
            }
        }
        s
    }
    fn total(s: &[u8]) -> i64 {
        BankState::decode(s).wallets.values().sum()
    }

    #[test]
    fn mint_and_transfer() {
        let s = fold(b"", &[mint("alice", 100), transfer("alice", "bob", 30)]);
        let st = BankState::decode(&s);
        assert_eq!(st.balance("alice"), 70);
        assert_eq!(st.balance("bob"), 30);
    }

    #[test]
    fn insufficient_balance_rejected() {
        let s = fold(b"", &[mint("alice", 100)]);
        assert!(do_validate(&transfer("alice", "bob", 200), &s).is_err());
        let s2 = fold(&s, &[transfer("alice", "bob", 200), transfer("alice", "bob", 40)]);
        assert_eq!(BankState::decode(&s2).balance("alice"), 60);
    }

    #[test]
    fn double_spend_is_the_conflict_frontier() {
        let s = fold(b"", &[mint("alice", 100)]);
        let t1 = transfer("alice", "bob", 60);
        let t2 = transfer("alice", "carol", 60);
        assert!(do_validate(&t1, &s).is_ok());
        assert!(do_validate(&t2, &s).is_ok(), "each transfer is valid against its own ancestry");
        let merged = do_apply(t2, &do_apply(t1, &s));
        let st = BankState::decode(&merged);
        assert_eq!(st.balance("alice"), -20, "overdraw: 100 - 60 - 60");
        assert_eq!(total(&merged), 100, "supply is not created, but alice is negative");
    }
}
