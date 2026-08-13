//! `bank-sm` — a currency as an RSM `state-machine` component.
//!
//! Wallets → balances. `Mint` creates money (always valid); `Transfer{from,to,amount}`
//! is valid **iff** `from` holds `amount` — the first **conflict-prone** SM (two transfers
//! from one wallet, each valid against its own ancestry, double-spend when both apply; see
//! the `double_spend_*` tests). Balances are `i64` so an overdraw shows as negative.
//!
//! Both the payload AND the state are **typed** (`bank_protocol::{Cmd, BankState}`): the
//! node is generic over the payload `p` and the state `s`, so `validate`/`apply` receive a
//! real `Cmd` and a real `BankState` — no `decode`, no serde. `BankState.wallets` is a
//! `BTreeMap` carried on the wire as a key-sorted `list<tuple<string,s64>>` (packr's
//! `map<string,s64>`), which keeps every node's folded state byte-identical.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use bank_protocol::{BankState, Cmd};
use packr_guest::export;

#[cfg(not(test))]
packr_guest::setup_guest!();

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> bank-state,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: bank-state) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: bank-state) -> bank-state,
            members: func(state: bank-state) -> list<list<u8>>,
        }
    }
}

// ===== core logic (host-testable) — payload AND state arrive TYPED, no decode =====

fn do_validate(cmd: &Cmd, state: &BankState) -> Result<bool, String> {
    match cmd {
        Cmd::Mint { .. } => Ok(true),
        Cmd::Transfer { from, amount, .. } => {
            if state.balance(from) >= *amount as i64 {
                Ok(true)
            } else {
                Err("insufficient balance".to_string())
            }
        }
    }
}

fn do_apply(cmd: Cmd, mut state: BankState) -> BankState {
    match cmd {
        Cmd::Mint { to, amount } => {
            *state.wallets.entry(to).or_insert(0) += amount as i64;
        }
        Cmd::Transfer { from, to, amount } => {
            *state.wallets.entry(from).or_insert(0) -= amount as i64;
            *state.wallets.entry(to).or_insert(0) += amount as i64;
        }
    }
    state
}

// ===== the interface =====

#[export(name = "initial-state")]
fn initial_state() -> BankState {
    BankState::default()
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, state: BankState) -> Result<bool, String> {
    do_validate(&payload, &state)
}

#[export]
fn apply(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, state: BankState) -> BankState {
    do_apply(payload, state)
}

#[export]
fn members(_state: BankState) -> Vec<Vec<u8>> {
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
    fn fold(seed: BankState, evs: &[Cmd]) -> BankState {
        let mut s = seed;
        for c in evs {
            if do_validate(c, &s).is_ok() {
                s = do_apply(c.clone(), s);
            }
        }
        s
    }
    fn total(s: &BankState) -> i64 {
        s.wallets.values().sum()
    }

    #[test]
    fn mint_and_transfer() {
        let s = fold(BankState::default(), &[mint("alice", 100), transfer("alice", "bob", 30)]);
        assert_eq!(s.balance("alice"), 70);
        assert_eq!(s.balance("bob"), 30);
    }

    #[test]
    fn insufficient_balance_rejected() {
        let s = fold(BankState::default(), &[mint("alice", 100)]);
        assert!(do_validate(&transfer("alice", "bob", 200), &s).is_err());
        let s2 = fold(s, &[transfer("alice", "bob", 200), transfer("alice", "bob", 40)]);
        assert_eq!(s2.balance("alice"), 60);
    }

    #[test]
    fn double_spend_is_the_conflict_frontier() {
        let s = fold(BankState::default(), &[mint("alice", 100)]);
        let t1 = transfer("alice", "bob", 60);
        let t2 = transfer("alice", "carol", 60);
        assert!(do_validate(&t1, &s).is_ok());
        assert!(do_validate(&t2, &s).is_ok(), "each transfer is valid against its own ancestry");
        let merged = do_apply(t2, do_apply(t1, s.clone()));
        assert_eq!(merged.balance("alice"), -20, "overdraw: 100 - 60 - 60");
        assert_eq!(total(&merged), 100, "supply is not created, but alice is negative");
    }
}
