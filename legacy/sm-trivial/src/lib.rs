//! `sm-trivial` — a trivial `state-machine` component (mesh `state-machine.pact`,
//! DESIGN-rsm.md Interface 1).
//!
//! It exists to prove the SM side of the composition boundary: a consumer state
//! machine EXPORTS `initial-state` / `validate` / `apply` / `members`, and the mesh
//! node composes it in and calls it synchronously on the fold. This one tracks the
//! trivial state "the set of authors seen" — `apply` records the author, `members`
//! chunks them back out, `validate` admits everything. Real SMs (control-SM,
//! chat-SM) swap the bodies; the signatures + the pure / confluent / structure-blind
//! contract are the shared thing. A reference template, not a real SM.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::export;

packr_guest::setup_guest!();

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
}

/// Genesis seed: empty state.
#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    Vec::new()
}

/// Trivial admissibility: admit everything. A real SM checks membership / authz
/// against `state` here; a genuine conflict is an `Err` against the consistent state.
#[export]
fn validate(
    _id: Vec<u8>,
    _author: Vec<u8>,
    _timestamp: u64,
    _payload: Vec<u8>,
    _state: Vec<u8>,
) -> Result<bool, String> {
    Ok(true)
}

/// Trivial transition: record the author (append its 32 bytes) if not already
/// present. State is the concatenation of distinct author pubkeys — an idempotent,
/// commutative fold (order-independent), so it satisfies the confluence contract.
#[export]
fn apply(
    _id: Vec<u8>,
    author: Vec<u8>,
    _timestamp: u64,
    _payload: Vec<u8>,
    state: Vec<u8>,
) -> Vec<u8> {
    let mut state = state;
    if author.len() == 32 && !contains_key(&state, &author) {
        state.extend_from_slice(&author);
    }
    state
}

/// Project the member set: `state` chunked into 32-byte pubkeys.
#[export]
fn members(state: Vec<u8>) -> Vec<Vec<u8>> {
    state.chunks_exact(32).map(|c| c.to_vec()).collect()
}

/// Whether `state` (a concatenation of 32-byte keys) already contains `key`.
fn contains_key(state: &[u8], key: &[u8]) -> bool {
    state.chunks_exact(32).any(|c| c == key)
}
