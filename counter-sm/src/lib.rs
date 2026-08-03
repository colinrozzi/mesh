//! `counter-sm` — a replicated counter as an RSM `state-machine` component.
//!
//! The SM layer of the reference stack (executor / protocol / **SM** / node),
//! implementing DESIGN-rsm.md Interface 1 (`initial-state`/`validate`/`apply`/
//! `members`). Pure, deterministic, structure-blind: it sees only an event and a
//! state, never the DAG. It imports `counter-protocol` for the payload codec — the
//! SM never re-shapes bytes, so there is one codec and zero drift.
//!
//! **Conflict-free (admission-final).** `apply` is `count += n` / `count = 0`; the
//! only validity rule (`Inc` deltas must be positive) is a pure function of the
//! payload, independent of prior state — so concurrent increments commute and every
//! admitted event is final. This is the simplest complete SM: enough to exercise the
//! full node+executor loop, with nothing conflict-prone to distract from the wiring.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use counter_protocol::{decode, Cmd};
use packr_guest::export;
use serde::{Deserialize, Serialize};

#[cfg(not(test))]
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

// ===== state =====

#[derive(Serialize, Deserialize, Default, Clone)]
struct CounterState {
    count: i64,
    /// Number of increments applied — shows a derived field beyond the raw count.
    ops: u64,
}

impl CounterState {
    fn decode(bytes: &[u8]) -> CounterState {
        if bytes.is_empty() {
            return CounterState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

// ===== core logic (host-testable; the #[export] wrappers are thin) =====

fn do_validate(payload: &[u8]) -> Result<bool, String> {
    if payload.is_empty() {
        return Ok(true); // inert graft (the node's own genesis / witness)
    }
    match decode(payload).ok_or_else(|| "undecodable counter payload".to_string())? {
        // Positive-only increments: a pure-payload rule, so validity is
        // state-independent and every event commutes (confluent, admission-final).
        Cmd::Inc(n) if n > 0 => Ok(true),
        Cmd::Inc(_) => Err("increment must be positive".to_string()),
        Cmd::Reset => Ok(true),
    }
}

fn do_apply(payload: &[u8], state: &[u8]) -> Vec<u8> {
    let mut s = CounterState::decode(state);
    match decode(payload) {
        Some(Cmd::Inc(n)) => {
            s.count = s.count.saturating_add(n);
            s.ops = s.ops.saturating_add(1);
        }
        Some(Cmd::Reset) => {
            s.count = 0;
            s.ops = s.ops.saturating_add(1);
        }
        None => {} // empty / undecodable → inert
    }
    s.encode()
}

// ===== the interface =====

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    CounterState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, _state: Vec<u8>) -> Result<bool, String> {
    do_validate(&payload)
}

#[export]
fn apply(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8> {
    do_apply(&payload, &state)
}

#[export]
fn members(_state: Vec<u8>) -> Vec<Vec<u8>> {
    // A counter has no membership concept.
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use counter_protocol::encode;

    fn fold(seed: &[u8], cmds: &[Cmd]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for cmd in cmds {
            let p = encode(cmd);
            if do_validate(&p).is_ok() {
                s = do_apply(&p, &s);
            }
        }
        s
    }

    #[test]
    fn increments_accumulate() {
        let s = fold(b"", &[Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(10)]);
        let st = CounterState::decode(&s);
        assert_eq!(st.count, 17);
        assert_eq!(st.ops, 3);
    }

    #[test]
    fn reset_zeroes() {
        let s = fold(b"", &[Cmd::Inc(5), Cmd::Reset, Cmd::Inc(2)]);
        assert_eq!(CounterState::decode(&s).count, 2);
    }

    #[test]
    fn non_positive_increment_is_rejected() {
        assert!(do_validate(&encode(&Cmd::Inc(0))).is_err());
        assert!(do_validate(&encode(&Cmd::Inc(-3))).is_err());
        assert!(do_validate(&encode(&Cmd::Inc(1))).is_ok());
        // a rejected event never applies → count unchanged
        let s = fold(b"", &[Cmd::Inc(5), Cmd::Inc(-100), Cmd::Inc(2)]);
        assert_eq!(CounterState::decode(&s).count, 7);
    }

    #[test]
    fn increments_commute() {
        // confluence: any order of increments yields the same count.
        let a = fold(b"", &[Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(5)]);
        let b = fold(b"", &[Cmd::Inc(5), Cmd::Inc(3), Cmd::Inc(4)]);
        assert_eq!(CounterState::decode(&a).count, CounterState::decode(&b).count);
    }

    #[test]
    fn empty_payload_is_inert() {
        let s = fold(b"", &[Cmd::Inc(9)]);
        assert!(do_validate(b"").is_ok());
        assert_eq!(do_apply(b"", &s), s);
    }
}
