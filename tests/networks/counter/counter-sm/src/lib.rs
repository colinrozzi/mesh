//! `counter-sm` — a replicated counter as an RSM `state-machine` component.
//!
//! The SM layer of the reference stack (executor / protocol / **SM** / node),
//! implementing DESIGN-rsm.md Interface 1. This is the FIRST SM to use **typed state**
//! (packr 0.13 generics): it pins the node's generic `s` to a `CounterState` record via
//! `#[derive(GraphValue)]`, so `validate`/`apply` take and return a real `CounterState`
//! — no `serde_json`, no hand-rolled state codec. The state marshals across the composed
//! boundary through the Graph ABI; compose unifies the node's `s := counter-state`.
//!
//! **Conflict-free (admission-final).** `apply` is `count += n`; the only validity rule
//! (`Inc` deltas must be positive) is a pure function of the payload, so events commute.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use counter_protocol::{decode, Cmd, CounterState};
use packr_guest::export;

#[cfg(not(test))]
packr_guest::setup_guest!();

// State is a TYPED record now: `counter-state` in the interface, `CounterState` in Rust.
packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> counter-state,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: counter-state) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: counter-state) -> counter-state,
            members: func(state: counter-state) -> list<list<u8>>,
        }
    }
}

// `CounterState` (the typed `counter-state`) now lives in `counter-protocol`, shared with
// the counter system so `current-state` decodes back into it typed.

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

fn do_apply(payload: &[u8], mut s: CounterState) -> CounterState {
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
    s
}

// ===== the interface (typed state in/out) =====

#[export(name = "initial-state")]
fn initial_state() -> CounterState {
    CounterState::default()
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, _state: CounterState) -> Result<bool, String> {
    do_validate(&payload)
}

#[export]
fn apply(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: CounterState) -> CounterState {
    do_apply(&payload, state)
}

#[export]
fn members(_state: CounterState) -> Vec<Vec<u8>> {
    // A counter has no membership concept.
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use counter_protocol::encode;

    fn fold(cmds: &[Cmd]) -> CounterState {
        let mut s = CounterState::default();
        for cmd in cmds {
            let p = encode(cmd);
            if do_validate(&p).is_ok() {
                s = do_apply(&p, s);
            }
        }
        s
    }

    #[test]
    fn increments_accumulate() {
        let s = fold(&[Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(10)]);
        assert_eq!(s.count, 17);
        assert_eq!(s.ops, 3);
    }

    #[test]
    fn reset_zeroes() {
        assert_eq!(fold(&[Cmd::Inc(5), Cmd::Reset, Cmd::Inc(2)]).count, 2);
    }

    #[test]
    fn non_positive_increment_is_rejected() {
        assert!(do_validate(&encode(&Cmd::Inc(0))).is_err());
        assert!(do_validate(&encode(&Cmd::Inc(-3))).is_err());
        assert!(do_validate(&encode(&Cmd::Inc(1))).is_ok());
        assert_eq!(fold(&[Cmd::Inc(5), Cmd::Inc(2)]).count, 7);
    }

    #[test]
    fn increments_commute() {
        assert_eq!(
            fold(&[Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(5)]).count,
            fold(&[Cmd::Inc(5), Cmd::Inc(3), Cmd::Inc(4)]).count
        );
    }

    #[test]
    fn empty_payload_is_inert() {
        let s = fold(&[Cmd::Inc(9)]);
        assert!(do_validate(b"").is_ok());
        assert_eq!(do_apply(b"", s.clone()).count, s.count);
    }
}
