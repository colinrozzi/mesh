//! `counter-sm` — a replicated counter as an RSM `state-machine` component.
//!
//! The SM layer of the reference stack (system / **SM** / node), implementing
//! DESIGN-rsm.md Interface 1 with **typed state**: the node's generic `s` is pinned to a
//! `CounterState` record, so `validate`/`apply` take/return a real state — no serde, no
//! hand-rolled codec. `Cmd`/`CounterState` are the counter's data schema, defined ONCE in
//! `counter.wit` and generated here via `wit!` (shared with `counter-system` — no protocol
//! crate; drift is caught at compose by structural hashing).
//!
//! **Conflict-free (admission-final).** `apply` is `count += n`; the only validity rule
//! (`Inc` deltas must be positive) is a pure function of the payload, so events commute.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::{export, wit};

#[cfg(not(test))]
packr_guest::setup_guest!();

// Generate `Cmd` + `CounterState` from the shared counter.wit (wit/ symlink).
wit! {}

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

/// Decode a counter payload into a `Cmd` (the wire is the Graph-ABI encoding of `Cmd`).
fn decode(payload: &[u8]) -> Option<Cmd> {
    packr_guest::decode(payload).ok().and_then(|v| Cmd::try_from(v).ok())
}

// ===== core logic (host-testable; the #[export] wrappers are thin) =====

fn do_validate(payload: &[u8]) -> Result<bool, String> {
    if payload.is_empty() {
        return Ok(true); // inert graft (the node's own genesis / witness)
    }
    match decode(payload).ok_or_else(|| "undecodable counter payload".to_string())? {
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
    CounterState { count: 0, ops: 0 }
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
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use packr_guest::Value;

    fn encode(cmd: Cmd) -> Vec<u8> {
        packr_guest::encode(&Value::from(cmd)).unwrap_or_default()
    }

    #[test]
    fn increments_accumulate() {
        let mut s = CounterState { count: 0, ops: 0 };
        for c in [Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(10)] {
            let p = encode(c);
            assert!(do_validate(&p).is_ok());
            s = do_apply(&p, s);
        }
        assert_eq!(s.count, 17);
        assert_eq!(s.ops, 3);
    }

    #[test]
    fn non_positive_increment_is_rejected() {
        assert!(do_validate(&encode(Cmd::Inc(0))).is_err());
        assert!(do_validate(&encode(Cmd::Inc(-3))).is_err());
        assert!(do_validate(&encode(Cmd::Inc(1))).is_ok());
    }

    #[test]
    fn reset_zeroes() {
        let s0 = do_apply(&encode(Cmd::Inc(5)), CounterState { count: 0, ops: 0 });
        let s1 = do_apply(&encode(Cmd::Reset), s0);
        let s2 = do_apply(&encode(Cmd::Inc(2)), s1);
        assert_eq!(s2.count, 2);
    }

    #[test]
    fn empty_payload_is_inert() {
        let s = do_apply(&encode(Cmd::Inc(9)), CounterState { count: 0, ops: 0 });
        assert!(do_validate(b"").is_ok());
        assert_eq!(do_apply(b"", CounterState { count: s.count, ops: s.ops }).count, s.count);
    }
}
