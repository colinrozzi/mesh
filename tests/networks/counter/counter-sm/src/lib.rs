//! `counter-sm` — a replicated counter as an RSM `state-machine` component.
//!
//! The SM layer of the reference stack (system / **SM** / node), implementing
//! DESIGN-rsm.md Interface 1 with **typed everything**: the node is generic over the SM's
//! state `s` AND its payload `p`, so `validate`/`apply` receive a real `CounterState` *and*
//! a real `Cmd` — no serde, no hand-rolled codec, no `decode` in the SM. `Cmd`/`CounterState`
//! are the counter's data schema, defined ONCE in `counter.pact` and generated via `pact!`
//! (shared with `counter-system` — no protocol crate; drift is caught at compose).
//!
//! **Conflict-free (admission-final).** `apply` is `count += n`; the only validity rule
//! (`Inc` deltas must be positive) is a pure function of the payload, so events commute.
//! (Empty genesis/witness grafts never reach the SM — the node handles those.)

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::{export, pact};

#[cfg(not(test))]
packr_guest::setup_guest!();

// Generate `Cmd` + `CounterState` from the shared counter.pact (shared, via pact!(from …)).
pact!(from "../counter.pact");

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> counter-state,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: counter-state) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: cmd, state: counter-state) -> counter-state,
            members: func(state: counter-state) -> list<list<u8>>,
        }
    }
}

// ===== core logic (host-testable) — payload arrives TYPED as `Cmd`, no decode =====

fn do_validate(cmd: &Cmd) -> Result<bool, String> {
    match cmd {
        Cmd::Inc(n) if *n > 0 => Ok(true),
        Cmd::Inc(_) => Err("increment must be positive".to_string()),
        Cmd::Reset => Ok(true),
    }
}

fn do_apply(cmd: &Cmd, mut s: CounterState) -> CounterState {
    match cmd {
        Cmd::Inc(n) => {
            s.count = s.count.saturating_add(*n);
            s.ops = s.ops.saturating_add(1);
        }
        Cmd::Reset => {
            s.count = 0;
            s.ops = s.ops.saturating_add(1);
        }
    }
    s
}

// ===== the interface (typed payload + state in/out) =====

#[export(name = "initial-state")]
fn initial_state() -> CounterState {
    CounterState { count: 0, ops: 0 }
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, _state: CounterState) -> Result<bool, String> {
    do_validate(&payload)
}

#[export]
fn apply(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Cmd, state: CounterState) -> CounterState {
    do_apply(&payload, state)
}

#[export]
fn members(_state: CounterState) -> Vec<Vec<u8>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increments_accumulate() {
        let mut s = CounterState { count: 0, ops: 0 };
        for c in [Cmd::Inc(3), Cmd::Inc(4), Cmd::Inc(10)] {
            assert!(do_validate(&c).is_ok());
            s = do_apply(&c, s);
        }
        assert_eq!(s.count, 17);
        assert_eq!(s.ops, 3);
    }

    #[test]
    fn non_positive_increment_is_rejected() {
        assert!(do_validate(&Cmd::Inc(0)).is_err());
        assert!(do_validate(&Cmd::Inc(-3)).is_err());
        assert!(do_validate(&Cmd::Inc(1)).is_ok());
    }

    #[test]
    fn reset_zeroes() {
        let s0 = do_apply(&Cmd::Inc(5), CounterState { count: 0, ops: 0 });
        let s1 = do_apply(&Cmd::Reset, s0);
        let s2 = do_apply(&Cmd::Inc(2), s1);
        assert_eq!(s2.count, 2);
    }
}
