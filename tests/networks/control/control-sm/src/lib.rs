//! `control-sm` — the sentinel control plane as an RSM `state-machine` component.
//!
//! Interface 1 of DESIGN-rsm.md (`initial-state`/`validate`/`apply`/`members`),
//! implementing /tmp/CONTROL-SM-design.md: membership + `command_allow` authz + a
//! command/response journal keyed by `(author, corr_id)`. Pure, deterministic,
//! structure-blind (operates only on an event and a state, never the DAG).
//!
//! Admission-final under the **no-kick invariant** (membership shrinks only via
//! causal self-`depart`, never a concurrent kick), so nothing here is conflict-
//! prone in v0. Config (members + allow-lists) arrives as a **genesis event**, not
//! `initial-state` args, so it lives in signed history (no byte-identical-members
//! footgun).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use control_protocol::{decode, Msg};
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
struct ControlState {
    members: BTreeSet<Vec<u8>>,
    join_allow: BTreeSet<Vec<u8>>,
    command_allow: BTreeSet<Vec<u8>>,
    journal: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Entry {
    author: Vec<u8>, // the command's author (with corr_id, the journal key)
    corr_id: u64,
    verb: String,
    args: Vec<u8>,
    response: Option<Vec<u8>>,
}

impl ControlState {
    fn decode(bytes: &[u8]) -> ControlState {
        if bytes.is_empty() {
            return ControlState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn entry(&self, author: &[u8], corr_id: u64) -> Option<&Entry> {
        self.journal.iter().find(|e| e.author == author && e.corr_id == corr_id)
    }
}

// ===== core logic (host-testable; the #[export] wrappers are thin) =====
// The payload codec (kinds + encode/decode) lives in `control-protocol`.

fn do_validate(author: &[u8], payload: &[u8], state: &[u8]) -> Result<bool, String> {
    if payload.is_empty() {
        return Ok(true); // inert graft (the node's own genesis / witness)
    }
    let s = ControlState::decode(state);
    let msg = decode(payload).ok_or_else(|| "undecodable control payload".to_string())?;
    match msg {
        Msg::Genesis { .. } => {
            if s.members.is_empty() && s.join_allow.is_empty() && s.command_allow.is_empty() {
                Ok(true)
            } else {
                Err("genesis after the control mesh already exists".to_string())
            }
        }
        Msg::JoinRequest => {
            if s.join_allow.contains(author) {
                Ok(true)
            } else {
                Err("join-request: author not in join_allow".to_string())
            }
        }
        Msg::Depart => {
            if s.members.contains(author) {
                Ok(true)
            } else {
                Err("depart: author is not a member".to_string())
            }
        }
        Msg::Command { corr_id, .. } => {
            if !s.members.contains(author) {
                Err("command: author is not a member".to_string())
            } else if !s.command_allow.contains(author) {
                Err("command: author not in command_allow".to_string())
            } else if s.entry(author, corr_id).is_some() {
                Err("command: corr_id already used by this author".to_string())
            } else {
                Ok(true)
            }
        }
        Msg::Response { corr_id, cmd_author, .. } => {
            if !s.members.contains(author) {
                return Err("response: author is not a member".to_string());
            }
            match s.entry(&cmd_author, corr_id) {
                Some(e) if e.response.is_none() => Ok(true),
                Some(_) => Err("response: already answered".to_string()),
                None => Err("response: no matching command".to_string()),
            }
        }
    }
}

fn do_apply(author: &[u8], payload: &[u8], state: &[u8]) -> Vec<u8> {
    let mut s = ControlState::decode(state);
    let Some(msg) = decode(payload) else {
        return s.encode(); // empty / undecodable → inert
    };
    match msg {
        Msg::Genesis { members, join_allow, command_allow } => {
            s.members = members.into_iter().collect();
            s.join_allow = join_allow.into_iter().collect();
            s.command_allow = command_allow.into_iter().collect();
        }
        Msg::JoinRequest => {
            s.members.insert(author.to_vec());
        }
        Msg::Depart => {
            s.members.remove(author);
        }
        Msg::Command { corr_id, verb, args } => {
            s.journal.push(Entry { author: author.to_vec(), corr_id, verb, args, response: None });
        }
        Msg::Response { corr_id, cmd_author, result } => {
            if let Some(e) = s.journal.iter_mut().find(|e| e.author == cmd_author && e.corr_id == corr_id) {
                e.response = Some(result);
            }
        }
    }
    s.encode()
}

// ===== the interface =====

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    ControlState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&author, &payload, &state)
}

#[export]
fn apply(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8> {
    do_apply(&author, &payload, &state)
}

#[export]
fn members(state: Vec<u8>) -> Vec<Vec<u8>> {
    ControlState::decode(&state).members.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_protocol::encode;

    fn k(n: u8) -> Vec<u8> {
        alloc::vec![n; 32]
    }

    // Fold a sequence of (author, msg) through validate+apply from genesis-seed,
    // returning the final state (applying only what validates — the node's rule).
    fn fold(seed: &[u8], evs: &[(Vec<u8>, Msg)]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for (author, msg) in evs {
            let p = encode(msg);
            if do_validate(author, &p, &s).is_ok() {
                s = do_apply(author, &p, &s);
            }
        }
        s
    }

    fn sentinel() -> Vec<u8> {
        k(1)
    }
    fn manager() -> Vec<u8> {
        k(2)
    }
    fn stranger() -> Vec<u8> {
        k(9)
    }

    // sentinel authors genesis: members={sentinel}, join_allow=command_allow={manager}.
    fn genesis() -> Msg {
        Msg::Genesis {
            members: alloc::vec![sentinel()],
            join_allow: alloc::vec![manager()],
            command_allow: alloc::vec![manager()],
        }
    }

    #[test]
    fn genesis_seeds_membership_and_allowlists() {
        let s = fold(&do_apply(&[], &[], b""), &[(sentinel(), genesis())]);
        let st = ControlState::decode(&s);
        assert!(st.members.contains(&sentinel()));
        assert!(st.join_allow.contains(&manager()) && st.command_allow.contains(&manager()));
    }

    #[test]
    fn second_genesis_is_rejected() {
        let s = fold(b"", &[(sentinel(), genesis())]);
        assert!(do_validate(&sentinel(), &encode(&genesis()), &s).is_err());
    }

    #[test]
    fn join_request_gated_by_join_allow() {
        let s = fold(b"", &[(sentinel(), genesis())]);
        // manager is in join_allow → admits; stranger is not → rejected.
        assert!(do_validate(&manager(), &encode(&Msg::JoinRequest), &s).is_ok());
        assert!(do_validate(&stranger(), &encode(&Msg::JoinRequest), &s).is_err());
        let s2 = fold(&s, &[(manager(), Msg::JoinRequest)]);
        assert!(ControlState::decode(&s2).members.contains(&manager()));
    }

    #[test]
    fn command_requires_membership_and_command_allow_and_fresh_corrid() {
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest)]);
        let cmd = |c| Msg::Command { corr_id: c, verb: "list".into(), args: alloc::vec![] };
        // manager is a member ∧ command_allow → ok.
        assert!(do_validate(&manager(), &encode(&cmd(1)), &s).is_ok());
        // stranger is neither a member nor command_allow → rejected.
        assert!(do_validate(&stranger(), &encode(&cmd(1)), &s).is_err());
        // apply the command, then a duplicate corr_id from the same author is rejected.
        let s2 = fold(&s, &[(manager(), cmd(1))]);
        assert!(do_validate(&manager(), &encode(&cmd(1)), &s2).is_err(), "dup corr_id");
        assert!(do_validate(&manager(), &encode(&cmd(2)), &s2).is_ok(), "fresh corr_id");
    }

    #[test]
    fn response_answers_a_pending_command_once() {
        let cmd = Msg::Command { corr_id: 7, verb: "list".into(), args: alloc::vec![] };
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest), (manager(), cmd)]);
        let resp = |r: &[u8]| Msg::Response { corr_id: 7, cmd_author: manager(), result: r.to_vec() };
        // sentinel (a member) answers → ok; then a second answer is rejected.
        assert!(do_validate(&sentinel(), &encode(&resp(b"ok")), &s).is_ok());
        let s2 = fold(&s, &[(sentinel(), resp(b"ok"))]);
        let st = ControlState::decode(&s2);
        assert_eq!(st.entry(&manager(), 7).unwrap().response.as_deref(), Some(&b"ok"[..]));
        assert!(do_validate(&sentinel(), &encode(&resp(b"again")), &s2).is_err(), "already answered");
        // a response with no matching command is rejected.
        assert!(do_validate(&sentinel(), &encode(&Msg::Response { corr_id: 99, cmd_author: manager(), result: alloc::vec![] }), &s2).is_err());
    }

    #[test]
    fn depart_removes_self_and_empty_is_inert() {
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest)]);
        let s2 = fold(&s, &[(manager(), Msg::Depart)]);
        assert!(!ControlState::decode(&s2).members.contains(&manager()));
        // an empty payload (a node's genesis graft) is a valid no-op.
        assert!(do_validate(&stranger(), b"", &s2).is_ok());
        assert_eq!(do_apply(&stranger(), b"", &s2), s2, "empty payload does not change state");
    }
}
