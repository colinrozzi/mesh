//! `chat-sm` — the messaging chat plane as an RSM `state-machine` component.
//!
//! Interface 1 of DESIGN-rsm.md (`initial-state`/`validate`/`apply`/`members`),
//! implementing manager's `chat-sm-design.md`: **OR-Set** membership + an
//! append-only text log. Pure, deterministic, structure-blind (operates only on an
//! event and a state, never the DAG).
//!
//! **Fully conflict-free** (the design goal): every kind commutes, so every event
//! is admission-final and the `on-conflict` surface is empty. The load-bearing
//! choice is that membership is an **observed-remove (OR-Set)**, not a plain set —
//! concurrent add/remove of the same member commutes (add-wins) instead of
//! diverging. The OR-Set tag is the sm-event `id` (already unique per event), so no
//! separate tag allocation. `member-remove` deletes only the `(subject, tag)` pairs
//! present in the event's ANCESTRY state, so a concurrent add (whose tag this remove
//! never observed) survives — add-wins falls out of ancestry-relative validity.
//!
//! A room is one SM instance (one mesh); a DM is a 2-member genesis. Config arrives
//! as a **genesis event** (not `initial-state` args) so it lives in signed history.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

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
struct ChatState {
    /// OR-Set: an entry is `(pubkey, tag)`; a pubkey is a member iff it has ≥1 tag.
    members: BTreeSet<(Vec<u8>, Vec<u8>)>,
    log: Vec<Message>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Message {
    id: Vec<u8>,
    author: Vec<u8>,
    ts: u64,
    body: String,
}

impl ChatState {
    fn decode(bytes: &[u8]) -> ChatState {
        if bytes.is_empty() {
            return ChatState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn is_member(&self, pk: &[u8]) -> bool {
        self.members.iter().any(|(p, _)| p == pk)
    }
    fn is_empty_seed(&self) -> bool {
        self.members.is_empty() && self.log.is_empty()
    }
}

// ===== payload codec: [version u16][kind u8][content] =====

const VERSION: u16 = 0;

enum Msg {
    /// The room's distinguished ROOT: seed the initial members (valid only as the
    /// causal root). room-id = this event's id; a DM is a 2-member genesis.
    Genesis { members: Vec<Vec<u8>> },
    Text { body: String },
    MemberAdd { subject: Vec<u8> },
    MemberRemove { subject: Vec<u8> },
}

fn decode_msg(payload: &[u8]) -> Option<Msg> {
    let mut c = Cur { b: payload, p: 0 };
    if c.u16()? != VERSION {
        return None;
    }
    match c.u8()? {
        0 => Some(Msg::Genesis { members: c.keys()? }),
        1 => Some(Msg::Text { body: String::from_utf8(c.rest()).ok()? }),
        2 => Some(Msg::MemberAdd { subject: c.key()? }),
        3 => Some(Msg::MemberRemove { subject: c.key()? }),
        _ => None,
    }
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let e = self.p.checked_add(n)?;
        if e > self.b.len() {
            return None;
        }
        let s = &self.b[self.p..e];
        self.p = e;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }
    fn key(&mut self) -> Option<Vec<u8>> {
        Some(self.take(32)?.to_vec())
    }
    fn keys(&mut self) -> Option<Vec<Vec<u8>>> {
        let n = self.u16()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.key()?);
        }
        Some(out)
    }
    fn rest(&mut self) -> Vec<u8> {
        self.b[self.p..].to_vec()
    }
}

// ===== encoders (used by tests + downstream systems/harnesses) =====

/// Build a chat payload. Public codec so a system/harness encodes what it Submits.
#[allow(dead_code)]
fn encode(msg: &Msg) -> Vec<u8> {
    let mut out = VERSION.to_be_bytes().to_vec();
    match msg {
        Msg::Genesis { members } => {
            out.push(0);
            out.extend_from_slice(&(members.len() as u16).to_be_bytes());
            for m in members {
                out.extend_from_slice(m);
            }
        }
        Msg::Text { body } => {
            out.push(1);
            out.extend_from_slice(body.as_bytes());
        }
        Msg::MemberAdd { subject } => {
            out.push(2);
            out.extend_from_slice(subject);
        }
        Msg::MemberRemove { subject } => {
            out.push(3);
            out.extend_from_slice(subject);
        }
    }
    out
}

// ===== core logic (host-testable; the #[export] wrappers are thin) =====

fn do_validate(author: &[u8], payload: &[u8], state: &[u8]) -> Result<bool, String> {
    if payload.is_empty() {
        return Ok(true); // inert graft (the node's own genesis / witness)
    }
    let s = ChatState::decode(state);
    let msg = decode_msg(payload).ok_or_else(|| "undecodable chat payload".to_string())?;
    match msg {
        Msg::Genesis { .. } => {
            if s.is_empty_seed() {
                Ok(true)
            } else {
                Err("genesis after the room already exists".to_string())
            }
        }
        // v0: any member may post / add / remove (OR-Set makes add/remove race-safe).
        Msg::Text { .. } | Msg::MemberAdd { .. } | Msg::MemberRemove { .. } => {
            if s.is_member(author) {
                Ok(true)
            } else {
                Err("author is not a member of the room".to_string())
            }
        }
    }
}

fn do_apply(id: &[u8], author: &[u8], ts: u64, payload: &[u8], state: &[u8]) -> Vec<u8> {
    let mut s = ChatState::decode(state);
    let Some(msg) = decode_msg(payload) else {
        return s.encode(); // empty / undecodable → inert
    };
    match msg {
        Msg::Genesis { members } => {
            // Seed each member with the genesis event as its OR-Set tag.
            for m in members {
                s.members.insert((m, id.to_vec()));
            }
        }
        Msg::Text { body } => {
            s.log.push(Message { id: id.to_vec(), author: author.to_vec(), ts, body });
        }
        Msg::MemberAdd { subject } => {
            // OR-Set add: (subject, tag = this event's id).
            s.members.insert((subject, id.to_vec()));
        }
        Msg::MemberRemove { subject } => {
            // OR-Set observed-remove: drop every (subject, *) tag present in THIS
            // (ancestry) state. A concurrent add's tag isn't here → it survives.
            s.members.retain(|(pk, _)| pk != &subject);
        }
    }
    s.encode()
}

fn distinct_members(s: &ChatState) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (pk, _) in &s.members {
        if out.last().map(|l| l != pk).unwrap_or(true) {
            out.push(pk.clone()); // BTreeSet is sorted by pubkey → dedup adjacent
        }
    }
    out
}

// ===== the interface =====

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    ChatState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&author, &payload, &state)
}

#[export]
fn apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8> {
    do_apply(&id, &author, timestamp, &payload, &state)
}

#[export]
fn members(state: Vec<u8>) -> Vec<Vec<u8>> {
    distinct_members(&ChatState::decode(&state))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(n: u8) -> Vec<u8> {
        alloc::vec![n; 32]
    }
    fn id(n: u8) -> Vec<u8> {
        alloc::vec![0xf0 | (n & 0x0f); 32]
    }

    fn alice() -> Vec<u8> {
        k(1)
    }
    fn bob() -> Vec<u8> {
        k(2)
    }
    fn carol() -> Vec<u8> {
        k(3)
    }

    // Fold a sequence of (id, author, msg) through validate+apply from the seed,
    // applying only what validates (the node's rule).
    fn fold(seed: &[u8], evs: &[(Vec<u8>, Vec<u8>, Msg)]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for (id, author, msg) in evs {
            let p = encode(msg);
            if do_validate(author, &p, &s).is_ok() {
                s = do_apply(id, author, 0, &p, &s);
            }
        }
        s
    }

    fn genesis(ms: &[Vec<u8>]) -> Msg {
        Msg::Genesis { members: ms.to_vec() }
    }
    fn text(b: &str) -> Msg {
        Msg::Text { body: b.to_string() }
    }

    #[test]
    fn genesis_seeds_members_and_second_is_rejected() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice(), bob()]))]);
        let st = ChatState::decode(&s);
        assert!(st.is_member(&alice()) && st.is_member(&bob()));
        assert!(!st.is_member(&carol()));
        // a room already exists → a second genesis is inadmissible.
        assert!(do_validate(&alice(), &encode(&genesis(&[carol()])), &s).is_err());
    }

    #[test]
    fn text_requires_membership() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice()]))]);
        assert!(do_validate(&alice(), &encode(&text("hi")), &s).is_ok());
        assert!(do_validate(&bob(), &encode(&text("sneaky")), &s).is_err(), "non-member");
        let s2 = fold(&s, &[(id(1), alice(), text("hi"))]);
        assert_eq!(ChatState::decode(&s2).log.len(), 1);
    }

    #[test]
    fn member_add_admits_a_new_poster() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice()]))]);
        // bob can't post until added.
        assert!(do_validate(&bob(), &encode(&text("hi")), &s).is_err());
        let s2 = fold(&s, &[(id(1), alice(), Msg::MemberAdd { subject: bob() })]);
        assert!(ChatState::decode(&s2).is_member(&bob()));
        assert!(do_validate(&bob(), &encode(&text("now i can")), &s2).is_ok());
    }

    #[test]
    fn member_remove_is_observed_and_add_wins() {
        // alice adds bob (tag id(1)); a CONCURRENT add of bob carries a different
        // tag id(2). A remove that only observed id(1) must leave id(2) live.
        let s = fold(
            b"",
            &[
                (id(0), alice(), genesis(&[alice()])),
                (id(1), alice(), Msg::MemberAdd { subject: bob() }),
            ],
        );
        // the concurrent add (tag id(2)) lands in state too.
        let s_concurrent = do_apply(&id(2), &alice(), 0, &encode(&Msg::MemberAdd { subject: bob() }), &s);
        // a remove applied over THAT state drops every observed bob tag → bob gone
        // here (both tags observed). This is the linearized case.
        let s_removed = do_apply(&id(3), &alice(), 0, &encode(&Msg::MemberRemove { subject: bob() }), &s_concurrent);
        assert!(!ChatState::decode(&s_removed).is_member(&bob()), "observed remove clears bob");
        // but a remove that only saw tag id(1) (state `s`, without id(2)) leaves the
        // id(2) add to re-merge → add-wins. Model it: remove over `s`, then the
        // concurrent add re-applies.
        let s_removed_partial = do_apply(&id(3), &alice(), 0, &encode(&Msg::MemberRemove { subject: bob() }), &s);
        let s_merged = do_apply(&id(2), &alice(), 0, &encode(&Msg::MemberAdd { subject: bob() }), &s_removed_partial);
        assert!(ChatState::decode(&s_merged).is_member(&bob()), "unobserved add wins");
    }

    #[test]
    fn empty_payload_is_inert() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice()]))]);
        assert!(do_validate(&bob(), b"", &s).is_ok());
        assert_eq!(do_apply(&id(9), &bob(), 0, b"", &s), s, "empty payload does not change state");
    }

    #[test]
    fn members_query_is_distinct() {
        // bob added twice (two tags) still lists once.
        let s = fold(
            b"",
            &[
                (id(0), alice(), genesis(&[alice()])),
                (id(1), alice(), Msg::MemberAdd { subject: bob() }),
                (id(2), alice(), Msg::MemberAdd { subject: bob() }),
            ],
        );
        let m = distinct_members(&ChatState::decode(&s));
        assert_eq!(m.len(), 2, "alice + bob, deduped");
    }
}
