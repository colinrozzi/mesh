//! `chat-sm` — the messaging chat plane as an RSM `state-machine` component.
//!
//! Interface 1 of DESIGN-rsm.md (`initial-state`/`validate`/`apply`/`members`):
//! **OR-Set** membership + an append-only text log. Pure, deterministic, structure-blind.
//!
//! **Fully conflict-free**: every kind commutes, so every event is admission-final. The
//! load-bearing choice is that membership is an **observed-remove (OR-Set)** — concurrent
//! add/remove of the same member commutes (add-wins). The OR-Set tag is the sm-event `id`.
//! `member-remove` deletes only the `(subject, tag)` pairs in the event's ANCESTRY state,
//! so a concurrent add survives — add-wins from ancestry-relative validity.
//!
//! The payload arrives **typed** as `Msg`, generated from the shared `chat.wit` via
//! `wit!(from …)` — no protocol crate, no `decode` in the SM (the node is generic over the
//! payload `p`). State is opaque bytes (serde).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::export;
use serde::{Deserialize, Serialize};

#[cfg(not(test))]
packr_guest::setup_guest!();

// `Msg` — the chat payload, generated from the shared `chat.wit` (one source of truth).
packr_guest::wit!(from "../chat.wit");

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: msg, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: msg, state: list<u8>) -> list<u8>,
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

// ===== core logic (host-testable) — payload arrives TYPED as `Msg`, no decode =====

fn do_validate(author: &[u8], msg: &Msg, state: &[u8]) -> Result<bool, String> {
    let s = ChatState::decode(state);
    match msg {
        Msg::Genesis(_) => {
            if s.is_empty_seed() {
                Ok(true)
            } else {
                Err("genesis after the room already exists".to_string())
            }
        }
        // v0: any member may post / add / remove (OR-Set makes add/remove race-safe).
        Msg::Text(_) | Msg::MemberAdd(_) | Msg::MemberRemove(_) => {
            if s.is_member(author) {
                Ok(true)
            } else {
                Err("author is not a member of the room".to_string())
            }
        }
    }
}

fn do_apply(id: &[u8], author: &[u8], ts: u64, msg: Msg, state: &[u8]) -> Vec<u8> {
    let mut s = ChatState::decode(state);
    match msg {
        Msg::Genesis(members) => {
            for m in members {
                s.members.insert((m, id.to_vec()));
            }
        }
        Msg::Text(body) => {
            s.log.push(Message { id: id.to_vec(), author: author.to_vec(), ts, body });
        }
        Msg::MemberAdd(subject) => {
            s.members.insert((subject, id.to_vec()));
        }
        Msg::MemberRemove(subject) => {
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
fn validate(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Msg, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&author, &payload, &state)
}

#[export]
fn apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Msg, state: Vec<u8>) -> Vec<u8> {
    do_apply(&id, &author, timestamp, payload, &state)
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

    /// Fold (id, author, msg) through validate+apply, applying only what validates.
    fn fold(seed: &[u8], evs: &[(Vec<u8>, Vec<u8>, Msg)]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for (id, author, msg) in evs {
            if do_validate(author, msg, &s).is_ok() {
                s = do_apply(id, author, 0, msg.clone(), &s);
            }
        }
        s
    }

    fn genesis(ms: &[Vec<u8>]) -> Msg {
        Msg::Genesis(ms.to_vec())
    }
    fn text(b: &str) -> Msg {
        Msg::Text(b.to_string())
    }

    #[test]
    fn genesis_seeds_members_and_second_is_rejected() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice(), bob()]))]);
        let st = ChatState::decode(&s);
        assert!(st.is_member(&alice()) && st.is_member(&bob()));
        assert!(!st.is_member(&carol()));
        assert!(do_validate(&alice(), &genesis(&[carol()]), &s).is_err());
    }

    #[test]
    fn text_requires_membership() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice()]))]);
        assert!(do_validate(&alice(), &text("hi"), &s).is_ok());
        assert!(do_validate(&bob(), &text("sneaky"), &s).is_err(), "non-member");
        let s2 = fold(&s, &[(id(1), alice(), text("hi"))]);
        assert_eq!(ChatState::decode(&s2).log.len(), 1);
    }

    #[test]
    fn member_add_admits_a_new_poster() {
        let s = fold(b"", &[(id(0), alice(), genesis(&[alice()]))]);
        assert!(do_validate(&bob(), &text("hi"), &s).is_err());
        let s2 = fold(&s, &[(id(1), alice(), Msg::MemberAdd(bob()))]);
        assert!(ChatState::decode(&s2).is_member(&bob()));
        assert!(do_validate(&bob(), &text("now i can"), &s2).is_ok());
    }

    #[test]
    fn member_remove_is_observed_and_add_wins() {
        let s = fold(
            b"",
            &[
                (id(0), alice(), genesis(&[alice()])),
                (id(1), alice(), Msg::MemberAdd(bob())),
            ],
        );
        let s_concurrent = do_apply(&id(2), &alice(), 0, Msg::MemberAdd(bob()), &s);
        let s_removed = do_apply(&id(3), &alice(), 0, Msg::MemberRemove(bob()), &s_concurrent);
        assert!(!ChatState::decode(&s_removed).is_member(&bob()), "observed remove clears bob");
        let s_removed_partial = do_apply(&id(3), &alice(), 0, Msg::MemberRemove(bob()), &s);
        let s_merged = do_apply(&id(2), &alice(), 0, Msg::MemberAdd(bob()), &s_removed_partial);
        assert!(ChatState::decode(&s_merged).is_member(&bob()), "unobserved add wins");
    }

    #[test]
    fn members_query_is_distinct() {
        let s = fold(
            b"",
            &[
                (id(0), alice(), genesis(&[alice()])),
                (id(1), alice(), Msg::MemberAdd(bob())),
                (id(2), alice(), Msg::MemberAdd(bob())),
            ],
        );
        let m = distinct_members(&ChatState::decode(&s));
        assert_eq!(m.len(), 2, "alice + bob, deduped");
    }
}
