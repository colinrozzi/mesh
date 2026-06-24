//! DAG storage + traversal + state derivation.
//!
//! Events form a content-addressed DAG via `parent: EventHash`. The
//! genesis pubkey (root Node) is implicitly a member; the first real
//! event branches off `GENESIS_PARENT`.
//!
//! State distinguishes two member kinds: **Nodes** and **Mailboxes**.
//! Their roles differ:
//!   - Nodes may sign any op. Their Witnesses count for consensus.
//!   - Mailboxes may sign Send only. They are addressable identities,
//!     not consensus participants.
//!
//! State derivation: walk the DAG from genesis to a chosen head, sort
//! events causally (parents before children, hash-sort for siblings),
//! apply state-mutating ops.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::event::{Event, Hash, Op, PubKey, GENESIS_PARENT};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberKind {
    Node,
    Mailbox,
}

#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub kind: MemberKind,
    pub name: String,
    pub introduced_by: PubKey, // For root Node this equals its own pubkey.
}

#[derive(Clone, Debug, Default)]
pub struct State {
    pub members: BTreeMap<PubKey, MemberInfo>,
}

impl State {
    /// Genesis state: just the root Node.
    pub fn genesis(root: PubKey) -> Self {
        let mut members = BTreeMap::new();
        members.insert(
            root,
            MemberInfo {
                kind: MemberKind::Node,
                name: "root".to_string(),
                introduced_by: root,
            },
        );
        State { members }
    }

    /// Genesis state with additional pre-admitted peer Nodes (founding members).
    /// Used to bootstrap multi-node networks without a runtime NodeIntroduce dance.
    pub fn genesis_with_peers(root: PubKey, peers: &[(PubKey, String)]) -> Self {
        let mut state = Self::genesis(root);
        for (pk, name) in peers {
            if *pk == root {
                continue;
            }
            state.members.insert(
                *pk,
                MemberInfo {
                    kind: MemberKind::Node,
                    name: name.clone(),
                    introduced_by: root,
                },
            );
        }
        state
    }

    pub fn get(&self, pk: &PubKey) -> Option<&MemberInfo> {
        self.members.get(pk)
    }

    pub fn is_member(&self, pk: &PubKey) -> bool {
        self.members.contains_key(pk)
    }

    pub fn is_node(&self, pk: &PubKey) -> bool {
        matches!(self.members.get(pk), Some(m) if m.kind == MemberKind::Node)
    }
}

/// The DAG: indexed by event hash, with children index + pending buffer
/// for events whose parents arrive later.
#[derive(Clone, Debug, Default)]
pub struct Dag {
    pub root_pubkey: PubKey,
    /// Additional pre-admitted Nodes at genesis (besides root). Used to bootstrap
    /// multi-node networks. Each is (pubkey, name).
    pub peer_nodes: Vec<(PubKey, String)>,
    pub events: BTreeMap<Hash, Event>,
    pub children: BTreeMap<Hash, BTreeSet<Hash>>,
    pub pending_by_missing_parent: BTreeMap<Hash, Vec<Event>>,
}

impl Dag {
    pub fn new(root_pubkey: PubKey) -> Self {
        Self::new_with_peers(root_pubkey, Vec::new())
    }

    pub fn new_with_peers(root_pubkey: PubKey, peer_nodes: Vec<(PubKey, String)>) -> Self {
        Dag {
            root_pubkey,
            peer_nodes,
            events: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_by_missing_parent: BTreeMap::new(),
        }
    }

    fn genesis_state(&self) -> State {
        State::genesis_with_peers(self.root_pubkey, &self.peer_nodes)
    }

    /// Ingest one event. Ok(true) on accept, Ok(false) on parent-buffer,
    /// Err on hard validation failure.
    pub fn ingest(&mut self, event: Event) -> Result<bool, String> {
        event.verify_signature()?;
        let h = event.event_hash();
        if self.events.contains_key(&h) {
            return Ok(true);
        }
        let parent = event.parent;
        let parent_present = parent == GENESIS_PARENT || self.events.contains_key(&parent);
        if !parent_present {
            self.pending_by_missing_parent
                .entry(parent)
                .or_insert_with(Vec::new)
                .push(event);
            return Ok(false);
        }
        let state_at_parent = self.state_at(&parent)?;
        validate_event(&event, &state_at_parent, &self.root_pubkey)?;

        self.children
            .entry(parent)
            .or_insert_with(BTreeSet::new)
            .insert(h);
        self.events.insert(h, event);

        if let Some(waiters) = self.pending_by_missing_parent.remove(&h) {
            for w in waiters {
                let _ = self.ingest(w);
            }
        }
        Ok(true)
    }

    pub fn has(&self, h: &Hash) -> bool {
        self.events.contains_key(h)
    }

    pub fn get(&self, h: &Hash) -> Option<&Event> {
        self.events.get(h)
    }

    /// State *at* a given event (i.e. inclusive — including that event's
    /// own state-mutating op if any). Genesis returns the bare-root state.
    pub fn state_at(&self, h: &Hash) -> Result<State, String> {
        if *h == GENESIS_PARENT {
            return Ok(self.genesis_state());
        }
        let ancestors = self.ancestors_including(h)?;
        let ordered = self.topo_sort(&ancestors)?;
        let mut state = self.genesis_state();
        for eh in &ordered {
            let ev = self.events.get(eh).ok_or_else(|| "missing event in walk".to_string())?;
            apply_op(&mut state, ev);
        }
        Ok(state)
    }

    fn ancestors_including(&self, h: &Hash) -> Result<BTreeSet<Hash>, String> {
        let mut out = BTreeSet::new();
        let mut stack = alloc::vec![*h];
        while let Some(cur) = stack.pop() {
            if cur == GENESIS_PARENT { continue; }
            if !out.insert(cur) { continue; }
            let ev = self.events.get(&cur).ok_or_else(|| {
                format!("ancestors_including: missing event {}", hex(&cur))
            })?;
            stack.push(ev.parent);
        }
        Ok(out)
    }

    fn topo_sort(&self, set: &BTreeSet<Hash>) -> Result<Vec<Hash>, String> {
        let mut indeg: BTreeMap<Hash, usize> = BTreeMap::new();
        for h in set {
            let ev = self.events.get(h).ok_or_else(|| "topo: missing event".to_string())?;
            let parent_in_set = set.contains(&ev.parent);
            indeg.insert(*h, if parent_in_set { 1 } else { 0 });
        }
        let mut ready: BTreeSet<Hash> = indeg
            .iter()
            .filter_map(|(h, d)| if *d == 0 { Some(*h) } else { None })
            .collect();
        let mut out = Vec::with_capacity(set.len());
        while !ready.is_empty() {
            let cur = *ready.iter().next().unwrap();
            ready.remove(&cur);
            out.push(cur);
            if let Some(kids) = self.children.get(&cur) {
                for k in kids {
                    if let Some(d) = indeg.get_mut(k) {
                        *d -= 1;
                        if *d == 0 && set.contains(k) {
                            ready.insert(*k);
                        }
                    }
                }
            }
        }
        if out.len() != set.len() {
            return Err(format!(
                "topo: cycle or unreachable ({} sorted of {})",
                out.len(),
                set.len()
            ));
        }
        Ok(out)
    }

    pub fn leaves(&self) -> BTreeSet<Hash> {
        let mut leaves: BTreeSet<Hash> = self.events.keys().copied().collect();
        for parent in self.events.values().map(|e| &e.parent) {
            leaves.remove(parent);
        }
        leaves
    }

    // ===== Finality + consensus state ============================================
    //
    // An event E is "seen" by another event E' if E is reachable from E'
    // via either the parent chain or Witness.also_cite edges. Witnesses
    // from Nodes that see E are E's finality votes.
    //
    // E is "finalized" iff the set of Node-authors of Witnesses seeing E
    // is a superset of "current Nodes at E.parent" (the Nodes that existed
    // when E was proposed).
    //
    // Consensus state = state walked from genesis through the linear chain
    // of finalized state-changing events. Sequential consensus = one
    // state-changing event extends the chain at a time; ties broken by
    // lowest-hash.

    /// All events that transitively "see" `target` — either descendant via
    /// parent chain, OR Witness whose `also_cite` includes `target`, OR
    /// (recursively) a Witness whose `also_cite` includes such an event.
    pub fn events_that_see(&self, target: &Hash) -> BTreeSet<Hash> {
        let mut seen: BTreeSet<Hash> = BTreeSet::from([*target]);
        loop {
            let mut grew = false;
            for (h, ev) in &self.events {
                if seen.contains(h) {
                    continue;
                }
                let via_parent = seen.contains(&ev.parent);
                let via_cite = match &ev.op {
                    Op::Witness { also_cite } => also_cite.iter().any(|c| seen.contains(c)),
                    _ => false,
                };
                if via_parent || via_cite {
                    seen.insert(*h);
                    grew = true;
                }
            }
            if !grew {
                break;
            }
        }
        seen
    }

    /// Node-pubkeys among Witnesses in `events_that_see(target)`.
    pub fn witnessing_nodes(&self, target: &Hash) -> BTreeSet<PubKey> {
        let seen = self.events_that_see(target);
        let mut out = BTreeSet::new();
        for h in &seen {
            if let Some(ev) = self.events.get(h) {
                if matches!(ev.op, Op::Witness { .. }) {
                    out.insert(ev.author);
                }
            }
        }
        out
    }

    /// True iff every current Node (at target's parent state) has signed
    /// a Witness that transitively references target.
    pub fn is_finalized(&self, target: &Hash) -> bool {
        let parent = match self.events.get(target) {
            Some(e) => e.parent,
            None => return false,
        };
        let state_at_parent = match self.state_at(&parent) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let current_nodes: BTreeSet<PubKey> = state_at_parent
            .members
            .iter()
            .filter(|(_, info)| info.kind == MemberKind::Node)
            .map(|(pk, _)| *pk)
            .collect();
        let witnesses = self.witnessing_nodes(target);
        current_nodes.is_subset(&witnesses)
    }

    /// True if op is a state-changing op (NodeIntroduce, MailboxCreate, Revoke).
    pub fn is_state_mutating(op: &Op) -> bool {
        matches!(
            op,
            Op::NodeIntroduce { .. } | Op::MailboxCreate { .. } | Op::Revoke { .. }
        )
    }

    /// Walk forward from genesis through the linear chain of finalized
    /// state-changing events. Ties broken by lowest event-hash. Returns the
    /// current state-DAG head (or GENESIS_PARENT if no state changes yet).
    pub fn consensus_state_head(&self) -> Hash {
        let mut head = GENESIS_PARENT;
        loop {
            let children = match self.children.get(&head) {
                Some(c) => c,
                None => break,
            };
            let mut candidates: Vec<Hash> = children
                .iter()
                .filter(|h| {
                    let ev = match self.events.get(*h) {
                        Some(e) => e,
                        None => return false,
                    };
                    Self::is_state_mutating(&ev.op) && self.is_finalized(h)
                })
                .copied()
                .collect();
            if candidates.is_empty() {
                break;
            }
            candidates.sort();
            head = candidates[0];
        }
        head
    }

    /// The current consensus state — state walked through the state-DAG
    /// from genesis to consensus head.
    pub fn consensus_state(&self) -> State {
        self.state_at(&self.consensus_state_head())
            .unwrap_or_else(|_| self.genesis_state())
    }
}

fn apply_op(state: &mut State, ev: &Event) {
    match &ev.op {
        Op::NodeIntroduce { subject, name } => {
            if !state.members.contains_key(subject) {
                state.members.insert(
                    *subject,
                    MemberInfo {
                        kind: MemberKind::Node,
                        name: name.clone(),
                        introduced_by: ev.author,
                    },
                );
            }
        }
        Op::MailboxCreate { subject, name } => {
            if !state.members.contains_key(subject) {
                state.members.insert(
                    *subject,
                    MemberInfo {
                        kind: MemberKind::Mailbox,
                        name: name.clone(),
                        introduced_by: ev.author,
                    },
                );
            }
        }
        Op::Revoke { subject } => {
            let is_root = state
                .members
                .get(subject)
                .map(|m| m.name.as_str())
                == Some("root");
            if !is_root {
                state.members.remove(subject);
            }
        }
        Op::Send { .. } | Op::Witness { .. } => {
            // No state change.
        }
    }
}

fn validate_event(
    event: &Event,
    state_at_parent: &State,
    root_pubkey: &PubKey,
) -> Result<(), String> {
    // Genesis-only special case: the first event after the genesis sentinel
    // must come from the root Node.
    if event.parent == GENESIS_PARENT && event.author != *root_pubkey {
        return Err("only root may author the first event off genesis".to_string());
    }
    if !state_at_parent.is_member(&event.author) {
        return Err(format!("author {} not a member at parent", hex(&event.author)));
    }
    match &event.op {
        Op::NodeIntroduce { subject, .. } => {
            require_node(state_at_parent, &event.author, "NodeIntroduce")?;
            if state_at_parent.is_member(subject) {
                return Err(format!("NodeIntroduce: {} already a member", hex(subject)));
            }
        }
        Op::MailboxCreate { subject, .. } => {
            require_node(state_at_parent, &event.author, "MailboxCreate")?;
            if state_at_parent.is_member(subject) {
                return Err(format!("MailboxCreate: {} already a member", hex(subject)));
            }
        }
        Op::Revoke { subject } => {
            require_node(state_at_parent, &event.author, "Revoke")?;
            if !state_at_parent.is_member(subject) {
                return Err(format!("Revoke: {} not a member", hex(subject)));
            }
            let is_root = state_at_parent
                .members
                .get(subject)
                .map(|m| m.name.as_str())
                == Some("root");
            if is_root {
                return Err("Revoke: cannot revoke root".to_string());
            }
        }
        Op::Send { recipient, .. } => {
            // Send may be authored by any member (Node OR Mailbox).
            if !state_at_parent.is_member(recipient) {
                return Err(format!("Send: recipient {} not a member", hex(recipient)));
            }
        }
        Op::Witness { .. } => {
            // Witness must be authored by a Node — Mailboxes may not witness.
            require_node(state_at_parent, &event.author, "Witness")?;
        }
    }
    Ok(())
}

fn require_node(state: &State, pk: &PubKey, op_name: &str) -> Result<(), String> {
    if state.is_node(pk) {
        Ok(())
    } else {
        Err(format!("{}: author {} is not a Node", op_name, hex(pk)))
    }
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
}
