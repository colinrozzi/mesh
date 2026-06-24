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

use crate::codec::hex;
use crate::event::{Event, Hash, Op, PubKey, GENESIS_PARENT};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberKind {
    Node,
    Mailbox,
}

#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub kind: MemberKind,
    /// Human label carried by the NodeIntroduce/MailboxCreate op. Captured
    /// from the wire protocol; not yet surfaced by any query path.
    #[allow(dead_code)]
    pub name: String,
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
                },
            );
        }
        state
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
    pub fn new(root_pubkey: PubKey, peer_nodes: Vec<(PubKey, String)>) -> Self {
        Dag {
            root_pubkey,
            peer_nodes,
            events: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_by_missing_parent: BTreeMap::new(),
        }
    }

    /// Rebuild from persisted, already-validated events: insert each one and
    /// rebuild the children index. Unlike `ingest`, this skips signature and
    /// rule checks — the events passed validation when first received, so
    /// reloading our own store is O(n) inserts rather than a full
    /// re-verification on every callback.
    pub fn rehydrate(
        root_pubkey: PubKey,
        peer_nodes: Vec<(PubKey, String)>,
        events: Vec<Event>,
    ) -> Self {
        let mut dag = Self::new(root_pubkey, peer_nodes);
        for ev in events {
            let h = ev.event_hash();
            dag.children.entry(ev.parent).or_default().insert(h);
            dag.events.insert(h, ev);
        }
        dag
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
            apply_op(&mut state, ev, &self.root_pubkey);
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
    ///
    /// Computed as a reverse-reachability BFS: build the "observed-by" graph
    /// (for each event, who points at it via parent or also_cite), then walk
    /// outward from `target`. O(events + edges) rather than the O(events²) of
    /// a repeated fixpoint scan.
    pub fn events_that_see(&self, target: &Hash) -> BTreeSet<Hash> {
        // observed_by[x] = events that *directly* see x (x is their parent, or
        // a Witness cites x).
        let mut observed_by: BTreeMap<Hash, Vec<Hash>> = BTreeMap::new();
        for (h, ev) in &self.events {
            observed_by.entry(ev.parent).or_default().push(*h);
            if let Op::Witness { also_cite } = &ev.op {
                for cited in also_cite {
                    observed_by.entry(*cited).or_default().push(*h);
                }
            }
        }

        let mut seen: BTreeSet<Hash> = BTreeSet::from([*target]);
        let mut frontier = alloc::vec![*target];
        while let Some(cur) = frontier.pop() {
            if let Some(observers) = observed_by.get(&cur) {
                for &obs in observers {
                    if seen.insert(obs) {
                        frontier.push(obs);
                    }
                }
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

    /// The Node-pubkeys present in the state at event `h` — i.e. the Nodes
    /// whose Witnesses are required to finalize `h`'s children. Returns None if
    /// the state can't be derived (e.g. a missing ancestor).
    fn nodes_at(&self, h: &Hash) -> Option<BTreeSet<PubKey>> {
        let state = self.state_at(h).ok()?;
        Some(
            state
                .members
                .iter()
                .filter(|(_, info)| info.kind == MemberKind::Node)
                .map(|(pk, _)| *pk)
                .collect(),
        )
    }

    /// Whether `target` is witnessed by every Node in `required`.
    fn has_required_witnesses(&self, target: &Hash, required: &BTreeSet<PubKey>) -> bool {
        required.is_subset(&self.witnessing_nodes(target))
    }

    /// True iff every current Node (at target's parent state) has signed
    /// a Witness that transitively references target. Public consensus-API
    /// surface (see README); `consensus_state_head` inlines the equivalent
    /// check with the Node set hoisted per level.
    #[allow(dead_code)]
    pub fn is_finalized(&self, target: &Hash) -> bool {
        let parent = match self.events.get(target) {
            Some(e) => e.parent,
            None => return false,
        };
        match self.nodes_at(&parent) {
            Some(required) => self.has_required_witnesses(target, &required),
            None => false,
        }
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
            // The Nodes required to finalize any child are those present at
            // `head` (the children's shared parent) — compute once per level.
            let Some(required) = self.nodes_at(&head) else {
                break;
            };
            // `children` is a BTreeSet, so iteration is hash-ascending; the
            // first finalized state-changing child is the lowest-hash winner.
            let next = children.iter().find(|h| {
                self.events.get(*h).is_some_and(|ev| {
                    Self::is_state_mutating(&ev.op)
                        && self.has_required_witnesses(h, &required)
                })
            });
            match next {
                Some(h) => head = *h,
                None => break,
            }
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

fn apply_op(state: &mut State, ev: &Event, root_pubkey: &PubKey) {
    match &ev.op {
        Op::NodeIntroduce { subject, name } => {
            state.members.entry(*subject).or_insert_with(|| MemberInfo {
                kind: MemberKind::Node,
                name: name.clone(),
            });
        }
        Op::MailboxCreate { subject, name } => {
            state.members.entry(*subject).or_insert_with(|| MemberInfo {
                kind: MemberKind::Mailbox,
                name: name.clone(),
            });
        }
        Op::Revoke { subject } => {
            // Root is identified by pubkey, not by the "root" name label.
            if subject != root_pubkey {
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
            if subject == root_pubkey {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Op};
    use ed25519_dalek::{Signer, SigningKey};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pk(sk: &SigningKey) -> PubKey {
        sk.verifying_key().to_bytes()
    }

    fn signed(sk: &SigningKey, parent: Hash, op: Op) -> Event {
        let author = pk(sk);
        let signing_hash = Event::signing_hash(&parent, &author, &op);
        Event { parent, author, op, signature: sk.sign(&signing_hash).to_bytes() }
    }

    /// Ingest `op` from `author` at the current consensus head, then have the
    /// root witness it so single-node consensus finalizes. Returns the event hash.
    fn commit(dag: &mut Dag, root: &SigningKey, author: &SigningKey, op: Op) -> Hash {
        let head = dag.consensus_state_head();
        let ev = signed(author, head, op);
        let h = ev.event_hash();
        dag.ingest(ev).expect("ingest event");
        let witness = signed(root, h, Op::Witness { also_cite: alloc::vec![h] });
        dag.ingest(witness).expect("ingest witness");
        h
    }

    #[test]
    fn genesis_has_root_as_node() {
        let root = key(1);
        let dag = Dag::new(pk(&root), Vec::new());
        let state = dag.consensus_state();
        assert!(state.is_member(&pk(&root)));
        assert!(state.is_node(&pk(&root)));
    }

    #[test]
    fn genesis_with_peers_admits_peer_nodes() {
        let root = key(1);
        let peer = key(2);
        let dag = Dag::new(pk(&root), alloc::vec![(pk(&peer), "node-b".to_string())]);
        let state = dag.consensus_state();
        assert!(state.is_node(&pk(&peer)));
    }

    #[test]
    fn create_mailbox_needs_a_witness_to_finalize() {
        let root = key(1);
        let alice = key(7);
        let mut dag = Dag::new(pk(&root), Vec::new());

        let create = signed(&root, GENESIS_PARENT, Op::MailboxCreate {
            subject: pk(&alice),
            name: "alice".to_string(),
        });
        let h = create.event_hash();
        dag.ingest(create).expect("ingest create");

        // Not finalized before any witness; alice not yet in consensus state.
        assert!(!dag.is_finalized(&h));
        assert!(!dag.consensus_state().is_member(&pk(&alice)));

        // Root witnesses -> finalized and applied.
        let witness = signed(&root, h, Op::Witness { also_cite: alloc::vec![h] });
        dag.ingest(witness).expect("ingest witness");
        assert!(dag.is_finalized(&h));
        assert_eq!(dag.consensus_state_head(), h);
        let state = dag.consensus_state();
        assert!(state.is_member(&pk(&alice)));
        assert!(!state.is_node(&pk(&alice))); // it's a Mailbox
    }

    #[test]
    fn first_event_off_genesis_must_be_root() {
        let root = key(1);
        let stranger = key(9);
        let mut dag = Dag::new(pk(&root), Vec::new());
        let ev = signed(&stranger, GENESIS_PARENT, Op::Send {
            recipient: pk(&root),
            payload: Vec::new(),
        });
        assert!(dag.ingest(ev).is_err());
    }

    #[test]
    fn mailbox_cannot_introduce_or_witness() {
        let root = key(1);
        let alice = key(7);
        let mut dag = Dag::new(pk(&root), Vec::new());
        commit(&mut dag, &root, &root, Op::MailboxCreate {
            subject: pk(&alice),
            name: "alice".to_string(),
        });
        let head = dag.consensus_state_head();

        // A Mailbox may not author NodeIntroduce...
        let intro = signed(&alice, head, Op::NodeIntroduce {
            subject: pk(&key(8)),
            name: "x".to_string(),
        });
        assert!(dag.ingest(intro).is_err());

        // ...nor a Witness.
        let witness = signed(&alice, head, Op::Witness { also_cite: Vec::new() });
        assert!(dag.ingest(witness).is_err());

        // ...but it MAY author a Send to a member.
        let send = signed(&alice, head, Op::Send { recipient: pk(&root), payload: b"hi".to_vec() });
        assert!(dag.ingest(send).is_ok());
    }

    #[test]
    fn send_to_non_member_is_rejected() {
        let root = key(1);
        let mut dag = Dag::new(pk(&root), Vec::new());
        let send = signed(&root, GENESIS_PARENT, Op::Send {
            recipient: pk(&key(42)),
            payload: Vec::new(),
        });
        assert!(dag.ingest(send).is_err());
    }

    #[test]
    fn root_cannot_be_revoked() {
        let root = key(1);
        let mut dag = Dag::new(pk(&root), Vec::new());
        let revoke = signed(&root, GENESIS_PARENT, Op::Revoke { subject: pk(&root) });
        assert!(dag.ingest(revoke).is_err());
    }

    /// Regression: root protection is by pubkey, not by the "root" name label.
    /// A Mailbox literally named "root" must still be revocable.
    #[test]
    fn mailbox_named_root_is_still_revocable() {
        let root = key(1);
        let impostor = key(5);
        let mut dag = Dag::new(pk(&root), Vec::new());

        commit(&mut dag, &root, &root, Op::MailboxCreate {
            subject: pk(&impostor),
            name: "root".to_string(),
        });
        assert!(dag.consensus_state().is_member(&pk(&impostor)));

        commit(&mut dag, &root, &root, Op::Revoke { subject: pk(&impostor) });
        assert!(!dag.consensus_state().is_member(&pk(&impostor)));
    }

    #[test]
    fn sequential_state_changes_apply_in_order() {
        let root = key(1);
        let a = key(7);
        let b = key(8);
        let mut dag = Dag::new(pk(&root), Vec::new());
        commit(&mut dag, &root, &root, Op::MailboxCreate { subject: pk(&a), name: "a".to_string() });
        commit(&mut dag, &root, &root, Op::NodeIntroduce { subject: pk(&b), name: "b".to_string() });
        let state = dag.consensus_state();
        assert!(state.is_member(&pk(&a)));
        assert!(state.is_node(&pk(&b)));
    }

    #[test]
    fn witness_of_witness_is_seen_transitively() {
        let root = key(1);
        let mut dag = Dag::new(pk(&root), Vec::new());
        let create = signed(&root, GENESIS_PARENT, Op::MailboxCreate {
            subject: pk(&key(7)),
            name: "a".to_string(),
        });
        let ch = create.event_hash();
        dag.ingest(create).unwrap();

        // w1 cites create; w2 cites w1 (not create directly).
        let w1 = signed(&root, ch, Op::Witness { also_cite: alloc::vec![ch] });
        let w1h = w1.event_hash();
        dag.ingest(w1).unwrap();
        let w2 = signed(&root, w1h, Op::Witness { also_cite: alloc::vec![w1h] });
        let w2h = w2.event_hash();
        dag.ingest(w2).unwrap();

        // create is transitively seen, and root counts as a witnessing node.
        assert!(dag.events_that_see(&ch).contains(&w2h));
        assert!(dag.witnessing_nodes(&ch).contains(&pk(&root)));
    }
}
