//! DAG storage + finality, over a multi-parent event graph with dynamic
//! membership.
//!
//! Each event has back-edges `self_parent ∪ refs` (see event.rs). Membership is
//! **derived**: it starts from a configured genesis set and evolves as
//! `Introduce`/`Depart` system events finalize. The substrate interprets those
//! ops directly (it must — finality depends on the member set). See DESIGN.md.
//!
//! Two member-set views:
//!   - `members_at_frontier(deps)` — members *live at a point*, folded from the
//!     ancestry reachable from `deps`. Used to validate an event and to decide
//!     which members must witness it for finality.
//!   - `consensus_members()` — members folded over the *finalized* order. The
//!     agreed current set; used for the membership-gated handshake.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::hex;
use crate::event::{Event, Hash, PubKey, SystemOp};

/// The back-edges of an event: its `self_parent` (if any) and its `refs`,
/// de-duplicated (an honest event won't repeat, but decode permits it).
fn dep_set(ev: &Event) -> BTreeSet<Hash> {
    let mut deps = BTreeSet::new();
    if let Some(sp) = ev.self_parent {
        deps.insert(sp);
    }
    for r in &ev.refs {
        deps.insert(*r);
    }
    deps
}

#[derive(Clone, Debug)]
pub struct Dag {
    /// The configured bootstrap member set — the network's members at genesis,
    /// before any Introduce/Depart. The live set is derived from here.
    pub genesis_members: BTreeSet<PubKey>,
    pub events: BTreeMap<Hash, Event>,
    /// Reverse adjacency: for each event hash, the events that directly
    /// reference it (via `self_parent` or `refs`) — "who observes me."
    observed_by: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Events held until a missing dependency arrives, keyed by one missing
    /// dependency hash. When that hash lands, the waiters are re-ingested.
    pending: BTreeMap<Hash, Vec<Event>>,
}

impl Dag {
    pub fn new(genesis_members: BTreeSet<PubKey>) -> Self {
        Dag {
            genesis_members,
            events: BTreeMap::new(),
            observed_by: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    /// Rebuild from persisted, already-validated events.
    pub fn rehydrate(genesis_members: BTreeSet<PubKey>, events: Vec<Event>) -> Self {
        let mut dag = Self::new(genesis_members);
        for ev in events {
            let h = ev.event_hash();
            dag.insert(h, ev);
        }
        dag
    }

    /// Ingest one event. `Ok(true)` on accept (or already-present),
    /// `Ok(false)` when buffered pending a missing dependency, `Err` on hard
    /// validation failure.
    pub fn ingest(&mut self, event: Event) -> Result<bool, String> {
        event.verify_signature()?;
        let h = event.event_hash();
        if self.events.contains_key(&h) {
            return Ok(true); // dedup — idempotent
        }

        // Buffer until every dependency (self_parent + refs) is present.
        let deps: Vec<Hash> = dep_set(&event).into_iter().collect();
        if let Some(missing) = deps.iter().find(|d| !self.events.contains_key(*d)) {
            self.pending.entry(*missing).or_default().push(event);
            return Ok(false);
        }

        // Members live *at this event's position* — folded from its ancestry.
        let members = self.members_at_frontier(&deps);
        if !members.contains(&event.author) {
            return Err(format!("author {} is not a member", hex(&event.author)));
        }

        // self_parent must be one of the author's own events.
        if let Some(sp) = event.self_parent {
            if self.events.get(&sp).map(|p| p.author) != Some(event.author) {
                return Err("self_parent must be authored by the same node".into());
            }
        }

        // Membership-op rules.
        if let Some(op) = &event.system {
            match op {
                SystemOp::Introduce { node } => {
                    if members.contains(node) {
                        return Err(format!("Introduce: {} already a member", hex(node)));
                    }
                }
                SystemOp::Depart { node } => {
                    if !members.contains(node) {
                        return Err(format!("Depart: {} not a member", hex(node)));
                    }
                    if *node != event.author {
                        return Err("Depart must be self-authored".into());
                    }
                }
            }
        }

        self.insert(h, event);

        if let Some(waiters) = self.pending.remove(&h) {
            for w in waiters {
                let _ = self.ingest(w);
            }
        }
        Ok(true)
    }

    fn insert(&mut self, h: Hash, event: Event) {
        for dep in dep_set(&event) {
            self.observed_by.entry(dep).or_default().insert(h);
        }
        self.events.insert(h, event);
    }

    pub fn has(&self, h: &Hash) -> bool {
        self.events.contains_key(h)
    }

    /// Events buffered awaiting a missing dependency. Persisted across callbacks.
    pub fn pending_events(&self) -> Vec<Event> {
        self.pending.values().flatten().cloned().collect()
    }

    /// The author's head(s): events authored by `author` that no other event of
    /// that author builds on. Exactly one under honest operation; a set if forked.
    pub fn heads_of(&self, author: &PubKey) -> BTreeSet<Hash> {
        let mut heads = BTreeSet::new();
        for (h, ev) in &self.events {
            if ev.author != *author {
                continue;
            }
            let extended = self.observed_by.get(h).is_some_and(|obs| {
                obs.iter().any(|o| {
                    self.events
                        .get(o)
                        .is_some_and(|e| e.author == *author && e.self_parent == Some(*h))
                })
            });
            if !extended {
                heads.insert(*h);
            }
        }
        heads
    }

    // ===== Membership derivation ===============================================

    /// All events reachable from `frontier` via back-edges (frontier included).
    fn ancestors_of(&self, frontier: &[Hash]) -> BTreeSet<Hash> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<Hash> = frontier.to_vec();
        while let Some(cur) = stack.pop() {
            if !self.events.contains_key(&cur) || !seen.insert(cur) {
                continue;
            }
            if let Some(ev) = self.events.get(&cur) {
                stack.extend(dep_set(ev));
            }
        }
        seen
    }

    /// Members live at a point whose causal past is `frontier` — genesis members
    /// with every Introduce/Depart in that ancestry folded in canonical order.
    /// Structural (not finality-gated): a pure function of the DAG, so every
    /// node agrees.
    fn members_at_frontier(&self, frontier: &[Hash]) -> BTreeSet<PubKey> {
        let ancestors = self.ancestors_of(frontier);
        let ordered = self.topo_sort(&ancestors);
        let mut members = self.genesis_members.clone();
        for h in ordered {
            match self.events.get(&h).and_then(|e| e.system.as_ref()) {
                Some(SystemOp::Introduce { node }) => {
                    members.insert(*node);
                }
                Some(SystemOp::Depart { node }) => {
                    members.remove(node);
                }
                None => {}
            }
        }
        members
    }

    /// The agreed current member set — genesis members with every *finalized*
    /// membership op folded in canonical order. Used for the handshake.
    pub fn consensus_members(&self) -> BTreeSet<PubKey> {
        let mut members = self.genesis_members.clone();
        for h in self.ordered_finalized() {
            match self.events.get(&h).and_then(|e| e.system.as_ref()) {
                Some(SystemOp::Introduce { node }) => {
                    members.insert(*node);
                }
                Some(SystemOp::Depart { node }) => {
                    members.remove(node);
                }
                None => {}
            }
        }
        members
    }

    // ===== Finality ============================================================
    //
    // E is finalized iff every member *live at E's position* has authored an
    // event that sees E. `members_at_frontier(E's deps)` gives that set; note it
    // excludes E's own membership effect, so an Introduce doesn't require the new
    // node to witness its own admission, while a Depart still requires the
    // departing node (which supplies its own witness by authoring it).

    /// All events that transitively see `target` (target included).
    pub fn events_that_see(&self, target: &Hash) -> BTreeSet<Hash> {
        let mut seen = BTreeSet::from([*target]);
        let mut frontier = alloc::vec![*target];
        while let Some(cur) = frontier.pop() {
            if let Some(observers) = self.observed_by.get(&cur) {
                for &o in observers {
                    if seen.insert(o) {
                        frontier.push(o);
                    }
                }
            }
        }
        seen
    }

    /// Authors of events that see `target`.
    fn witnessing_authors(&self, target: &Hash) -> BTreeSet<PubKey> {
        self.events_that_see(target)
            .iter()
            .filter_map(|h| self.events.get(h).map(|e| e.author))
            .collect()
    }

    /// True iff every member live at `target`'s position has witnessed it.
    pub fn is_finalized(&self, target: &Hash) -> bool {
        let Some(ev) = self.events.get(target) else {
            return false;
        };
        let deps: Vec<Hash> = dep_set(ev).into_iter().collect();
        let required = self.members_at_frontier(&deps);
        required.is_subset(&self.witnessing_authors(target))
    }

    /// Finalized events in canonical order (topo-sort, hash tiebreak) — the
    /// reducer's input. Ancestry-closed. Re-folds each call; incremental
    /// application is a near-term optimization (DESIGN.md).
    pub fn ordered_finalized(&self) -> Vec<Hash> {
        let finalized: BTreeSet<Hash> = self
            .events
            .keys()
            .copied()
            .filter(|h| self.is_finalized(h))
            .collect();
        self.topo_sort(&finalized)
    }

    fn topo_sort(&self, set: &BTreeSet<Hash>) -> Vec<Hash> {
        let mut indeg: BTreeMap<Hash, usize> = BTreeMap::new();
        for h in set {
            let ev = &self.events[h];
            let d = dep_set(ev).iter().filter(|x| set.contains(*x)).count();
            indeg.insert(*h, d);
        }
        let mut ready: BTreeSet<Hash> =
            indeg.iter().filter(|(_, d)| **d == 0).map(|(h, _)| *h).collect();
        let mut out = Vec::with_capacity(set.len());
        while let Some(&cur) = ready.iter().next() {
            ready.remove(&cur);
            out.push(cur);
            if let Some(observers) = self.observed_by.get(&cur) {
                for o in observers {
                    if let Some(d) = indeg.get_mut(o) {
                        *d -= 1;
                        if *d == 0 {
                            ready.insert(*o);
                        }
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pk(sk: &SigningKey) -> PubKey {
        sk.verifying_key().to_bytes()
    }

    fn members(sks: &[&SigningKey]) -> BTreeSet<PubKey> {
        sks.iter().map(|sk| pk(sk)).collect()
    }

    fn ev(sk: &SigningKey, sp: Option<Hash>, refs: Vec<Hash>, payload: Vec<u8>) -> Event {
        Event::sign(sk, sp, refs, payload, None)
    }

    fn sys(sk: &SigningKey, sp: Option<Hash>, refs: Vec<Hash>, op: SystemOp) -> Event {
        Event::sign(sk, sp, refs, Vec::new(), Some(op))
    }

    #[test]
    fn single_member_finalizes_its_own_genesis() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let g = ev(&a, None, Vec::new(), Vec::new());
        let gh = g.event_hash();
        assert!(dag.ingest(g).unwrap());
        assert!(dag.is_finalized(&gh));
        assert_eq!(dag.ordered_finalized(), alloc::vec![gh]);
    }

    #[test]
    fn two_members_finalize_via_mutual_grafting() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();
        assert!(!dag.is_finalized(&gah));

        let a1 = ev(&a, Some(gah), alloc::vec![gbh], Vec::new());
        let b1 = ev(&b, Some(gbh), alloc::vec![gah], Vec::new());
        dag.ingest(a1).unwrap();
        dag.ingest(b1).unwrap();
        assert!(dag.is_finalized(&gah));
        assert!(dag.is_finalized(&gbh));
    }

    #[test]
    fn rejects_non_member_author() {
        let a = key(1);
        let stranger = key(9);
        let mut dag = Dag::new(members(&[&a]));
        assert!(dag.ingest(ev(&stranger, None, Vec::new(), Vec::new())).is_err());
    }

    #[test]
    fn rejects_self_parent_from_another_author() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        assert!(dag.ingest(ev(&b, Some(gah), Vec::new(), Vec::new())).is_err());
    }

    #[test]
    fn buffers_then_admits_when_dependency_arrives() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let gbh = gb.event_hash();
        let a1 = ev(&a, Some(gah), alloc::vec![gbh], Vec::new());
        let a1h = a1.event_hash();
        assert_eq!(dag.ingest(a1).unwrap(), false);
        assert!(!dag.has(&a1h));
        dag.ingest(gb).unwrap();
        assert!(dag.has(&a1h));
    }

    #[test]
    fn forks_are_admitted_not_rejected() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        let f1 = ev(&a, Some(gah), Vec::new(), b"one".to_vec());
        let f2 = ev(&a, Some(gah), Vec::new(), b"two".to_vec());
        assert!(dag.ingest(f1.clone()).unwrap());
        assert!(dag.ingest(f2.clone()).unwrap());
        assert_eq!(dag.heads_of(&pk(&a)).len(), 2);
    }

    #[test]
    fn finality_requires_all_members() {
        let a = key(1);
        let b = key(2);
        let c = key(3);
        let mut dag = Dag::new(members(&[&a, &b, &c]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let gc = ev(&c, None, Vec::new(), Vec::new());
        let (gah, gbh, gch) = (ga.event_hash(), gb.event_hash(), gc.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();
        dag.ingest(gc).unwrap();
        let m = ev(&a, Some(gah), alloc::vec![gbh, gch], b"x".to_vec());
        let mh = m.event_hash();
        dag.ingest(m).unwrap();
        assert!(!dag.is_finalized(&mh));
        dag.ingest(ev(&b, Some(gbh), alloc::vec![mh], Vec::new())).unwrap();
        assert!(!dag.is_finalized(&mh));
        dag.ingest(ev(&c, Some(gch), alloc::vec![mh], Vec::new())).unwrap();
        assert!(dag.is_finalized(&mh));
    }

    // ----- dynamic membership -----

    /// Everyone in `current` grafts `target` so it finalizes. `heads` maps each
    /// signer to its current head. Returns nothing; mutates the dag.
    fn all_witness(dag: &mut Dag, target: Hash, signers: &[(&SigningKey, Hash)]) {
        for (sk, head) in signers {
            let g = ev(sk, Some(*head), alloc::vec![target], Vec::new());
            dag.ingest(g).unwrap();
        }
    }

    #[test]
    fn introduce_admits_a_new_member() {
        let a = key(1);
        let b = key(2);
        let n = key(7);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();

        // A introduces N (grafting B's genesis so it can finalize among {A,B}).
        let intro = sys(&a, Some(gah), alloc::vec![gbh], SystemOp::Introduce { node: pk(&n) });
        let ih = intro.event_hash();
        dag.ingest(intro).unwrap();
        // B must witness the introduction for it to finalize.
        all_witness(&mut dag, ih, &[(&b, gbh)]);
        assert!(dag.is_finalized(&ih));
        assert!(dag.consensus_members().contains(&pk(&n)));

        // N can now author its first event by grafting the introduction.
        let n1 = ev(&n, None, alloc::vec![ih], b"hello".to_vec());
        assert!(dag.ingest(n1).is_ok());
    }

    #[test]
    fn depart_removes_a_member() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();

        // B departs itself (grafting A's genesis).
        let dep = sys(&b, Some(gbh), alloc::vec![gah], SystemOp::Depart { node: pk(&b) });
        let dh = dep.event_hash();
        dag.ingest(dep).unwrap();
        // A must witness; B already witnessed by authoring it.
        all_witness(&mut dag, dh, &[(&a, gah)]);
        assert!(dag.is_finalized(&dh));
        assert!(!dag.consensus_members().contains(&pk(&b)));
        assert!(dag.consensus_members().contains(&pk(&a)));
    }

    #[test]
    fn introduce_of_existing_member_is_rejected() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        let bad = sys(&a, Some(gah), Vec::new(), SystemOp::Introduce { node: pk(&a) });
        assert!(dag.ingest(bad).is_err());
    }

    #[test]
    fn depart_must_be_self_authored() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();
        // A tries to depart B — illegal (no third-party eviction).
        let bad = sys(&a, Some(gah), alloc::vec![gbh], SystemOp::Depart { node: pk(&b) });
        assert!(dag.ingest(bad).is_err());
    }
}
