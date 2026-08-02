//! DAG storage over a multi-parent event graph — the **dumb core** (DESIGN-rsm.md).
//!
//! Each event has back-edges `self_parent ∪ refs` (see event.rs). This module owns
//! only the *structure*: it stores events, buffers them until their dependencies
//! arrive, records the reverse adjacency ("who observes me"), and answers the
//! witness / ancestry / topological queries the node needs to drive the consumer
//! state machine's fold. It has **no notion of membership, finality, or ordering
//! as a contract** — validity is the SM's job (the node folds each event through
//! `validate`/`apply` against its ancestry-relative state; see lib.rs). `topo_sort`
//! survives only as a *local* convenience for building that fold order, never as a
//! total order the contract depends on.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::string::String;
use alloc::vec::Vec;

use crate::event::{Event, Hash, PubKey};

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

#[derive(Clone, Debug, Default)]
pub struct Dag {
    pub events: BTreeMap<Hash, Event>,
    /// Reverse adjacency: for each event hash, the events that directly
    /// reference it (via `self_parent` or `refs`) — "who observes me."
    observed_by: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Events held until a missing dependency arrives, keyed by one missing
    /// dependency hash. When that hash lands, the waiters are re-ingested.
    pending: BTreeMap<Hash, Vec<Event>>,
}

impl Dag {
    pub fn new() -> Self {
        Dag::default()
    }

    /// Rebuild from persisted, already-validated events.
    pub fn rehydrate(events: Vec<Event>) -> Self {
        let mut dag = Self::new();
        for ev in events {
            let h = ev.event_hash();
            dag.insert(h, ev);
        }
        dag
    }

    /// A dependency is available once we hold its body. The mesh retains full
    /// history, so every honest dependency eventually arrives as a real event.
    fn dep_present(&self, h: &Hash) -> bool {
        self.events.contains_key(h)
    }

    /// Ingest one event. `Ok(true)` on accept (or already-present),
    /// `Ok(false)` when buffered pending a missing dependency, `Err` on hard
    /// (structural) failure — a bad signature or a mis-authored self_parent.
    pub fn ingest(&mut self, event: Event) -> Result<bool, String> {
        let mut admitted = Vec::new();
        self.ingest_into(event, &mut admitted)
    }

    /// Like [`ingest`], but returns the hashes of every event **newly admitted**
    /// as a result — the event itself plus any buffered waiters that resolved
    /// because it landed. Empty when the event was buffered or already present.
    ///
    /// The distinction matters for witnessing: during catch-up, an event often
    /// arrives before its dependencies and is admitted later as a *waiter*. The
    /// caller must fold those too, or they never enter the state.
    pub fn ingest_admitted(&mut self, event: Event) -> Result<Vec<Hash>, String> {
        let mut admitted = Vec::new();
        self.ingest_into(event, &mut admitted)?;
        Ok(admitted)
    }

    /// Core ingest, pushing each newly-admitted hash (event + resolved waiters,
    /// transitively) into `admitted`. Returns `true` if `event` was admitted (or
    /// already present), `false` if buffered.
    ///
    /// **Structural only** — no membership gate: a non-member's event is admitted
    /// here (permissive transport) and cleared or stranded by the SM's `validate`
    /// during the fold. The only checks are the ones intrinsic to the graph: the
    /// signature must verify, and a `self_parent` must be one of the same author's
    /// own events.
    fn ingest_into(&mut self, event: Event, admitted: &mut Vec<Hash>) -> Result<bool, String> {
        event.verify_signature()?;
        let h = event.event_hash();
        if self.events.contains_key(&h) {
            return Ok(true); // dedup — idempotent
        }

        // Buffer until every dependency (self_parent + refs) is present.
        let deps: Vec<Hash> = dep_set(&event).into_iter().collect();
        if let Some(missing) = deps.iter().find(|d| !self.dep_present(d)) {
            self.pending.entry(*missing).or_default().push(event);
            return Ok(false);
        }

        // self_parent must be one of the author's own events. It is guaranteed
        // present here (the dependency check above admitted it), but we guard with
        // `if let` rather than index to avoid a panic on any future code path.
        if let Some(sp) = event.self_parent {
            if let Some(parent) = self.events.get(&sp) {
                if parent.author != event.author {
                    return Err("self_parent must be authored by the same node".into());
                }
            }
        }

        self.insert(h, event);
        admitted.push(h);

        if let Some(waiters) = self.pending.remove(&h) {
            for w in waiters {
                let _ = self.ingest_into(w, admitted);
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

    // ===== Structural queries (drive the SM fold + gossip) =====================

    /// Every distinct author with an event in the DAG. Replaces the old
    /// `consensus_members` for frontier/gossip: membership is the SM's concern, so
    /// the transport just tracks whose chains it holds.
    pub fn authors(&self) -> BTreeSet<PubKey> {
        self.events.values().map(|e| e.author).collect()
    }

    /// All events reachable from `frontier` via back-edges (frontier included).
    /// The node folds this — an event's causal past — to build the ancestry-
    /// relative state its `validate` is judged against (lib.rs `fold_state_at`).
    pub fn ancestors_of(&self, frontier: &[Hash]) -> BTreeSet<Hash> {
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

    /// All events that transitively see `target` (target included). The witness
    /// structure — feeds `witnesses(E)` (Interface 2) and, when the deferred
    /// conflict-prone bundle lands, witness-based finality.
    #[allow(dead_code)]
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

    /// Authors of events that see `target` — `witnesses(E)` (DESIGN-rsm Interface 2).
    // Exposed via the `mesh` interface's `witnesses` request (step 3).
    #[allow(dead_code)]
    pub fn witnesses(&self, target: &Hash) -> BTreeSet<PubKey> {
        self.events_that_see(target)
            .iter()
            .filter_map(|h| self.events.get(h).map(|e| e.author))
            .collect()
    }

    /// Every admitted event in one topological (hash-tiebroken) order. In
    /// admission-final v0 this is the whole fold input: the node walks it,
    /// folding each event through the SM's `validate`/`apply`. A *local*
    /// convenience — under confluence any linearization yields the same state, so
    /// the tiebreak is swappable and never part of the contract (DESIGN-rsm §4).
    pub fn ordered(&self) -> Vec<Hash> {
        self.topo_sort(&self.events.keys().copied().collect())
    }

    /// Topologically sort `set` (deps before dependents), hash tiebreak among
    /// ready nodes. `set` need not be ancestry-closed; edges to events outside it
    /// are ignored.
    pub fn topo_sort(&self, set: &BTreeSet<Hash>) -> Vec<Hash> {
        let mut indeg: BTreeMap<Hash, usize> = BTreeMap::new();
        for h in set {
            let Some(ev) = self.events.get(h) else { continue };
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

    fn ev(sk: &SigningKey, sp: Option<Hash>, refs: Vec<Hash>, payload: Vec<u8>) -> Event {
        Event::sign(sk, 0, sp, refs, payload)
    }

    #[test]
    fn admits_any_signed_author() {
        // No membership gate in the dumb core: a "stranger" is admitted
        // structurally (the SM's validate is what would strand a non-member).
        let stranger = key(9);
        let mut dag = Dag::new();
        assert!(dag.ingest(ev(&stranger, None, Vec::new(), Vec::new())).unwrap());
    }

    #[test]
    fn rejects_bad_signature() {
        let a = key(1);
        let mut bad = ev(&a, None, Vec::new(), b"x".to_vec());
        bad.payload[0] ^= 0xff; // invalidates the signature
        let mut dag = Dag::new();
        assert!(dag.ingest(bad).is_err());
    }

    #[test]
    fn rejects_self_parent_from_another_author() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new();
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        assert!(dag.ingest(ev(&b, Some(gah), Vec::new(), Vec::new())).is_err());
    }

    #[test]
    fn buffers_then_admits_when_dependency_arrives() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new();
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
    fn ingest_admitted_reports_resolved_waiters() {
        // An event that arrives before its dep is buffered, then admitted as a
        // *waiter* when the dep lands. `ingest_admitted` must report it so the
        // caller folds it — otherwise a caught-up event never enters the state.
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new();
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();

        // b's payload event depends on ga (a ref); arrives before gb (its self_parent).
        let gb = ev(&b, None, Vec::new(), Vec::new());
        let gbh = gb.event_hash();
        let b1 = ev(&b, Some(gbh), alloc::vec![gah], b"hi".to_vec());
        let b1h = b1.event_hash();
        assert!(dag.ingest_admitted(b1).unwrap().is_empty(), "buffered, nothing admitted yet");

        // gb lands → it and the buffered waiter b1 are both admitted and reported.
        let admitted = dag.ingest_admitted(gb).unwrap();
        assert!(admitted.contains(&gbh));
        assert!(admitted.contains(&b1h), "the resolved waiter must be reported");
    }

    #[test]
    fn forks_are_admitted_not_rejected() {
        let a = key(1);
        let mut dag = Dag::new();
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        let f1 = ev(&a, Some(gah), Vec::new(), b"one".to_vec());
        let f2 = ev(&a, Some(gah), Vec::new(), b"two".to_vec());
        assert!(dag.ingest(f1).unwrap());
        assert!(dag.ingest(f2).unwrap());
        assert_eq!(dag.heads_of(&pk(&a)).len(), 2);
    }

    #[test]
    fn ordered_is_ancestry_respecting() {
        // deps come before dependents in the fold order.
        let a = key(1);
        let mut dag = Dag::new();
        let g = ev(&a, None, Vec::new(), Vec::new());
        let gh = g.event_hash();
        dag.ingest(g).unwrap();
        let e1 = ev(&a, Some(gh), Vec::new(), b"1".to_vec());
        let e1h = e1.event_hash();
        dag.ingest(e1).unwrap();
        let ordered = dag.ordered();
        let pos = |h: &Hash| ordered.iter().position(|x| x == h).unwrap();
        assert!(pos(&gh) < pos(&e1h));
    }

    #[test]
    fn witnesses_are_authors_that_see_target() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new();
        let ga = ev(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        let gb = ev(&b, None, alloc::vec![gah], Vec::new()); // b grafts a's genesis
        dag.ingest(gb).unwrap();
        let w = dag.witnesses(&gah);
        assert!(w.contains(&pk(&a)) && w.contains(&pk(&b)));
    }
}
