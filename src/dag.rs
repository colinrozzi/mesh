//! DAG storage + finality, over a multi-parent event graph (v3).
//!
//! Each event has back-edges `self_parent ∪ refs` (see event.rs): `self_parent`
//! is the author's own previous event, `refs` are foreign heads it grafted.
//! Membership is **static configuration** — the substrate is told the member
//! set; it does not derive it from the log. The substrate's only jobs here are
//! to admit valid events, track reverse-reachability (who has witnessed what),
//! decide finality (every member has witnessed an event), and produce the
//! canonical finalized order for a reducer to consume. See DESIGN.md.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::codec::hex;
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

#[derive(Clone, Debug)]
pub struct Dag {
    /// Static, configured member set. Identical on every node in the network.
    pub members: BTreeSet<PubKey>,
    pub events: BTreeMap<Hash, Event>,
    /// Reverse adjacency: for each event hash, the events that directly
    /// reference it (via `self_parent` or `refs`) — "who observes me."
    /// Maintained incrementally; drives `events_that_see`.
    observed_by: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Events held until a missing dependency arrives, keyed by one missing
    /// dependency hash. When that hash lands, the waiters are re-ingested.
    pending: BTreeMap<Hash, Vec<Event>>,
}

impl Dag {
    pub fn new(members: BTreeSet<PubKey>) -> Self {
        Dag {
            members,
            events: BTreeMap::new(),
            observed_by: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    /// Rebuild from persisted, already-validated events: insert each and
    /// rebuild the reverse index. Skips signature + rule checks (they passed
    /// when first ingested), so reloading is O(n) inserts.
    pub fn rehydrate(members: BTreeSet<PubKey>, events: Vec<Event>) -> Self {
        let mut dag = Self::new(members);
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
        if !self.members.contains(&event.author) {
            return Err(format!("author {} is not a member", hex(&event.author)));
        }
        let h = event.event_hash();
        if self.events.contains_key(&h) {
            return Ok(true); // dedup — idempotent
        }

        // Buffer until every dependency (self_parent + refs) is present.
        if let Some(missing) = dep_set(&event)
            .into_iter()
            .find(|d| !self.events.contains_key(d))
        {
            self.pending.entry(missing).or_default().push(event);
            return Ok(false);
        }

        // self_parent must be one of the author's own events.
        if let Some(sp) = event.self_parent {
            let parent = self.events.get(&sp).expect("dep present");
            if parent.author != event.author {
                return Err("self_parent must be authored by the same node".into());
            }
        }

        self.insert(h, event);

        // Re-drive anything that was waiting on this event.
        if let Some(waiters) = self.pending.remove(&h) {
            for w in waiters {
                let _ = self.ingest(w);
            }
        }
        Ok(true)
    }

    /// Insert an event and update the reverse index. (No validation — callers
    /// validate, or trust persisted input via `rehydrate`.)
    fn insert(&mut self, h: Hash, event: Event) {
        for dep in dep_set(&event) {
            self.observed_by.entry(dep).or_default().insert(h);
        }
        self.events.insert(h, event);
    }

    pub fn has(&self, h: &Hash) -> bool {
        self.events.contains_key(h)
    }

    /// Events buffered awaiting a missing dependency. Persisted across callbacks
    /// so backfilled children survive until their parents arrive.
    pub fn pending_events(&self) -> Vec<Event> {
        self.pending.values().flatten().cloned().collect()
    }

    /// The author's head(s): events authored by `author` that no other event of
    /// that author builds on. Exactly one under honest operation; a set if the
    /// author has forked.
    pub fn heads_of(&self, author: &PubKey) -> BTreeSet<Hash> {
        let mut heads = BTreeSet::new();
        for (h, ev) in &self.events {
            if ev.author != *author {
                continue;
            }
            let extended = self
                .observed_by
                .get(h)
                .map(|obs| {
                    obs.iter().any(|o| {
                        self.events
                            .get(o)
                            .is_some_and(|e| e.author == *author && e.self_parent == Some(*h))
                    })
                })
                .unwrap_or(false);
            if !extended {
                heads.insert(*h);
            }
        }
        heads
    }

    // ===== Finality ============================================================
    //
    // An event E is "seen" by E' if E is reachable from E' over self_parent/refs
    // back-edges. E is finalized iff every member has authored an event that
    // sees E. Membership is static, so "every member" is just the configured set.

    /// All events that transitively see `target` (target included). Reverse-BFS
    /// over the incrementally-maintained `observed_by` index.
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

    /// Members who have authored an event that sees `target`.
    pub fn witnessing_members(&self, target: &Hash) -> BTreeSet<PubKey> {
        let mut out = BTreeSet::new();
        for h in self.events_that_see(target) {
            if let Some(ev) = self.events.get(&h) {
                if self.members.contains(&ev.author) {
                    out.insert(ev.author);
                }
            }
        }
        out
    }

    /// True iff every member has witnessed `target`.
    pub fn is_finalized(&self, target: &Hash) -> bool {
        self.events.contains_key(target) && self.members.is_subset(&self.witnessing_members(target))
    }

    /// The finalized events in canonical order: topologically sorted (every
    /// event after its `self_parent ∪ refs`), ties broken by event hash. This is
    /// the stream a reducer consumes. The finalized set is ancestry-closed, so
    /// every dependency of an included event is also included.
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
        // `ready` is a BTreeSet so iteration is hash-ascending → deterministic
        // lowest-hash tie-break among concurrent events.
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
    use ed25519_dalek::{Signer, SigningKey};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pk(sk: &SigningKey) -> PubKey {
        sk.verifying_key().to_bytes()
    }

    fn members(sks: &[&SigningKey]) -> BTreeSet<PubKey> {
        sks.iter().map(|sk| pk(sk)).collect()
    }

    fn signed(sk: &SigningKey, self_parent: Option<Hash>, refs: Vec<Hash>, payload: Vec<u8>) -> Event {
        let author = pk(sk);
        let signing_hash = Event::signing_hash(&author, &self_parent, &refs, &payload);
        Event { author, self_parent, refs, payload, signature: sk.sign(&signing_hash).to_bytes() }
    }

    #[test]
    fn single_member_finalizes_its_own_genesis() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let g = signed(&a, None, Vec::new(), Vec::new());
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

        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gb = signed(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();

        // Neither genesis is finalized yet — only its own author has witnessed.
        assert!(!dag.is_finalized(&gah));
        assert!(!dag.is_finalized(&gbh));

        // Each grafts the other's genesis.
        let a1 = signed(&a, Some(gah), alloc::vec![gbh], Vec::new());
        let b1 = signed(&b, Some(gbh), alloc::vec![gah], Vec::new());
        let (a1h, b1h) = (a1.event_hash(), b1.event_hash());
        dag.ingest(a1).unwrap();
        dag.ingest(b1).unwrap();

        // Both genesis events are now seen by A and B → finalized.
        assert!(dag.is_finalized(&gah));
        assert!(dag.is_finalized(&gbh));
        // The grafts themselves aren't finalized: each is seen by only one member.
        assert!(!dag.is_finalized(&a1h));
        assert!(!dag.is_finalized(&b1h));

        // Canonical order is a valid topo-sort over the finalized set.
        let order = dag.ordered_finalized();
        assert_eq!(order.len(), 2);
        assert!(order.contains(&gah) && order.contains(&gbh));
        assert_topo_valid(&dag, &order);
    }

    #[test]
    fn finality_requires_all_members() {
        let a = key(1);
        let b = key(2);
        let c = key(3);
        let mut dag = Dag::new(members(&[&a, &b, &c]));
        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gb = signed(&b, None, Vec::new(), Vec::new());
        let gc = signed(&c, None, Vec::new(), Vec::new());
        let (gah, gbh, gch) = (ga.event_hash(), gb.event_hash(), gc.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();
        dag.ingest(gc).unwrap();

        let m = signed(&a, Some(gah), alloc::vec![gbh, gch], b"x".to_vec());
        let mh = m.event_hash();
        dag.ingest(m).unwrap();
        assert!(!dag.is_finalized(&mh), "only A has witnessed");

        let b1 = signed(&b, Some(gbh), alloc::vec![mh], Vec::new());
        dag.ingest(b1).unwrap();
        assert!(!dag.is_finalized(&mh), "A and B — still missing C");

        let c1 = signed(&c, Some(gch), alloc::vec![mh], Vec::new());
        dag.ingest(c1).unwrap();
        assert!(dag.is_finalized(&mh), "all three have witnessed");
    }

    #[test]
    fn rejects_non_member_author() {
        let a = key(1);
        let stranger = key(9);
        let mut dag = Dag::new(members(&[&a]));
        let ev = signed(&stranger, None, Vec::new(), Vec::new());
        assert!(dag.ingest(ev).is_err());
    }

    #[test]
    fn rejects_self_parent_from_another_author() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));
        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();
        // B claims A's event as its self_parent — illegal.
        let bad = signed(&b, Some(gah), Vec::new(), Vec::new());
        assert!(dag.ingest(bad).is_err());
    }

    #[test]
    fn buffers_then_admits_when_dependency_arrives() {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(members(&[&a, &b]));

        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();

        let gb = signed(&b, None, Vec::new(), Vec::new());
        let gbh = gb.event_hash();

        // a1 refs gb, which we don't have yet → buffered, not admitted.
        let a1 = signed(&a, Some(gah), alloc::vec![gbh], Vec::new());
        let a1h = a1.event_hash();
        assert_eq!(dag.ingest(a1).unwrap(), false);
        assert!(!dag.has(&a1h));

        // gb arrives → a1's dependency is satisfied and it gets admitted.
        dag.ingest(gb).unwrap();
        assert!(dag.has(&a1h));
    }

    #[test]
    fn forks_are_admitted_not_rejected() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();

        // Two distinct events off the same self_parent (different payloads).
        let f1 = signed(&a, Some(gah), Vec::new(), b"one".to_vec());
        let f2 = signed(&a, Some(gah), Vec::new(), b"two".to_vec());
        assert!(dag.ingest(f1.clone()).unwrap());
        assert!(dag.ingest(f2.clone()).unwrap());
        assert!(dag.has(&f1.event_hash()));
        assert!(dag.has(&f2.event_hash()));
        // The author now has two heads.
        assert_eq!(dag.heads_of(&pk(&a)).len(), 2);
    }

    #[test]
    fn ingest_is_idempotent() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let g = signed(&a, None, Vec::new(), Vec::new());
        dag.ingest(g.clone()).unwrap();
        dag.ingest(g).unwrap();
        assert_eq!(dag.events.len(), 1);
    }

    #[test]
    fn heads_track_the_chain_tip() {
        let a = key(1);
        let mut dag = Dag::new(members(&[&a]));
        let g = signed(&a, None, Vec::new(), Vec::new());
        let gh = g.event_hash();
        dag.ingest(g).unwrap();
        assert_eq!(dag.heads_of(&pk(&a)), BTreeSet::from([gh]));

        let e1 = signed(&a, Some(gh), Vec::new(), b"x".to_vec());
        let e1h = e1.event_hash();
        dag.ingest(e1).unwrap();
        assert_eq!(dag.heads_of(&pk(&a)), BTreeSet::from([e1h]));
    }

    /// Assert every event in `order` appears after all of its in-set dependencies.
    fn assert_topo_valid(dag: &Dag, order: &[Hash]) {
        let mut pos = BTreeMap::new();
        for (i, h) in order.iter().enumerate() {
            pos.insert(*h, i);
        }
        for (i, h) in order.iter().enumerate() {
            for dep in dep_set(&dag.events[h]) {
                if let Some(&dpos) = pos.get(&dep) {
                    assert!(dpos < i, "dependency must precede dependent");
                }
            }
        }
    }
}
