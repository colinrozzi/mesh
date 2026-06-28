//! The reducer seam (v3).
//!
//! The substrate produces a finalized, canonically-ordered event stream
//! (`Dag::ordered_finalized`). A *reducer* folds that stream's payloads into
//! committed application state. The substrate is agnostic to what payloads mean
//! — one network runs one state machine, and this is its only coupling point.
//! See DESIGN-v3.md.

use crate::dag::Dag;
use crate::event::{Hash, PubKey};

pub trait Reducer {
    /// Committed application state, derived purely from the finalized stream.
    type State: Default;

    /// Apply one finalized event, in canonical order. Empty payloads are pure
    /// grafts / heartbeats; reducers typically ignore them. A malformed payload
    /// must be handled deterministically (ignored or rejected the same way on
    /// every node) — never panic.
    fn apply(state: &mut Self::State, author: &PubKey, event_hash: &Hash, payload: &[u8]);
}

/// Fold the finalized, ordered event stream into committed reducer state.
///
/// Re-folds from the start on each call; incremental application (apply only
/// newly-finalized events, snapshot state) is a near-term optimization noted in
/// DESIGN-v3.md.
pub fn fold<R: Reducer>(dag: &Dag) -> R::State {
    let mut state = R::State::default();
    for h in dag.ordered_finalized() {
        if let Some(ev) = dag.events.get(&h) {
            R::apply(&mut state, &ev.author, &h, &ev.payload);
        }
    }
    state
}
