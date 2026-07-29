// The `state-machine` interface — the pure, shared protocol of an RSM mesh (see
// DESIGN-rsm.md, Interface 1). A consumer state machine (control-SM, chat-SM, ...)
// EXPORTS these; the mesh node IMPORTS them and composes the SM in via
// `packr compose`, calling them synchronously on the fold hot path.
//
// The interface hash covers ALL bindings, so a component must declare the COMPLETE
// interface even for functions the node does not yet call (e.g. `members`, which is
// dormant while v0 is uniformly admission-final and lights up when witness-based
// finality lands with the first conflict-prone consumer).
//
// CONTRACT (not expressible in the signature, but binding):
//   - PURE: no I/O, no side effects, deterministic. Every member folds the same
//     ordered history through the same validate/apply and must converge.
//   - CONFLUENT: concurrent non-conflicting events must commute (any linearization
//     of the partial order yields the same state). The node never imposes a total
//     order in the contract; any local fold-order tiebreak must not change the
//     converged state.
//   - STRUCTURE-BLIND: the SM sees only an event (below) and a state — never the
//     DAG. Ordering/ancestry is the node's job.
//
// An event is (id, author, timestamp, payload), flattened across the boundary:
//   id        : list<u8>  — 32-byte event hash (stable identity: corr_id journals, dedup)
//   author    : list<u8>  — 32-byte signer pubkey (the SM gates membership on this)
//   timestamp : u64       — author's wall clock (ms); SM may use for display / LWW
//   payload   : list<u8>  — the SM's own event bytes: [version u16][kind][content]
// state is opaque bytes the SM owns and (de)serializes; the node never inspects it.
//
// Validate:  pact check state-machine.pact
interface state-machine {
    exports {
        // Genesis seed. Returns the empty/pre-genesis state; per-instance config
        // (a room's members, a control mesh's allow-lists) arrives as a distinguished
        // genesis EVENT folded via apply, not as an argument here. See DESIGN-rsm.md.
        initial-state: func() -> list<u8>

        // Is this event admissible against the state at its ANCESTRY position (which
        // the node has already folded and passes in)? Pure. Ok(true) = admit;
        // Err(reason) = inadmissible (reason is a human string for surfacing). A
        // genuine conflict is an Err against the consistent state at fold time.
        validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> result<bool, string>

        // Deterministic transition. Called ONLY on events that passed validate.
        // Returns the next state.
        apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> list<u8>

        // Project the current member set (32-byte pubkeys) from state. Feeds the
        // witness-based finality utility (witnesses(E) superset-of members). Dormant
        // in admission-final v0; declared now so the interface hash is stable when it
        // lights up.
        members: func(state: list<u8>) -> list<list<u8>>
    }
}
