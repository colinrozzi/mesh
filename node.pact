// The `node` interface — the system↔core boundary of an RSM mesh (DESIGN-dx.md,
// "Interface 0"). The pure network engine (DAG + fold + finality + the gossip
// protocol) EXPORTS these; the system entry actor IMPORTS them and composes the node
// in via `packr compose`. The node in turn imports `state-machine` (Interface 1) and
// composes the SM — so the full artifact is `system ⊕ node ⊕ SM`.
//
// WHY this shape (see DESIGN-dx.md + the theater dispatch constraint): every incoming
// theater I/O callback reaches only the ENTRY component, so the SYSTEM is the entry and
// owns ALL host I/O (tcp / timer / message-server). The node is therefore I/O-FREE — it
// never calls a host import. It takes bytes/events in and returns, as DATA:
//   - a new `node-state` (opaque bytes the system persists and never inspects — the
//     same "state is opaque" contract the SM has with the node), and
//   - a list of EFFECTS the system performs. Each effect is one self-framed blob:
//       [kind:u8][id-len:u16 BE][id utf8][payload...]
//     kind 0 = tcp send `payload` on connection `id`
//     kind 1 = message-server send `payload` to app actor `id`
//     kind 2 = tcp close connection `id` (no payload)
//   The finalized "stream" is not a separate channel: finalized dag-nodes ride out as
//   `app` effects (kind 1), exactly the frames the old message-server stream carried.
//
// CONTRACT (binding, not in the signature):
//   - PURE: no I/O, no wall-clock, deterministic given (state, input). The node's own
//     genesis timestamp / nonce are derived, not sampled (a pure core can't read a
//     clock); the system supplies time where it matters via `tick`.
//   - The system treats `node-state` as opaque; only the node (de)serializes it.
//
// Validate:  pact check node.pact
interface node {
    exports {
        // Create the node from its JSON config (node_seed / listen_addr / dial list):
        // derive identity, author genesis. Returns the initial `node-state` and an
        // encoded INIT PLAN the system performs — it owns the sockets, so the node
        // cannot `listen`/`connect` itself. Plan encoding:
        //   [listen-len:u16 BE][listen utf8]  then, repeated: [pk-len:u16][pk][addr-len:u16][addr]
        // For each planned dial the system `connect`s, then calls `on-connect(dialed=true)`.
        init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>

        // A raw connection now exists: `dialed`=true for a peer WE dialed (the system
        // just `connect`ed it), false for an inbound accept. `peer` is the expected peer
        // pubkey hex for a dial (empty for inbound). Returns new state + effects (a dial
        // emits our HELLO; an inbound waits for the peer's HELLO).
        on-connect: func(state: list<u8>, conn: string, dialed: bool, peer: string) -> tuple<list<u8>, list<list<u8>>>

        // Inbound bytes on a connection — handshake or gossip. Drives the connection's
        // phase machine and the fold. Returns new state + effects.
        on-bytes: func(state: list<u8>, conn: string, data: list<u8>, now: u64) -> tuple<list<u8>, list<list<u8>>>

        // A connection closed; drop it from the peer table. New state, no effects.
        on-close: func(state: list<u8>, conn: string) -> list<u8>

        // Periodic tick: deliver newly-final events + re-advertise frontier (anti-
        // entropy). New state + effects.
        tick: func(state: list<u8>) -> tuple<list<u8>, list<list<u8>>>

        // Author a payload event on this node's chain, PRE-VALIDATED against the current
        // frontier. Returns new state, an `ok` flag + `data` (the 32-byte hash on success,
        // the SM's reason bytes on rejection — a rejection is a normal outcome, not an
        // error), and effects (gossip the event + emit the finalized stream). Flattened
        // rather than `result<..>` so it decodes on the import side.
        author: func(state: list<u8>, payload: list<u8>, now: u64) -> tuple<list<u8>, bool, list<u8>, list<list<u8>>>

        // Register/replace the subscribed app and replay the finalized history to it as
        // `app` effects (idempotent — the app folds by SM state). New state + effects.
        subscribe: func(state: list<u8>, app-id: string) -> tuple<list<u8>, list<list<u8>>>

        // Read verbs — pure queries, no state change, no effects. `current-state` folds
        // the SM at the current frontier; `event-status` is unknown/pending/finalized/
        // stranded (0/1/2/3).
        current-state: func(state: list<u8>) -> list<u8>
        event-status: func(state: list<u8>, id: list<u8>) -> u8
    }
}
