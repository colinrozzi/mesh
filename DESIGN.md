# mesh — v2 design

**Status (as of 2026-06-24):** mesh v2 is *built and running*. This
document is now a historical record of the design conversation that led
to v2, plus the protocol-level reasoning behind the implementation. The
**current spec lives in `README.md`** — read that first if you want to
know how mesh actually works today.

A few things evolved during the build that this document doesn't yet
reflect:

- **Member kinds.** v2 introduced Node and Mailbox as distinct roles. The
  Op enum is `NodeIntroduce / MailboxCreate / Revoke / Send / Witness` —
  not the generic `Admit / Revoke` described below. Only Nodes count for
  consensus; Mailboxes are addressable endpoints that can Send.
- **No emails.** Member records carry just a name; no email field.
- **Inline witness emission.** When mesh receives a state-mutating event,
  it emits a Witness *inline* (synchronously) before `on_data` returns —
  not just on the periodic timer. This means single-node consensus is
  reached immediately and clients can act on freshly-created members
  without waiting for a tick.
- **`from_state` / `to_state` on state-mutating events.** Designed in but
  not implemented; events carry only `parent: EventHash` as their
  position commitment.
- **Catch-up sync (HEAD exchange + walk-back).** Designed in §"Joining
  the network" but not implemented. Multi-node v2 requires identical
  genesis configs on every node; runtime catch-up is future work.

The rest of this document is the original design — useful for reasoning
about why the protocol looks the way it does.

---

## Mental model

Imagine the network as git. Every member has a working copy. The
working copy is a DAG of signed events extending out from genesis.
Members create new events by branching off whatever point in the DAG
they currently see as the "head". Events propagate; members witness
them; eventually most members have witnessed the same events and a
shared "consensus head" emerges.

A node's local DAG isn't a write-once log. It's the operational data
structure the node uses to:

- Know who is a member (state derived from event sequence)
- Know what events the network has and hasn't seen yet
- Decide who to forward new events to
- Verify incoming events without re-querying authority
- Recover from being offline by hashing-into-someone-else's-state

## Core data shape

```
Event {
    parent:    EventHash,        // sha256 of one prior event (genesis = special)
    author:    PubKey,           // ed25519 verifying key
    op:        Op,               // the action this event records
    signature: Sig,              // ed25519 over sha256(parent, author, op)
}

Op = enum {
    Admit  { subject: PubKey, name, email },
    Revoke { subject: PubKey },
    Send   { recipient: PubKey, payload: Vec<u8> },
    Witness { also_cite: Vec<EventHash> },
}
```

Every event references exactly one parent (except genesis). The DAG
forms by following parent pointers backward. Witness events
additionally cite events the author observed but did NOT chain off of —
those references are propagation/attestation, not causal extension.

### Why these four ops

- **Admit / Revoke**: control membership.
- **Send**: deliver a message. State doesn't change but the event
  exists in the DAG so it propagates + is provably attributable.
- **Witness**: attestation. An author signs that they've observed
  specific events. This is the load-bearing primitive for gossip + for
  deriving consensus.

### Why state hashes are NOT in events

State is a pure function of the DAG: walk from genesis, apply
state-mutating events in some canonical order, look at the resulting
member set. State hashes are useful **internally** for fast equality
checks; they are not part of the wire protocol or event schema. Two
nodes that disagree on a state hash also necessarily disagree on the
DAG, and the DAG is the source of truth.

(Optionally, state-mutating events MAY include `from_state: Hash`,
`to_state: Hash` fields as a self-check. These are recomputable and
don't change semantics; they're purely for failing fast on garbled
input. v2 includes them.)

## Wire protocol

Same handshake as v1: HELLO → CHALLENGE → AUTH → ACCEPTED. After
accept, the connection's identity is bound to the pubkey from HELLO.

Frame kinds carry over from v1; SUBMIT now carries v2-shaped events.
New frames for catch-up:

| Direction | Kind | Name | Purpose |
|---|---|---|---|
| client ↔ mesh | `0x20` | HAVE | `Vec<EventHash>` — "I have these events." Used to negotiate what to send. |
| client ↔ mesh | `0x21` | WANT | `Vec<EventHash>` — "Please send me these events." |
| mesh → client | `0x90` | DELIVERED | full event bytes (any kind, any author) |

When peers connect, both sides exchange HAVE lists (e.g. recent N
events from their DAG). Each side computes what the other is missing,
sends WANTs, and the responses fill the gap. After catch-up, new events
propagate live via DELIVERED.

## Event validation

Every received event is validated independently. Validity is local:

1. **Signature** — `ed25519_verify(author, sign_hash(parent, author, op))`
2. **Parent exists** — either genesis or already in our DAG. If not, we
   buffer until we get it.
3. **Author is a member at parent** — derive state up to `parent`,
   check `author` is in `members`. Genesis has implicit root membership.
4. **Op-specific rules**:
   - Admit: subject not currently a member at parent.
   - Revoke: subject IS a member at parent; subject ≠ root.
   - Send: recipient is a member at parent.
   - Witness: each cited event hash exists in our DAG (buffer
     otherwise).

If validation fails, drop. If it passes but `parent` is missing, hold
in pending; when the parent arrives, retry. This implements the
"backfill on demand" behaviour.

## State derivation

To compute state at any event `E`:

1. Walk back from `E` via parent pointers, building a list of ancestors.
2. Topologically sort: every event appears after its parent. Ties
   (events with the same parent) sort by `event_hash` ascending.
3. Apply state-mutating events in order: Admit adds, Revoke removes,
   Send is a no-op, Witness is a no-op.

State at `E` is deterministic given the same DAG view. Two nodes that
agree on the DAG up to `E` agree on the state at `E`.

## Propagation

Each node's job:

```
For each new event E I receive (from any source):
    Validate E (sig, parent in DAG, author was member at parent, op rules).
    If valid: add E to DAG.
    For each peer P I'm connected to ≠ source of E:
        If I don't yet know that P has E:
            Send DELIVERED(E) to P.
```

"I don't yet know that P has E" comes from:
- Witnesses from P that cite E (or transitively cite events that
  reference E as ancestor).
- HAVE messages from P listing E.
- Optimistic: events forwarded to P are marked sent.

This is observation-based, not handshake-based. We don't need P to
explicitly ack receipt of E — if P ever sends us an event that
cryptographically references E (via parent chain or a Witness), we
know P has it.

## Witnesses — the gossip-about-gossip layer

A Witness from author A is signed proof that "I, A, have observed these
event hashes at this point in time." Witnesses have:

- A `parent` — A's previous event (or some recent point A is branching
  from). This puts the Witness in A's causal chain.
- `also_cite: Vec<EventHash>` — events A has seen that AREN'T
  causally before `parent`. The cross-references.

A Witness is itself an event in the DAG, so it gets propagated and
later witnessed by others. The recursive structure means:

> A node can prove, from its local DAG, that any other member had seen
> a specific event by a specific time. It just needs a chain of
> Witnesses leading back to that event.

This is what enables consensus derivation without a separate voting
protocol — see below.

### Witness emission cadence

Policy, not protocol. Suggested heuristics for the v2 impl:

- Emit a Witness every K seconds with the events received since the
  last Witness.
- Or emit on-demand when a peer asks "have you seen X?"
- Or both.

### Witnesses of Witnesses are fine

A Witness W cites previous Witnesses W1, W2, etc. This is how
propagation evidence aggregates. Don't restrict.

## Consensus head derivation

The "consensus head" is the most recent event (or set of concurrent
events) that's been transitively witnessed by **every current member**.

To compute it locally:

```
For each event E in DAG:
    witnesses_of_E = {A : there exists a Witness from A whose
                          transitive closure includes E}
    if witnesses_of_E ⊇ current members at E:
        E is in consensus

consensus_head = the maximum (latest in causal order) events in consensus
```

This is purely a local computation given a local DAG view. No voting
round, no quorum protocol. The Witnesses ARE the votes; we just count.

Optimization: maintain a running per-event witness set as events
arrive, instead of recomputing each time.

## State machine — applied vs. finalized

- **Applied state**: result of walking the DAG from genesis through
  consensus head, applying ops.
- **Finalized events**: events at or before consensus head. Safe to
  prune the bodies (keep hashes only) — future Witnesses citing them
  still verify, just transitively.
- **Working copy**: events past the consensus head. Not yet finalized.

A node operates on applied state for everything (membership checks,
Send routing, etc.). Working-copy events are visible to everyone but
not "committed" — though for Send delivery, working-copy is enough; we
don't wait for finality before delivering messages.

## Joining the network

A new node N receives the root pubkey somehow (config, friend tells
them, whatever — bootstrap is external trust).

1. N generates keypair, opens connection to any known peer P.
2. N completes handshake (P checks N is a member — but N isn't yet!).
3. Some existing member (genesis, or any current member) issues an
   `Admit(N)` event and propagates.
4. N can now connect (handshake succeeds).
5. N exchanges HAVE/WANT with peers to backfill the DAG. They get every
   event from genesis to current head.
6. N is operational.

For pure bootstrap (N is the very first node after genesis): genesis
holder has a key. They just start; their event chain is itself the
network.

## Storage layout (theater:simple/store)

```
event/<event_hash>        — encoded Event bytes
event_index/by_author/<pubkey>/<seq> — author's per-author chain index
peer_have/<peer_pubkey>   — last-known HAVE set from peer (latest hashes)
consensus_head            — current consensus head (event hash list)
```

Genesis event is special: bootstrapped from `init_state.root_pubkey`,
represents the world before any signed event. Implicit, not actually
stored. All Admit events without a real parent reference "genesis"
sentinel.

## Pruning

Once an event E is finalized AND its effect is reflected in the
current applied state AND all events causally after it are also
finalized, the event body can be dropped. The hash stays — needed to
verify future Witnesses citing E.

Witnesses themselves prune via subsumption: if a later Witness W' from
author A causally precedes W (via the parent chain) and cites a
superset of what W cited, W can be dropped.

This keeps storage bounded by O(applied_state + recent unfinalized
events + active witness chain), not O(history).

## Concurrent events

Two events with the same parent are siblings — concurrent in causal
order. Both are valid (assuming each individually passes validation).
Both go into the DAG. State derivation breaks ties via event_hash
sort, so result is deterministic across nodes.

Authors can avoid creating concurrent events by tracking what they've
seen as the current head before signing. But if they DO accidentally
create concurrent events, no problem — the DAG accommodates.

## What's NOT in v2

- **Capability layer.** Any member can do anything still.
- **Multi-mesh broker topology.** v2 is still single-process per
  "node," but v2 IS a real distributed network — multiple nodes
  syncing via the DAG protocol. Topology stays simple: peer-to-peer
  among nodes that know about each other.
- **Threshold cryptography / Byzantine fault tolerance.** v2 assumes
  members are honest-but-may-be-offline. Adversarial peers (members
  forging events with their own valid sigs but lying about
  propagation) need a separate protocol layer.

## Anticipated implementation phases

1. **Schema rewrite.** New Event type with `parent: EventHash`, new
   Op enum. v1's state.rs + event.rs replaced. (~half day)
2. **DAG storage + traversal.** New `dag.rs` module. Stores events,
   builds parent index, walks for state derivation. (~half day)
3. **Witness emission + consensus computation.** New behaviour in
   actor: emit witnesses on schedule, compute consensus head from DAG.
   (~half day)
4. **HAVE/WANT protocol.** Catch-up frames + the negotiation logic.
   (~half day)
5. **Multi-node smoke test.** Two mesh instances, connected, with
   members on each side, exchanging messages. Replace the v1 smoke
   test. (~half day)

Total: ~2-3 days of focused work for a real distributed v2.

## Open questions

1. Cadence: how often do Witnesses get emitted? On every received
   event? Every K seconds? On a heuristic? — recommend tunable.
2. Initial peer discovery: how does a fresh node find the first peer
   to gossip with? — out-of-band for now (config file with seed peer
   addresses).
3. Recovery from genuine state corruption: if a node's DAG gets
   tampered locally, can it heal from peers? Yes — fetch fresh, but
   need verification that the peer's view is canonical (via Witness
   count from other members).
4. NAT traversal / connectivity: for now assume direct TCP between
   peers. Multi-host story stays simple until we need it.
