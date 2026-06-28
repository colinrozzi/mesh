# mesh — v3 design

The v2 mesh (see `README.md` for what's running, `DESIGN.md` for its design
history) is a single shared event chain with one global root: every event names
one `parent`, everyone branches off a common head, and a separate `Witness` op
carries the gossip-about-gossip that drives consensus.

v3 is a different shape. Each node is its **own** self-rooted log; events have
**multiple** back-references; and the network becomes a **generalized substrate
for replicated state machines** — message-passing is just the first thing we
build on it.

> Status: this is the *next* version's spec. The code still implements v2.
> `README.md` = what's running; `DESIGN.md` = v2 design history; this = v3.

## Why v3

v2 conflated three things into one `parent` pointer: a node's own history, the
network's shared ordering, and propagation evidence. That made catch-up sync an
afterthought (there is none — nodes must boot from identical genesis and see
every event live) and tied the whole system to a single global root.

v3 separates them. The insight: **if every node keeps a self-rooted log and
events can reference several predecessors, then identity, dissemination,
ordering, finality, and catch-up all fall out of the same structure.** This is a
Merkle-DAG / Merkle-clock — the shape of git history (merge commits), Matrix's
room DAG (`prev_events`), and Merkle-CRDTs.

## Mental model

Two tiers, and the substrate is almost entirely ignorant of what runs on top.

- **Substrate** — identities, the per-node logs, dissemination + sync, the
  canonical order, and finality. It treats event payloads as **opaque bytes**.
- **State machine** — a deterministic reducer fed the finalized, canonically
  ordered event stream. The network *is* a state machine; which one is implied
  by the network you've joined. Message-passing is one such reducer; the next
  thing we build is another.

A node's local DAG is the operational data structure it uses to: know the order
of events, know what the network has and hasn't seen, decide what to send peers,
verify incoming events without re-querying authority, and recover after being
offline.

## Threat model & guarantees

mesh is a **coordination channel among a fixed, fully-present set of nodes** —
think a multi-party TCP connection more than a fault-tolerant store. Everything
below follows from that framing:

- **Honest-but-may-be-offline.** Members run correct code; they may crash or
  disconnect, but they don't forge, lie, or equivocate. Byzantine faults are out
  of scope for v3 — the signed self-chains make misbehavior *detectable* (see
  *Equivocation*), but defending against it is a later BFT layer.
- **Consistency over availability (CP).** Finality requires *every* member, so
  if any member is offline the network **intentionally stops committing** until
  it returns. That is the contract, not a weakness: committing *means* everyone
  is present and agrees. This is not a Dynamo-style always-available store and
  isn't trying to be.
- **Determinism.** Given the same set of events, every node computes the same
  DAG, the same order, and the same committed state — *independent of arrival
  order*. This is why forks are admitted rather than arrival-order-rejected
  (see *Validity*); rejecting-on-arrival is the one thing that would break it.

## Core data shape

```
Event {
    author:      PubKey,          // ed25519 verifying key (a member node)
    self_parent: Option<Hash>,    // this author's previous event; None for a genesis
    refs:        Vec<Hash>,       // foreign heads this event grafts / witnesses
    payload:     Vec<u8>,         // opaque to the substrate; may be empty
    signature:   Sig,             // ed25519 over the canonical encoding above
}
```

`event_hash` is sha256 over the canonical encoding. Encoding stays hand-rolled
and deterministic (byte-stable across machines and language ports), as in v2 —
only the field set changes. `self_parent: None` encodes with an explicit
discriminant byte (`0` = none, `1` = followed by 32 bytes) so the layout stays
canonical.

### self_parent vs refs — the load-bearing distinction

A node's events form a **chain** via `self_parent`: each event names that
author's previous event, tracing back to the author's genesis. This is the
node's history, independently verifiable (walk it to genesis, check every
signature against the author's key). Under honest operation the chain is linear
with a single head; v3 tolerates an author forking it (the forks are admitted as
concurrent siblings — see *Validity*), so an author's "head" may transiently be
a *set*.

`refs` are the **grafting / witnessing** edges: foreign heads the author had
seen when it signed. An event with `refs` simultaneously (a) extends its
author's own log and (b) records "I have observed these other histories up to
here." There is no separate `Witness` op in v3 — **every event that grafts a
foreign head is a witness.**

So the back-edges of the DAG are `self_parent ∪ refs`. Ancestry ("E is in X's
causal past") is reachability over those edges.

### Genesis

A node's genesis event has `self_parent: None` and (typically) empty `refs`. It
is the root of that node's chain, signed by the node's key. Membership (below)
is configured out-of-band, so genesis doesn't need to *announce* identity to a
registry — it just anchors the chain. Its payload may declare the node's key(s)
for future key-rotation, but that's not required in v3. There is **no global
genesis** and no privileged root; each chain is self-rooted.

### Equivocation is detectable, but tolerated for now

Because each event names a `self_parent`, an author that signs two different
events off the same parent has forked its own chain — cryptographic, self-
incriminating evidence any node can verify. v3 **admits both forks** rather than
rejecting either: rejecting the second-seen would make finality depend on
arrival order and break determinism (the whole point of *Threat model*). Under
honest-but-offline this shouldn't happen; if it does, the two events are just
concurrent siblings the canonical order resolves by hash. Acting on the evidence
— rejecting or slashing an equivocator — is a future BFT concern; *disallowing*
forks outright is deferred until then.

## Membership (static in v3)

Membership is **objective** — every node computes the same member set — and in
v3 it is **static configuration**: each node is started with the set of member
pubkeys (and their addresses) pre-defined and identical across the network. This
is the v2 `peer_node_pubkeys` / `peer_meshes` bootstrap promoted to the whole
membership story.

This is the simplification that lets us drop a per-event "machine" tag: since
the member set comes from config and never changes at runtime, **the substrate
never has to interpret an event to learn membership.** It interprets nothing.
There is no membership op, no system namespace — one network is one member set is
one state machine.

> **v3 boundary, named deliberately.** The moment membership must change at
> runtime, the substrate again needs to learn the member set *from somewhere* —
> a substrate-reserved payload convention it may peek at, or an out-of-band
> reconfiguration protocol. v3 does not solve this. Static membership is a
> conscious boundary, not an oversight.

A consequence: v2's **Node vs Mailbox** roles dissolve from the substrate. The
substrate knows only *member nodes* (the consensus participants). Application
identities — agents, mailboxes, addressable endpoints — are an *application*
concern, expressed in payloads and interpreted by the reducer.

## Validity — two layers

**Substrate validity** (objective; every node agrees; decides whether an event
enters the DAG):

1. **Signature** verifies against `author`.
2. **Author is a member** (in the configured set).
3. **self_parent points into the author's own chain** (one of the author's
   prior events), or is `None` for a genesis. It need **not** be unique: if an
   author signs two events off the same `self_parent`, both are admitted. We
   never arrival-order-reject — that would diverge nodes (see *Equivocation*).
4. **self_parent and refs exist** in our DAG. If any is missing, the event is
   **buffered pending** and we request the gap (see *Dissemination*).

**Reducer validity** (application-defined): whether a payload is a legal
transition for the state machine. An event can be substrate-valid and admitted
to the DAG yet be a **no-op or rejected** by the reducer. Keeping rejection in
the reducer — not the substrate — is what keeps the substrate app-agnostic and
every node deterministic: all nodes admit the same events, order them the same,
and the reducer makes the same accept/reject decision everywhere.

## Dissemination & sync — one mechanic

Grafting *is* sync. There is no separate catch-up protocol.

```
On receiving an event E:
    1. DEDUP — if E is already in my DAG, drop it. (Mandatory: without this,
       any cycle in the peer graph re-forwards events forever.)
    2. If E's self_parent or any ref is missing from my DAG:
         buffer E, send WANT(missing hashes)        // backfill ancestry on demand
    3. Else validate E, add it to my DAG, then immediately:
         - forward E to peers that don't yet have it, and
         - author my own event grafting E
               self_parent = my head
               refs        = foreign heads newly seen
               payload     = empty                   // this graft IS my witness of E
       (When *I* have a payload to send, I author an event the same way, with
        payload set.)
```

Because validating E means being able to walk its ancestry to genesis, "receive
a head" naturally pulls "fetch the ancestry I'm missing." **That is the catch-up
sync** — a node that joins late or reconnects after a gap requests what it lacks
and converges. The orphan buffer (events held pending their parents) is a
first-class, persisted part of state, and it *drives* `WANT` requests rather
than waiting passively.

**Bulk sync on connect** is an optimization of the same idea: exchange a compact
**frontier** (the heads each side has per author — a head-set, since an author
may have forked) so a reconnecting node learns what it's behind on without
re-streaming the whole DAG.

### Emission is on-event, no timer (for now)

Receiving new information immediately produces a witness event — lowest latency,
simplest code. An **empty-payload event is a pure graft / heartbeat / witness**;
the same primitive as an app event, distinguished only by an empty payload. The
reducer skips empties. Continuous mutual grafting (ping-pong) doubles as a
liveness heartbeat, and we lean into it.

The cost, stated plainly: in a fully-connected N-node network each received
event begets a graft, so steady-state volume is ~O(N²) events per round-trip and
the DAG grows continuously even with no payloads. That's fine for the small,
TCP-like deployments v3 targets, and **dedup (step 1) bounds it to new events
only**, preventing true loops. **Batched emission** — graft on a tick that
collapses many refs into one witness, à la v2's witness-tick — is the known fix
for larger N or idle cost. Near-term, not a launch blocker (see below).

## Canonical order

State is a pure function of the DAG. To order events deterministically:

1. Take the finalized sub-DAG (events at or before the finalized frontier).
2. Topologically sort: every event after all of its `self_parent ∪ refs`
   predecessors. Break ties (concurrent events, including forks) by `event_hash`
   ascending.

Every node with the same DAG computes the same order. The reducer consumes
payloads in this order. This is v2's `state_at` topo-sort, generalized to
multi-parent ancestry.

## Finality — recast, not rebuilt

An event `E` is **finalized** iff every current member has authored an event
that transitively *sees* `E` (has `E` in its `self_parent ∪ refs` ancestry).
Witnesses are no longer a special op — they're just events that graft `E`'s
lineage. Because "every member" is required, commitment **halts the moment any
member is offline** — by design (see *Threat model*).

This maps directly onto machinery v2 already has:

- `events_that_see(E)` — reverse-reachability over the back-edges. v2 walked
  `parent ∪ also_cite`; v3 walks `self_parent ∪ refs`. Same reverse-BFS.
- `witnessing_members(E)` = authors of events that see `E`.
- `finalized(E)` = `witnessing_members(E) ⊇ members`.

The finalized frontier advances as grafts accumulate; the reducer's committed
state is derived strictly from the finalized, ordered prefix.

## Optimistic delivery vs finalized commitment

Two visibility tiers, as in v2's "Sends deliver before finality":

- **Optimistic**: a payload event can be surfaced to the application as soon as
  it's received and substrate-valid — low latency, not yet committed.
- **Committed**: the reducer only *advances state* over the finalized, ordered
  prefix. Anything past the finalized frontier is working-copy.

Caveat worth respecting: the hash-tiebreak order means a later-arriving
concurrent event can sort *before* one already shown optimistically. So
optimistic delivery is only safe for **reorderable / idempotent** payloads (e.g.
fire-and-forget message delivery). Anything order-sensitive or with side effects
should read committed state — there's no un-delivering.

## Wire protocol

TCP, length-prefixed binary frames: `LEN(u32 BE) || KIND(u8) || PAYLOAD`.
Config provides each member's pubkey **and address** (peer discovery beyond the
configured set is out of scope for v3).

Handshake (unchanged from v2 in spirit; membership checked against config):

```
client → HELLO(pubkey)                                   0x01
mesh   → CHALLENGE(nonce[32])     | REJECTED             0x80 | 0x82
client → AUTH(sig over nonce)                            0x02
mesh   → ACCEPTED(frontier)       | REJECTED             0x81 | 0x82
```

`ACCEPTED` carries the accepting node's frontier (per-author head-sets) so the
dialer can immediately compute what it's missing. After accept, the connection's
identity is bound to the HELLO pubkey for its lifetime.

Post-handshake:

| Direction | Kind | Name | Payload |
|---|---|---|---|
| both | `0x10` | DELIVER | one encoded `Event` (broadcast / backfill response) |
| both | `0x20` | FRONTIER | per-author head-sets — "here's what I have" |
| both | `0x21` | WANT | `Vec<Hash>` — "send me these events" |

`DELIVER` unifies v2's `DELIVERED`/`SUBMIT`: every event is just an event. A
node's own new events and a peer's gossiped events flow through the same path;
there is no privileged "submit." `WANT` backfills missing ancestry; `FRONTIER`
negotiates bulk catch-up.

## Liveness — halting is the contract, not a bug

Finality requires **all** members, so a member that goes down **halts
commitment** until it returns. In v3 this is the *intended* semantics: mesh is a
coordination channel for a fixed, present set (see *Threat model*) — "we're all
here and agree" is exactly what committing means. Optimistic delivery keeps
flowing among connected peers; only commitment waits.

A *permanently* gone member is handled by **reconfiguring membership** (changing
the configured set) — the dynamic-membership work deferred below — not by in-band
eviction. The ping-pong heartbeat is the signal that tells an operator (or a
future reconfiguration protocol) that a member is gone.

## What changes in the code (v2 → v3)

- `event.rs` — `Event` gains `self_parent: Option<Hash>` + `refs: Vec<Hash>`,
  drops the single `parent`. `Op` collapses entirely: `Witness` is gone (grafts
  witness), and `Send`/`NodeIntroduce`/`MailboxCreate`/`Revoke` become *payload*
  conventions of the message-passing reducer, not substrate ops. The substrate
  event has no `op` — just an opaque `payload`. Encoding/`Cursor` decode carry
  over (plus the `Option` discriminant).
- `dag.rs` — back-edges become `self_parent ∪ refs`. `events_that_see`,
  `witnessing_*`, finality, and the topo-sort generalize to multi-parent with
  little change. Membership becomes a static configured set; the
  sequential-consensus-on-membership spine goes away. Forks are admitted (no
  uniqueness check), so the author→head index is a head-*set*.
- New: a **reducer** seam — the substrate hands the finalized, ordered payload
  stream to an application reducer (message-passing first).
- `lib.rs` — `SUBMIT` folds into `DELIVER`; **dedup** on receive; orphan buffer
  becomes persisted and drives `WANT`; `FRONTIER` exchange on connect; emit a
  graft on every newly-seen event.
- `codec.rs` — persist the orphan buffer + per-author frontier alongside the DAG.

## Near-term (right after the first v3 cut — not launch blockers)

Not in the first running v3, but the first things to build on it. Unbounded
growth is an accepted failure mode until they land.

- **Pruning / compaction.** Finalized, applied history can be dropped (keep
  hashes for verification). Most urgent, because the on-event heartbeat grows
  the DAG continuously, even at idle.
- **Incremental finality + reducer.** Advance the finalized frontier and apply
  the reducer over *newly* finalized events instead of re-folding from genesis;
  snapshot committed state. Avoids O(history²) recompute.
- **Batched emission.** Replace on-event grafting with a tick that collapses
  many refs into one witness — the scaling fix for N>2 and idle cost.

## Deferred / not in v3

- **Dynamic membership** (runtime add/remove, and reconfiguration to drop a
  permanently-gone member) — the named boundary above.
- **Byzantine fault tolerance** — equivocation/forgery is detected, not
  defended; admitting forks keeps us consistent under honest-but-offline only.
- **The introduction problem** — admitting a genuinely new node without
  pre-shared config (web-of-trust, sponsor events, …).
- **Key rotation** — the self-rooted log supports it (genesis declares keys, a
  later event rotates); not implemented.
- **Application identities** (agents / mailboxes) — addressing for non-member
  participants is a reducer-layer design, per app.

## Prior art

This is a Merkle-clock / Merkle-DAG replicated log: per-node append-only logs
(à la Secure Scuttlebutt) with multi-parent merge events (à la git, Matrix's
event DAG, IPFS Merkle-CRDTs). The novel-for-us part is folding witnessing,
dissemination, and catch-up into the single grafting mechanic, over a statically
configured, objectively-agreed member set with all-members finality — a CP
coordination channel, not an AP store.
