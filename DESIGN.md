# mesh — design

This is the design rationale for mesh — the *why* behind the shape of the code.
`README.md` covers what it is and how to run it; this covers why it's built this
way.

mesh is a **generalized substrate for replicated state machines**. Each node is
its own self-rooted log; events have multiple back-references; the network runs
one deterministic state machine over a finalized, ordered event stream —
message-passing is the first one.

> History: this design (internally "v3") replaced an earlier version that used a
> single shared event chain with one global root and explicit `Witness` ops.
> Sections below contrast with that "v2" to explain the reasoning; `git log` has
> the old docs.

## Why this shape

The earlier design conflated three things into one `parent` pointer: a node's
own history, the network's shared ordering, and propagation evidence. That made
catch-up sync an afterthought (there was none — nodes had to boot from identical
genesis and see every event live) and tied the whole system to a single global
root.

This design separates them. The insight: **if every node keeps a self-rooted log and
events can reference several predecessors, then identity, dissemination,
ordering, finality, and catch-up all fall out of the same structure.** This is a
Merkle-DAG / Merkle-clock — the shape of git history (merge commits), Matrix's
room DAG (`prev_events`), and Merkle-CRDTs.

## Mental model

Two tiers, and the substrate is almost entirely ignorant of what runs on top.

- **Substrate** — identities, the per-node logs, dissemination + sync, the
  canonical order, finality, and **membership** (the one state machine it must
  compute itself, since finality depends on the member set). It treats all other
  event payloads as **opaque bytes**.
- **Application** — consumes the finalized, canonically-ordered stream. It can
  just read and write it (message-passing broadcasts opaque payloads), or fold
  it into its own replicated state (a shared task board, capability grants).
  That folding is the app's concern; the substrate doesn't provide a built-in
  reducer for it (yet) — it just delivers the stream.

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

## Membership

Membership is **objective** — every node computes the same member set. It starts
from a **configured genesis set** (pubkeys pre-defined and identical across the
network — the v2 `peer_node_pubkeys` bootstrap) and **evolves at runtime** via
`Introduce`/`Depart` system events. See *Dynamic membership* below for the full
model; the short version: the genesis set is config, and the live set is derived
by folding membership events over the finalized log.

Membership is the *one* thing the substrate interprets in events (it must —
finality depends on the member set). Everything else in an event's `payload` is
opaque; one network is one member set is one state machine.

A consequence: v2's **Node vs Mailbox** roles dissolve from the substrate. The
substrate knows only *member nodes* (the consensus participants). Application
identities — agents, mailboxes, addressable endpoints — are an *application*
concern, expressed in payloads and interpreted by the application.

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

**Application validity** (application-defined): whether a payload means anything
to the app. An event can be substrate-valid and admitted to the DAG yet be
ignored by the application. Keeping that interpretation in the application — not
the substrate — is what keeps the substrate app-agnostic and every node
deterministic: all nodes admit the same events, order them the same, and each
application makes the same decision everywhere.

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
the same primitive as an app event, distinguished only by an empty payload.
Delivery skips empties. Continuous mutual grafting (ping-pong) doubles as a
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

Every node with the same DAG computes the same order. Applications consume
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

The finalized frontier advances as grafts accumulate; committed state (or
delivery) is derived strictly from the finalized, ordered prefix.

## Optimistic delivery vs finalized commitment

Two visibility tiers, as in v2's "Sends deliver before finality":

- **Optimistic**: a payload event can be surfaced to the application as soon as
  it's received and substrate-valid — low latency, not yet committed.
- **Committed**: an application only acts on / advances state over the finalized,
  ordered prefix. Anything past the finalized frontier is working-copy. (The
  built-in message delivery is committed: payloads NOTIFY on finality.)

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

## App interface — co-located, over message-server

The wire protocol above is **node ↔ node** (TCP, across hosts, with the auth
handshake). But an *application* — the agent that owns a node — talks to *its
own* node over theater's native **message-server**, not TCP. The two links are
split by trust: peers are remote and must authenticate; an app is the node's
supervisor, so trust is structural — no handshake, and the app never signs.
The node authors every event under its own key regardless of who asked.

That collapses the app-facing surface to three commands and a delivery stream
(the envelope lives in the `mesh-api` crate, shared by node and app):

| Direction | Mechanism | Message |
|---|---|---|
| app → node | `request` | `Submit(payload)` / `Introduce(pubkey)` / `Depart` / `Register(app-id)` |
| node → app | `request` reply | ack: `ok + event-hash`, or an error string |
| node → app | `send` | delivery: `from[32] ‖ committed-payload` |

Commands go over `request` so the app gets a synchronous ack (did it author?
was I a member?). Delivery is a **callback**: the app calls `Register` once with
its own actor-id, and the node `send`s every committed payload to it — the app's
`handle-send` *is* the delivery hook. (An app learns its own id via theater's
`get-self`; on `Register` the node also flushes retained finalized history so a
late subscriber misses nothing.) This is the same committed-delivery stream the
TCP `NOTIFY` path carries for test clients — the substrate stays
payload-agnostic; addressing and message-type live in the payload bytes.

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

## Module map

- `event.rs` — the `Event` type (`author / self_parent / refs / payload /
  signature`), canonical hand-rolled encoding + `Cursor` decode, signing.
- `dag.rs` — DAG storage over `self_parent ∪ refs` back-edges; forks admitted;
  derived membership (`members_at` / `consensus_members`); `events_that_see` →
  finality; `ordered_finalized` (the finalized stream); persisted orphan buffer.
- `wire.rs` — node↔node frame protocol (handshake + DELIVER/WANT/FRONTIER +
  SUBMIT/INTRODUCE/DEPART/ACK/NOTIFY).
- `mesh-api/` — shared crate: the app↔node control envelope (command / ack /
  delivery), used by both the node and app actors. Not signed, not the wire
  format — the co-located message-server link (see *App interface*).
- `conn.rs` — per-connection handshake state.
- `codec.rs` — persistence of `ActorState` (DAG, orphan buffer, members,
  connections) and hex helpers.
- `lib.rs` — the actor: init, handshake, gossip + dedup + backfill, emit-a-graft
  on payload/membership events, broadcast delivery of committed payloads, the
  TCP SUBMIT/INTRODUCE/DEPART paths, and the `message-server` `handle-request`
  path that lets a co-located app drive the node.

## Near-term (not yet built — not launch blockers)

- **Incremental finality + delivery.** `members_at` / `ordered_finalized` /
  delivery re-scan the (now pruning-bounded) retained DAG each callback.
  Snapshotting committed state + processing only newly-finalized events would
  drop this from O(retained²) to incremental.
- **Batched emission.** Replace on-event grafting with a tick that collapses
  many refs into one witness — the scaling fix for N>2 and idle cost.

## Dynamic membership — introduction & departure

> Status: **implemented** (`SystemOp` in event.rs, derivation + validation in
> dag.rs, INTRODUCE/DEPART frames in lib.rs, `membership-test`). Stays inside the
> honest-but-offline / CP model — *no* fault tolerance. Nodes join and leave via
> explicit signed events; a crash without a departure halts the network
> (accepted). Motivating use: each agent in a fleet runs its own mesh node (in
> its supervision tree) and uses it to communicate.

### Membership becomes derived state (a substrate-reserved op)

Once membership is mutable, the substrate must *interpret* the events that change
it — finality can't be computed without knowing the member set. So membership
events are substrate-reserved, carried in a typed field rather than the opaque
payload:

```
SystemOp = Introduce { node: PubKey }   // admit a node
         | Depart    { node: PubKey }   // remove a node (self-announced)

Event { author, self_parent, refs, payload, system: Option<SystemOp>, signature }
```

An ordinary event has `system: None` + an opaque payload; a membership event has
a `SystemOp` + empty payload. The canonical encoding gains a tag byte after
`payload`: `0` = none, `1` = Introduce + node[32], `2` = Depart + node[32]. The
genesis member set is still the configured bootstrap set; it evolves from there.

### Deriving the live member set

The member set is a pure function of the DAG, computed by the substrate itself
(it can't be left to the application — finality needs it):

    members_at(E) = start from the configured genesis members, then fold every
                    finalized Introduce/Depart in E's causal ancestry, in
                    canonical order.

Inductive from genesis — no circularity — the same shape as v2's
`state_at(parent)`. The *live* member set is `members_at` the finalized frontier.

### Finality with a moving member set

`is_finalized(E) = members_at(E) ⊆ witnessing_members(E)` — the members *live at
E's position* must all have witnessed it. Two cases fall out cleanly:

- **Introduce{N}:** N isn't a member yet at this position, so it's *not* required
  to witness its own admission — the current members finalize it, then N joins.
- **Depart{N}:** N *is* still a member here, so it must witness — but it authored
  the event, and authorship is a witness, so that's automatic. The departure
  finalizes once the *other* members graft it. N can emit-and-die.

### Join flow (supervision-tree-mediated)

1. A parent agent spawns a child agent + its mesh node, and knows the child's key.
2. The parent (a member) authors `Introduce{child}`; it gossips and finalizes
   once the current members witness.
3. The parent signals the child it's admitted; the child connects as a now-valid
   member, catches up via WANT/FRONTIER, and starts witnessing.

The supervision hierarchy *is* the admission channel — a parent vouches in its
child — which sidesteps a separate bootstrap protocol and matches the fleet's
spawn pattern. (A node only passes the membership-gated handshake once its
`Introduce` has finalized; until then the parent relays.)

### Departure flow

1. On graceful shutdown, the child's node authors `Depart{self}`.
2. It gossips the departure and waits for **one live peer to acknowledge receipt**.
3. The node exits. Survivors graft the departure; it finalizes without the
   departed node; the member set shrinks.

The only hard requirement is that ≥1 peer receive the departure before the node
dies — from there gossip finalizes it. A crash with no departure (or before any
peer sees it) halts: accepted, per the threat model.

### Authorization & sequencing

- Any current member may `Introduce` (web-of-trust — a parent vouches for its
  child). `Depart` is self-authored. A member authoring `Depart` for *another*
  node — manual eviction of a crashed peer — is the one escape hatch that makes a
  crash recoverable; left out for now (it's "progress after a failure").
- Membership changes apply **one at a time** in canonical order. Concurrent
  changes (rare in a cooperative fleet) are ordered deterministically by hash;
  members chain a change off the latest membership state they've seen.
  Concurrent *conflicting* changes are the sharp edge to harden later.

## Pruning & snapshot transfer

> Status: **implemented** (`Dag::compact`, `install_checkpoint`, the CHECKPOINT
> frame; unit + integration tested). Bounds storage so broadcast-replication is
> redundancy, not unbounded waste.

**Why it's safe (the CP dividend).** All-members finality means every member has
witnessed the finalized frontier — so every member already holds everyone's
events up to it, and *no future event will ever reference below it* (a new
event's `self_parent` is its author's own head; its `refs` are current foreign
heads). So everything strictly below the frontier is droppable. The same
property that makes mesh CP makes pruning correct.

**Compaction (`compact`), a purely local decision.** Each node, on its tick,
drops the strict common ancestors of all current heads that are finalized (and,
for payloads, already delivered), folding any pruned Introduce/Depart into
`base_members`. `members_at(E)` is *invariant* to how far a node has pruned
(pruned ops in `base_members` + retained ops in the fold = all ops), so nodes
never have to coordinate watermarks — they agree on membership regardless.

**The catch — and snapshot transfer.** A *joining or lagging* node syncs the
retained events, but their deps point at pruned events it will never receive. So
compaction keeps **sealed anchors**: bare hashes of pruned events still
referenced by retained ones (the boundary; GC'd as the frontier advances). On
connect a node sends a **CHECKPOINT** = `base_members` + sealed anchors. A node
that's genuinely behind (`install_checkpoint` — the peer sealed something it
neither holds nor seals) adopts them: sealed deps now resolve, so it can ingest
retained events, and it derives membership from the adopted base. A caught-up
node ignores the checkpoint — pruning stays local.

## Deferred

- **Fault tolerance** — quorum finality, automatic eviction on failure, and
  partition recovery. The dynamic-membership design above deliberately stops
  short: a crashed member halts progress. Byzantine tolerance (defending against
  forged/equivocated events, not just detecting them) is further out still.
- **Key rotation** — the self-rooted log supports it (genesis declares keys, a
  later event rotates); not implemented.
- **Application identities** (agents / mailboxes) — addressing for non-member
  participants is an application-layer design, in the payload, per app.

## Prior art

This is a Merkle-clock / Merkle-DAG replicated log: per-node append-only logs
(à la Secure Scuttlebutt) with multi-parent merge events (à la git, Matrix's
event DAG, IPFS Merkle-CRDTs). The novel-for-us part is folding witnessing,
dissemination, and catch-up into the single grafting mechanic, over a statically
configured, objectively-agreed member set with all-members finality — a CP
coordination channel, not an AP store.
