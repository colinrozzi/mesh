# Mesh as a replicated state machine (RSM) — the pinned contract

**Status: DESIGN LOCKED (mesh-dev + Colin), not yet built.** This is mesh-dev's
answer to the three mesh-dev-owned open decisions in
`manager/federated-messaging-design.md` (§9.1 boundary, §9.2 consistency, §9.3
SM-interface signatures). Companion to `DESIGN.md` (the current substrate) and
`DESIGN-compaction.md` (deferred). Supersedes all earlier RSM drafts.

## Thesis

Abstract the mesh node over a **consumer-provided state machine**. The core goes
**dumb**: identity + connections + a gossip-replicated **partial-order** event graph.
No membership logic, no finality logic, no fabricated total order. Everything the
substrate used to decide moves into a state machine whose one job is a **pure fold
over event history**.

## The five principles (each a consequence of the one before)

1. **State is the finalized fold over event history.** `validate`/`apply` are pure;
   *only finalized events are state*; state is therefore **immutable** (never reorgs).
2. **Validity is a pure function of state** — decided by the fold, never by reality.
   Reality re-enters only as more events (the executor observes the world and *emits*
   what it learned). The log is the map; only the executor touches the territory.
3. **Validity is ancestry-relative** — an event is judged against the state of *its
   own causal past*, never the global "now." (Concurrent events can't invalidate each
   other; that would be acausal.)
4. **Correctness rests on confluence, not linearization** — concurrent non-conflicting
   events must commute, so the partial order (ancestry) is the whole truth. Any total
   order is a *swappable local convenience*, never the contract.
5. **Conflicts are surfaced, not resolved.** The substrate *detects and equips*; the
   application owns resolution policy (merge / rebase / alert / drop). The only
   baked-in behavior is: **a loser is inert.**

---

## Shared types

```
type pubkey = list<u8>        // 32B ed25519
type hash   = list<u8>        // 32B sha256 — an event's stable identity
type state  = list<u8>        // OPAQUE — the SM owns its representation + (de)serialization

record sm-event {             // the PURE view — what the SM sees. No graph structure.
    id:        hash,          // stable identity (corr_id journals, message dedup)
    author:    pubkey,        // signer — the SM gates membership on this
    timestamp: u64,           // author's wall clock (ms) — SM may use for display / LWW
    payload:   list<u8>,      // the SM's own event: [version u16][kind][content]
}

record dag-node {             // the STRUCTURAL view — an event AND its position in the DAG.
    event: sm-event,          // the content
    deps:  list<hash>,        // edges (self_parent + refs) — the partial-order structure
}

enum event-status {
    unknown,      // this node doesn't hold it
    pending,      // held, not yet part of the finalized state (buffering, or awaiting finality)
    finalized,    // validity is permanent → in the immutable state (terminal)
    stranded,     // conflicts with the finalized line → can never finalize, inert (terminal)
}
```

The `sm-event` / `dag-node` split is load-bearing: the **pure SM is structure-blind**
(it must not ancestry-walk — it just folds `(event, state)`), while the **impure
executor is structure-aware** (reasoning about causal position is its job). The type
system enforces the boundary.

## Interface 1 — `state-machine`  (SM implements · node calls, composed synchronously)

The pure, **shared protocol** — its own packr component, composed *into* the node
(the node calls `validate`/`apply` on the fold hot path, thousands of times; that must
be in-process, not message-passing). Chat and control are different implementations of
*this exact interface*. Pure, deterministic, **confluent**, no I/O.

```
initial-state : func() -> state                                        // genesis: seeds membership + app state
validate      : func(event: sm-event, s: state) -> result<_, string>   // pure; against the state at the event's ANCESTRY; Err(reason) if inadmissible
apply         : func(event: sm-event, s: state) -> state               // pure, deterministic transition
members       : func(s: state) -> list<pubkey>                         // project the member set — the finality utility needs it
```

## Interface 2 — `mesh`  (node implements · system calls, as requests)

Drive + inspect. One act verb; the rest is read-only.

```
author        : func(payload: list<u8>) -> result<hash, string>   // build+sign+gossip; PRE-VALIDATED (Err = your own validate reason)
current-state : func() -> state                                    // the FINALIZED fold — the only "real" state
event-status  : func(event: hash) -> event-status                  // where an event sits in its lifecycle
witnesses     : func(event: hash) -> list<pubkey>                  // inspection (custom conflict handling)
ancestry      : func(event: hash) -> list<hash>                    // inspection (custom conflict handling)
```

## Interface 3 — the node EMITS · the system consumes (a stream, not callbacks)

The node emits a settled-event stream; the system subscribes. **Emission, not
callbacks** — see *Runtime model*.

```
event finalized  { nodes: list<dag-node> }                         // newly-finalized nodes; ORDER AMONG THEM IS NOT SEMANTIC — structure is in `deps`
event conflict   { node: dag-node, reason: string, conflicts-with: list<hash> }   // a genuine conflict surfaced — app decides what to author
event stranded   { own-event: hash }                               // your un-finalized event lost — app decides
```

`finalized` hands over a **DAG delta**, not a linear list — the consumer reconstructs
the finalized DAG and linearizes *locally* however it needs (topological when it has
causal effects to sequence, arrival-order when its effects are confluent). Handing a
DAG is strictly more powerful than a list, and it honors principle 4 all the way to
delivery.

## The non-function contract (obligations the interface encodes)

- **`author` is the only way to affect anything** — rebase, merge, response,
  correction are all "author the right event."
- **Only finalized is state.** `current-state` + the `finalized` stream are the sole
  source of truth; there is no provisional application anywhere. "Eventual" isn't a
  weaker delivery path — it's finality that happens *instantly* because nothing can
  conflict (below).
- **`validate`/`apply` are pure + confluent + structure-blind.** The node folds the
  ancestry and hands the SM a `state`; the SM operates only on `(event, state)`.
- **The substrate's only conflict behavior is "loser inert."** Everything else —
  detect (`conflict`/`stranded`), inspect (`witnesses`/`ancestry`), act (`author`) —
  is the app's to compose into policy.

## Finality is conflict-scoped — and v0 is uniformly admission-final

An event is **`finalized` when its validity is permanent** — once (a) its
validity-dependencies are final and (b) nothing can conflict with it. A **conflict-free**
event satisfies both on admission, so it finalizes *instantly*; a **conflict-prone**
event must wait for all-members-witness to rule out a concurrent conflict.

**Both v0 consumers are fully conflict-free**, so v0 is *uniformly admission-final*
(`finalized = admitted`):
- **Chat** — every kind is a CRDT (OR-Set membership; append messages), so nothing ever
  conflicts; messages finalize on admission regardless of offline members.
- **Control** — under the *no-kick invariant* (membership shrinks only via causal
  self-`depart`, never a concurrent kick), no command is ever concurrent with its
  author's removal, so join/depart/command/response are all conflict-free and
  admission-final. This is what lets eviction drop entirely — no kind waits on a witness,
  so a crashed member stalls nothing.

So **v0 needs no witness-based finality at all.** The general form
`finalized = witnesses(E) ⊇ required(E)`, with `required` per-event and conflict-scoped,
is deferred with the conflict-prone bundle (below) and arrives with the first genuinely
conflict-prone consumer (a kick-enabled control plane, a document with real merge
conflicts).

## Conflicts: detect, surface, equip — never auto-resolve

Genuine conflicts (non-commuting concurrent events) are **application failure cases**,
and the right policy differs per app — so the substrate must not pick one:

- **Chat:** stranded message → re-author on the tip, or tell the user "resend."
- **Document:** concurrent edits → author a **merge event** reconciling both (it *wants*
  both — a chat app has no merge and shouldn't be forced to).
- **Control:** conflict shouldn't happen (stable membership); if it does → surface as an
  **error / alert**, never auto-resolve.

Convergence still holds because the app's policy is **deterministic + shared** (it *is*
the SM): the substrate hands every member the same conflict (same `reason`), the app
makes the same decision, everyone converges. Only the owner authors its own resolution
(per the keys); it propagates and everyone applies it.

**"Rebase" is not machinery** — it's the author re-authoring its own stranded event on
the current tip (the normal `author` path). The old version stays inert; no
supersede-links, no dedup. It's a *liveness/rescue* tool the app opts into, never a
safety mechanism (safety is the always-consistent finalized line).

## Ordering + the tiebreak (a swappable local detail)

A total order over the DAG is *fabricated* information; we don't bake one into the
contract. A tiebreak is allowed by exactly one test:

> **changing the tiebreak must not change the converged state.**

Under confluence that always holds for non-conflicting concurrency, so a hash tiebreak
is a legitimate *local* fold order. For genuine conflicts, resolution is
**nondeterministic-but-agreed** — whichever line all-members finality lands on; a soft,
swappable hint (hash now, ungrindable later for adversarial federation) breaks a 50/50
witness split. The hint only ever touches genuine conflicts, only affects
*which/how-fast* (fairness/liveness), never *whether the room agrees* (safety = finality).
None of this is in the contract; it lives in the node implementation.

## Runtime model

```
system actor  (per-identity: executor + storage + UI)  ── THE top level
   │  drives via requests:   author / query                         ← Interface 2 (mesh)
   │  consumes emitted stream: finalized / conflict / stranded      ← Interface 3
   ▼
node actor  ──composes──▶  SM component (synchronous, the protocol) ← Interface 1 (state-machine)
```

- **`node + SM` = one composable "protocol participant" actor.** The SM is composed
  *in* (synchronous calls on the fold hot path). The SM is the *only* genuine child of
  the node — correct and necessary.
- **The system is a separate, top-level actor** — the per-identity executor. It is *not*
  a child of the node; it drives the node (requests) and consumes the node's emitted
  stream.
- **Emission, not callbacks**, for substantive reasons: (1) it decouples finality
  progress from the executor's slow, side-effecting I/O — the world's latency never
  stalls consensus; (2) acyclic dependencies (callbacks would make node↔system cyclic);
  (3) restart/replay from a cursor (today's `delivered`-set pattern); (4) it's what
  the current mesh-api already does (request commands + a delivery stream), so this
  extends the node-side-done architecture rather than replacing it; (5) multiple
  consumers (UI + logger + executor) can subscribe.

## Core factoring — what stays, what leaves (symbol-level, from v0.4)

**STAYS:** `Event` sign/verify + canonical encoding; the DAG (`self_parent ∪ refs`) as
the causal/partial order; `ancestors_of`, `observed_by`, `events_that_see` (the witness
structure); gossip (`FRONTIER`/`WANT`/`DELIVER`, `ingest` dedup/buffer/admit); the
handshake as *identity proof* (membership check dropped — transport permissive);
persistence. `topo_sort` stays but demoted to a *local* convenience, never the contract.

**LEAVES → SM concern:** `is_finalized`, `members_at_frontier`, `consensus_members`,
`fold_membership`, `base_members`; `SystemOp::Introduce|Depart|Evict` + membership rules
in `ingest`; the `ingest` membership gate; eviction + the finality-heartbeat + N=2
shutdown. The `system` envelope field is removed; membership becomes SM payload.

Net: the core shrinks to *identity + gossip + partial-order graph + witness queries.*

## Not phases — two states: conflict-free v0, and a deferred conflict-prone bundle

There is no behavior-preserving intermediate. The v0 endpoint is *simpler* than today's
mesh (admission-final, no eviction, no witness-counting), so "reproduce today first, then
simplify" would mean building machinery just to delete it. Build v0 directly.

**v0 — build now:**
- **Dumb core, admission-final.** Strip membership/finality/eviction from v0.4; fold
  *every* event through the SM's `validate` (no core membership gate); `finalized =
  admitted`; emit the `finalized`/`conflict`/`stranded` stream on admission.
- **Conflict detection + surfacing** — the *substrate's* half of conflict-handling (§5).
  The fold checks each event against the consistent state; a genuine conflict strands the
  event and fires `conflict`/`stranded`. This makes "conflict-free by design" a *checked
  runtime invariant* (fail-loud), not an assumption — the guard against silent
  divergence. (The substrate's only built-in resolution stays "loser inert.")
- **The two conflict-free SMs** (control, chat), composed and validated. **Oracle: the
  sentinel round-trip + a chat smoke.**

**Deferred conflict-prone bundle** — coupled; arrives with the first conflict-prone
consumer or real storage pressure:
- conflict **resolution** helpers — the convergence hint for 50/50 witness splits and
  rebase-cascade management (the two mechanisms §9 leaves explicitly unpinned; they need a
  real conflict-prone consumer to design against).
- witness-based finality **as a gate** (v0 needs none).
- the `kick` kind + member-GC — dropping eviction *defers* member-GC, it doesn't
  eliminate it; a kick is conflict-prone, which is exactly what pulls witness-finality
  back, so these three land together.
- compaction (bounded storage; separate deferred design, `DESIGN-compaction.md`).

The seam: **the substrate fully handles conflicts in v0 (detect + surface + inert); only
the app-side *resolution* helpers, member-GC, and storage-bounding defer.**

## Open decisions — resolved

- **State representation:** opaque bytes; SM owns (de)serialization; node persists
  snapshots.
- **Recompute:** re-fold each pass for v0 (as today); optimize later with
  ancestor-anchored snapshots (never from the consensus head — concurrency).
- **Kind/version:** reserve a `u16` at the head of `payload`; single version for v0.
- **Confluence:** a stated SM contract; Phase 1 doesn't stress it.
- **Convergence hint / rebase:** swappable hash hint for genuine conflicts (cold path,
  not in the contract); rebase = auto-re-author, orphans inert, bounded by immutable
  finality. Both surfaced to the app.

## Data flow — the control round-trip

```
sentinelctl.system → mesh.author("stop X")            // node validates command_allow, signs, gossips
   … node folds; the event's validity becomes permanent (all-members-witnessed) …
node ⇒ emits finalized { [stop X] }  → sentinel.system  // real → sentinel acts
sentinel.system → (kills X)                           // side effect, owner-only, in the executor
sentinel.system → mesh.author("response: stopped X")  // result re-enters the log as data
   … finalizes …
node ⇒ emits finalized { [response…] } → sentinelctl.system   // matched by corr_id → done
```

Every boundary is one of the three interfaces; nothing leaks. The node never interprets
a payload; the SM never sees the graph; the executor is the only thing that touches the
world, and its output is just another event.

## Maps to the frame's open decisions
- **§9.1 boundary** → dumb core (identity + gossip + partial order + witness queries);
  membership/finality leave; finality returns as an opt-in, conflict-scoped utility.
- **§9.2 consistency** → per-consumer, not a mesh mode; "eventual" = instant finality
  for conflict-free SMs; only finalized is ever state.
- **§9.3 SM-interface** → the three interfaces above; pure ancestry-validated
  `state-machine`, `mesh` requests, an emitted `dag-node` stream; no tiebreak in the
  contract.

## First step
v0.4 is landed (the clean base). Build **v0 directly**: the SM-boundary plumbing (the
three interfaces + a trivial SM to prove composition), then strip the core to dumb +
admission-final fold + the emitted stream + conflict detection/surfacing, validated
against the two conflict-free SMs (oracle = the sentinel round-trip + a chat smoke).
