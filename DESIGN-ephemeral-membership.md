# Design: ephemeral membership (self-serve join → sync → act → depart)

**Status:** draft / working design. Not yet implemented; not yet shared with the fleet.

## Motivation

The control plane wants **ephemeral nodes**: a short-lived node (e.g. `sentinelctl`)
that joins the mesh, syncs current state, submits one command (and reads the
response), then leaves — without being a permanent member. Concretely: an
operator's `sentinelctl list` spins up a one-shot node, drives sentinel over the
mesh, and disappears.

The ideal lifecycle:

```
join (self-serve)  →  sync to current state  →  act (submit/await)  →  depart cleanly
```

## Current state (what already exists)

The core join→sync→act→depart flow **already works** and is exercised by
`membership-test/` — so the DAG/finality machinery does not need a rewrite. What
exists today:

- **Admission is by finalized membership.** The handshake (HELLO → CHALLENGE →
  AUTH) rejects any pubkey not in `consensus_members()` at HELLO. A non-member
  cannot complete the handshake at all.
- **Runtime join exists, but must be pre-arranged.** A node absent from the
  genesis config is a "joining node" (authors no genesis, dormant until admitted).
  An **existing member** authors an `Introduce(pk)`; once it *finalizes*, the new
  node can connect (handshake now passes), sync, and act.
- **Sync via catch-up.** On connect: `WANT`/`FRONTIER` backfill + `CHECKPOINT`
  adoption if behind a pruned watermark. There is **no explicit "synced" signal** —
  apps currently just wait on a timer (`membership-test` sleeps ~1.5s).
- **Depart is self-authored.** A node authors its own `Depart`; once finalized,
  survivors finalize independently again. **Third-party depart is rejected.**
- **Finality is all-members.** An event finalizes when every member *at that
  event's position* has witnessed it. A member that is **offline / never
  witnesses stalls finality indefinitely** — there is **no eviction**.

### The three gaps for a *clean, robust* ephemeral pattern

1. **Join trigger** — a node can't self-introduce, and a non-member can't connect,
   so *someone inside* must `Introduce` it before it connects. Not self-serve.
2. **Sync-ready signal** — no event says "you're caught up"; apps guess with a timer.
3. **Crash eviction** — an ephemeral node that dies *before* departing wedges
   finality forever, and can't be force-removed.

---

## Design 1 — Self-serve admission (the join trigger)

**Decision: APPROVED — build this.**

**Idea:** decouple *"authorized to join"* from *"currently a member."* Pre-provision
a set of **authorized joiner pubkeys** (`join_allow`); a node holding one of those
keys can join self-serve; whichever member it dials **auto-introduces** it.

**Chosen shape: a pre-defined allow-list of per-node pubkeys** (not a single shared
secret). Rationale:
- Reuses the existing signature challenge — a joiner proves it owns an allowed key
  the same way members authenticate today. No new crypto.
- Per-node identity → per-node authz (`command_allow`) and per-key revocation. A
  single shared secret allows joining *as any identity* and only all-or-nothing
  revocation.
- Unification: the manager's control keypair can BE the key in `join_allow` — one
  keypair = join credential + membership identity + `command_allow` authz identity.

**Mechanism:**
1. Config gains `join_allow`: a set of pubkeys permitted to join (distinct from
   `members`, the current set).
2. Widen the HELLO gate: accept a connecting pubkey if it is in
   `consensus_members()` **or** in `join_allow`. The CHALLENGE/AUTH signature step
   is unchanged (still proves key ownership).
3. On a successful handshake by a `join_allow` pubkey that is **not yet a member**,
   the dialed node authors `Introduce(pk)` automatically.
4. Once that `Introduce` finalizes, the joiner is a full member: it syncs, acts,
   departs as today.

**Open questions:**
- **Is `join_allow` static config or finalized meta-state?** Static config is
  simplest (all members provisioned alike, like `members` today); a finalized
  `join_allow` set would allow runtime changes but adds consensus surface. *Lean:
  static config to start.*
- **Candidate state between connect and introduce-finalized.** The joiner is
  connected + syncing but not yet a member — can it receive backfill but not
  author until its `Introduce` finalizes? (Probably yes: it needs the sync to see
  the `Introduce` in its ancestry before its first event is valid.)
- **Races / duplicate introduces.** If the joiner dials several members at once,
  each might author an `Introduce(pk)`. Ingestion already rejects a duplicate
  `Introduce` (pk already a member at that frontier), so the second is a no-op —
  but confirm it's a *clean* no-op, not a poison event.
- **Who introduces in a multi-member mesh?** The dialed member. Fine for the 2-node
  control mesh; revisit for larger meshes.

---

## Design 2 — Sync-ready signal (IN FOCUS)

**Problem:** apps sleep on a timer because nothing says "caught up." An ephemeral
node wants to act the *instant* it can, not after a fixed guess (too short → its
event buffers on missing deps; too long → wasted latency).

### What "ready" actually means — two conditions, not one

For an ephemeral node whose purpose is *submit a command that finalizes, read the
response*, "ready to act" is:

1. **Admitted** — its own `Introduce` has arrived AND **finalized** in its view (it
   sees itself in `consensus_members()`). Under Design 1's auto-introduce the
   Introduce is authored by the peer *after* connect, so this is the gating event,
   not the initial backfill.
2. **Synced** — caught up to the peer's frontier **as advertised at connect**: all
   `WANT`s resolved + any `CHECKPOINT` adopted. This is a **snapshot-in-time**
   milestone ("I have what the peer had when I connected"), not a global guarantee —
   new events may still arrive; the node's command simply grafts the latest heads it
   knows. This is the honest, locally-detectable bar.

So the useful signal is a **combined "admitted ∧ synced → ready-to-act."** The node
detects both locally (it knows when its Introduce finalizes and when its WANT set
drains).

### This is a core node ↔ mesh-client signal, NOT mesh-control

Readiness is a transport/lifecycle fact about the node's own sync state — *not* an
app-to-app control envelope. It rides the node↔app message-server channel and is
surfaced through mesh-client alongside `delivery`. It must **not** live in
`mesh-control` (which carries Command/Response/Lifecycle between apps over the mesh).

### Surface: push (lean) vs poll

- **Push (lean):** the node proactively notifies the app "ready" over the same
  app-notify channel it already uses for committed deliveries (`NOTIFY` →
  `handle-send`) — consistent with the existing push model, immediate reaction.
  Means generalizing mesh-client's decoder: today `delivery(msg) -> Option<(from,
  body)>`; instead a tagged `incoming(msg) -> Incoming` where `Incoming = Ready |
  Delivery{from, body}` (room to grow). The node tags its app-bound frames.
- **Poll (simpler):** the app calls `sync-status()` via request/response and loops
  until ready. No new push type, fits the existing request path, but adds latency
  granularity + extra round-trips.

*Lean: push* — consistent with how deliveries already reach the app, with one tagged
app-inbound decode path shared by Ready and Delivery.

### Signal OR timeout (not instead of)

The signal makes the happy path fast; the app keeps a **timeout backstop** — if
"ready" never arrives (can't sync / never admitted), the ephemeral node errors out
and departs. So: act on `ready` OR on timeout-with-error.

### Resulting ephemeral app loop

```
spawn node → await Ready (or timeout) → submit(command)
           → await Delivery matching corr_id (or timeout) → depart
```

### Open questions
- **Exact "synced" predicate — RESOLVED:** `synced = every one of the peer's
  connect-advertised heads (ACCEPTED + FRONTIER) is admitted` (present in `events`,
  not stuck in `pending`). Because admission is gated on *full ancestry present*,
  holding a head implies holding its entire past — so this is exactly "I have what
  the peer had at connect," locally detectable with no new protocol (just remember
  the advertised head hashes and watch for all-present).
- **One combined signal vs two:** emit a single `Ready` (admitted ∧ synced), or
  separate `Admitted` / `Synced` for non-ephemeral consumers? *Lean: one combined
  `Ready`, predicate kept internal.*
- **Tagged `incoming` vs a second callback:** generalize `delivery` → `incoming`
  (one decode path), or add a distinct ready callback? Generalizing is fewer moving
  parts and additive to mesh-client's surface. *Lean: tagged `incoming`.*

---

## Design 3 — Membership under failure (RESOLVED)

**Problem:** strict all-members finality means a member that crashes or times out
*before* departing requires its witness forever — stalling finality **and** freezing
the committed frontier (so trimming stops; see
[`DESIGN-retention.md`](./DESIGN-retention.md)). The mesh keeps strict all-members for
*commitment* (split-brain impossible by construction); this section is how it stays
*live* under failure.

### Heartbeat pump

Each member, every `N` + jitter, authors a **noop event** (empty payload — the existing
"pure graft / heartbeat"), whenever there is ≥ 1 peer (a sole member's events
auto-finalize, so there's nothing to prove liveness to). This:
- keeps the committed frontier **always advancing** → retention can trim continuously
  (bounded storage),
- gives a **per-member liveness signal**, and
- makes **wedged-vs-idle unambiguous** — an idle network still heartbeats, so "no
  progress" now means a real wedge, never idle.

Jittered intervals avoid lockstep rounds. Cost: a steady stream of committed events, so
retention/materialization must keep pace — the same frontier-motion that enables
trimming.

### Staleness (observer-local)

A member you have **not received a new event from in `T`** — by *your own* clock, never
the peer's self-stamped time — is stale. Since everyone heartbeats every ~`N`, a live
member always produces events, so silence past `T` = failed.

### Failure response — quorum-evict, or restart when no quorum is possible

Depends on member count — the classic **2f+1** threshold (3 members to tolerate 1
failure):

**N ≥ 3 (a survivor-majority exists) → quorum-evict.**
- An `Evict{node}` takes effect only when signed by a **majority of the member set**,
  each signer having independently confirmed — by its own clock — that `node` is silent
  past `T`.
- **Timeout is a lower bound:** a majority must *each independently* observe the full
  timeout, so clock skew only **delays** eviction, never fires it early.
- **Partition-safe:** a symmetric partition has at most one majority side — it evicts
  and proceeds; the minority can't reach majority and **halts**. Both sides agree on the
  (committed) member count, so each computes its own majority status.
- **Finality of the `Evict`** is over `members_at_frontier` MINUS `node` (the evictee
  needn't witness its own removal); the *authorization* is the majority signatures.
- **Re-join, not banishment:** an evicted (or wrongly-evicted-because-slow) node whose
  key is still in `join_allow` re-joins via self-serve and retries. Committed work
  stands; only un-committed in-flight events drop — an aggressive `T` costs a retry, not
  safety. Normal commitment stays all-(remaining)-members; only the membership-*shrink*
  uses the majority.

**N = 2 (no quorum possible) → self-destruct + restart.**
- With one survivor, a majority (2) is unreachable — eviction is *mathematically
  impossible*. So the survivor node **exits**; its supervisor (e.g. sentinel) restarts
  it fresh, resetting to the genesis member set. The wedge is gone; the failed peer
  re-joins via self-serve if it recovers. This is the *only* possible response at N=2,
  and it is exactly a network-failure + retry.

### Consequences / operating rules

- **3 members is the minimum for fault-tolerant (evict-and-continue) operation** (the
  2f+1 bound). A 2-member mesh has **zero** fault tolerance — a failure is a clean
  restart.
- **N=2 restart is safe only for transient/restartable meshes** (the control mesh:
  request/response, no persistent state to lose). A persistent **data mesh should run ≥
  3** so a failure never forces a wipe.
- **The control mesh degrades gracefully:** sentinel + 1 operator = 2 members =
  restart-on-failure (disrupts only that operator, who retries); sentinel + ≥2 concurrent
  operators = ≥ 3 = surgical eviction. More operators → more resilience.
- **Best-effort depart** on teardown keeps clean exits from triggering either path.
- **Supervisor-restart is otherwise just generic crash recovery** (theater restarts any
  crashed child); it is the *membership* response only in the N=2 no-quorum case.

### Strict mode (optional)

A correctness-over-liveness network can disable the heartbeat/eviction/restart machinery
and run pure strict all-members — a failure halts until the member returns or
self-departs. Config option, not the default.

### Deferred
- **Skew-hardened clock schemes** — reduce reliance on raw wall-clock; revisit.
- **Timestamp field usage** — the signed `Event.timestamp` isn't load-bearing yet
  (staleness is observer-local; ordering is causal); kept for audit + future use.

---

## Sequencing / recommendation

1. **Slice-1 proof (no core changes):** orchestrate an `Introduce` (as
   `membership-test` does) → join → sync (timer for now) → `list` round-trip →
   depart. Proves the mechanism end-to-end.
2. **Design 1 (self-serve join):** the ergonomic unlock; contained to the
   admission gate + an auto-introduce hook. Do this next.
3. **Design 2 (sync-ready):** removes the timer guesswork; modest.
4. **Design 3 (crash eviction):** the real engineering and the safety gate. I'd
   want at least the interim mitigations (best-effort depart + separate control
   mesh) before this drives anything important, and treat full eviction as a
   deliberate consensus-safety project, not a quick add.

## Transport note (settled)

Independent of the above: an ephemeral node reaches sentinel by **dialing** its
control node (single dial peer); it needs **no inbound listen port** — gossip is
bidirectional over the dialed connection, so the response event returns over it and
is delivered to the app in-process. `sentinelctl` config: `node_seed` = manager
keypair, `dial` = [sentinel control node], `members`/`join_allow` per Design 1,
`listen_addr` = throwaway loopback.
