# Design: finality-anchored retention (sliding-window state root)

**Status:** draft / working design. Not yet implemented. Sibling to
[`DESIGN-ephemeral-membership.md`](./DESIGN-ephemeral-membership.md) — they share
the finality machinery.

## The idea in one line

Hold the **committed frontier** (the leading edge of committed events, as anchors)
+ a **materialized snapshot of committed state** (the "root") + the live **working
region**; as the frontier advances, trim the committed interior behind it. A node
always holds *current committed state*, never the spent history that produced it.

## How "committed" works today (grounding)

Finality is **all-members witnessing, cryptographically**, and it already exists:

- Every event is **ed25519-signed** by its author and the signature is **verified on
  ingest** (`dag.rs`, `ingest_into` → `verify_signature`).
- A node **witnesses** event `E` by authoring its *own* signed event that
  transitively references `E` (via `self_parent`/`refs`, tracked in the
  `observed_by` back-edge index). Witnessing is the causal edge inside a signed
  event — there is no separate "vote."
- `E` is **committed/finalized** iff every member *live at E's position* has
  witnessed it (`dag.rs:299`): `members_at_frontier(E.deps) ⊆ witnessing_authors(E)`.
  You cannot forge another node's witness — it needs their key.

**Key consequence:** the committed cut is a **pure, deterministic function of the
DAG**. Every full node with the same history computes the *identical* cut; two full
nodes computing different cuts is a failure mode (corruption/Byzantine) that should
*halt*, not be reconciled. So among full nodes there is nothing to "verify" — the
signed witnesses *are* the proof, inherently.

## What the current prune does (and the gap)

`compact()` (`dag.rs:338`) already prunes at the right boundary:

> `common_ancestors(all members' heads)` **∩** `is_finalized`

i.e. events every member has in their causal past **and** that are fully committed —
exactly the cut. It folds pruned membership ops into `base_members` and keeps
`sealed` boundary anchors so retained events still validate.

**The gap:** it keeps only *structural* summaries (`base_members` + `sealed`). It
does **not** keep the committed *application state* — the reducer **re-folds
`ordered_finalized` from the events every call** (`dag.rs:308–311`, "re-folds each
call"). And the `CHECKPOINT` frame transfers only `(base_members, sealed)` —
structure, no state. So a node that trims (or adopts a peer's checkpoint) can
validate *structure* going forward but has **discarded the state/payloads below the
cut and cannot re-derive them.** That is the "missing events" risk.

## Why the state-root model closes the gap

Retain the **root = the materialized committed state itself**, not just structural
anchors. The raw events below the frontier are redundant — their entire effect is
already folded into the root — so trimming them loses nothing. A joiner or a
restarting node takes `root + frontier + working region` and is whole. Nothing
committed is ever missing, because committed *state* is what you keep; only the
spent raw events drop.

## Concrete deltas from today

1. **Materialize state incrementally.** Fold committed events *forward* into a
   persisted snapshot instead of re-folding. Today it re-folds (DESIGN.md calls
   incremental application a near-term optimization); here it becomes a
   **prerequisite** — an event may not be dropped until its effect is in the root.
2. **Carry a rolling "root".** A commitment (a hash over committed state +
   membership) that advances with the frontier, so the retained state is
   self-describing and transferable. Today: `base_members` + `sealed`, but **no
   state root** — this adds it.
3. **Already present:** frontier anchors (`sealed`) and folded membership
   (`base_members`).

## Retention policy: trim vs. full-retention

The mechanism above enables a *policy* choice, per network:

- **Trim-at-cut (bounded):** keep `root + frontier + working region`; drop the
  committed interior. Storage is bounded by the working-region size. A **brand-new
  node has no history to recompute from**, so it bootstraps from a peer's `root +
  frontier` — *trusting* it (fine under crash-fault) or checking a **witness
  certificate** (a retained aggregate of the cut's signed witnesses) if you want
  Byzantine safety.
- **Full-retention (unbounded):** keep everything. **Trustless and self-proving** —
  the signed witnesses already are the certificate; a joiner verifies the whole
  chain itself. Audit-friendly. The natural default when storage isn't the
  constraint.

Certificates are therefore *purely a consequence of choosing to trim* the signed
witnesses — not an inherent tax.

## The liveness coupling (the linchpin)

The frontier only advances as **finality** advances, and finality is **all-members**
— so **one stalled/offline member freezes the frontier, the working region grows
unbounded, and you cannot trim.** Bounded storage is therefore **gated on
crash-eviction** (see `DESIGN-ephemeral-membership.md` Design 3). This coupling is
unavoidable in an all-members model; it's why eviction keeps surfacing as central.
During a stall the options are: grow unbounded, refuse new events, or evict.

## Interaction with ephemeral membership

These compose cleanly: an **ephemeral joiner needs only current state + members, not
history**, so `root + frontier` is exactly the right bootstrap payload for it — it
takes the root, sees current membership, references the frontier as its refs, acts,
departs. Trim-at-cut is the natural retention policy for a control mesh whose
members are ephemeral. (Full history stays the opt-in policy for meshes that want a
complete picture.)

## Open questions

- **State-root construction:** a Merkle root over *what* — the reduced app state,
  the membership set, the frontier hashes, or a tuple? Needs to be a deterministic
  commitment both sides can recompute.
- **Root transfer:** extend `CHECKPOINT` to carry the root + committed state, or add
  a distinct frame? The current `CHECKPOINT` is structure-only.
- **Frontier precision:** the committed frontier is an antichain (the boundary
  between all-finalized and not-yet). `compact` approximates via
  common-ancestors-of-heads; pin the exact definition.
- **Witness certificate format (trim + Byzantine):** aggregate signatures over the
  cut? Threshold? Only needed if a trimmed network must stay trustless.
- **Reducer/state persistence:** where the materialized snapshot lives in
  `ActorState`, and how a restart rehydrates from `root` without the events.
- **Stall policy:** grow / refuse / evict — the explicit choice when the frontier
  is frozen by an unreachable member.

## Sequencing / recommendation

1. **Incremental state materialization** (delta #1) — worth doing regardless; it's
   the near-term reducer optimization and the prerequisite for everything here.
2. **Rolling state root** (delta #2) — makes committed state self-describing +
   transferable; closes the missing-events gap.
3. **Trim-at-cut policy** on top, for bounded meshes — with the honest dependency on
   eviction for the frontier to keep advancing.
4. **Witness certificates** only if/when a trimmed mesh needs Byzantine-safe
   bootstrap; full-retention meshes never need them.
