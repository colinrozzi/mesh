# Compaction: quorum-certified state checkpoints

**Status:** design / not yet built. Supersedes the seal-and-prune retention model
(`DESIGN-retention.md`) as the intended long-term shape.

This doc describes how a mesh trims its history without losing the ability to
*verify* the state that history produced — by treating retention as **compaction**
(absorb the settled past into a signed state checkpoint) rather than **garbage
collection** (delete the settled past and leave a blind marker).

## Why the current model is wrong (the seam we found)

Today (`DESIGN-retention.md`): compaction prunes finalized, non-system events and
leaves a `SEALED` marker in their place. Membership (system) events are retained
so the member set stays "derivable from signed history."

The flaw: **retained ≠ reachable.** The member set is computed two ways, and they
disagree once history is sealed:

- `consensus_members` iterates *all retained events* and folds the membership ops
  directly — so it still sees a retained `Introduce`.
- `members_at_frontier` walks *ancestry* (`ancestors_of`), and that walk **stops
  at a sealed hash** (its parent links are pruned). So a retained `Introduce` that
  sits *behind* a sealed boundary is present in the DAG but unreachable — and the
  positional member set under-counts.

`members_at_frontier` feeds `is_finalized` (the "required witnesses" set), so the
under-count means an event can finalize with fewer witnesses than the all-members
rule requires, and two nodes with different seal states can *disagree on finality*.
(Tracked as the `#[ignore]`d `membership_under_a_mark_sealed_boundary` test.
Precision-only — never stuck — and normal compaction doesn't trigger it, but it is
a genuine correctness gap.)

Root cause: **the `SEALED` marker carries no forward state.** We kept the events
but threw away the *path* to them, and left nothing in the gap that the walk can
read. Compaction should leave behind a *signed, self-describing boundary*, not a
blind hole.

## The reframe: compaction, not GC

Two kinds of events:

- **Liveness-noise** — empty-payload heartbeats and pure witnessing grafts. They
  exist only to advance the frontier, prove liveness, and carry witnesses. Once
  finalized their lasting state is *nothing*. Fully absorbable.
- **True state changes** — and for mesh this is a gift: the substrate's only
  persistent state is **membership** (payloads are message-passing — delivered
  exactly-once on finality, then ephemeral; there is no substrate-level reducer).
  So "the tree of true state changes" is essentially the **membership-op tree**,
  which is small and a *pure deterministic fold*.

Compaction = absorb the settled liveness-noise into a checkpoint of the true state,
keeping enough signed material to *verify* that checkpoint.

## The core design: `Checkpoint` as a `SystemOp`

A checkpoint is **finality applied to a state snapshot** instead of to an event.
Mesh's core primitive is already "all members sign off ⇒ canon"; a checkpoint is
the same move one level up.

Add a membership-op:

```
SystemOp::Checkpoint { frontier: Vec<Hash>, state_hash: Hash }
```

- **Independent parallel attestation (no proposer, no trust).** Because the state
  is a deterministic fold, each member *independently* computes the state at a
  finalized cut `W` and authors its *own* `Checkpoint { W, hash(S) }`. A checkpoint
  becomes canon when **every current member has authored a *matching* one** (same
  `W`, same `state_hash`).
- **Canon = the existing all-members check.** "All members authored a matching
  `Checkpoint` op" is literally the all-members-finality rule applied to those ops
  — no new agreement code, just a new op the fold understands.
- **Fail-loud on disagreement.** If any node derived a *different* state (a bug or
  a Byzantine node), the hashes differ and **no checkpoint forms.** Disagreement is
  visible; it never silently becomes canon.
- **Drop and rebase.** Once the checkpoint at `W` is canon: drop everything
  causally ≤ `W`, and keep the checkpoint (the M signatures + the serialized state)
  as the new *base* — the genesis-equivalent. A joiner boots from it: verify the M
  signatures, adopt state `S` at `W`, then sync events after `W`.

The blind `SEALED` marker grows up into a **quorum-certified, self-describing
checkpoint boundary.**

### What gets serialized

Small, and canonical:

- the **member set** (sorted pubkeys), and
- the **delivered-watermark** (for exactly-once delivery across the boundary).

Canonical encoding is load-bearing: mismatched bytes ⇒ mismatched `state_hash` ⇒
no checkpoint forms. Membership serializes trivially; model it on the existing
canonical event encoding.

### Why this is *verified*, not *trusted*

This reconciles with the earlier "derive from events, never send a trusted
checkpoint" call. Nobody adopts a checkpoint on faith: every signer *derived* it
(that's why they could sign), and any consumer *verifies* the M signatures and can
recompute the state. It is a **quorum certificate over a deterministic state** — a
different animal from the bulk-transfer `CHECKPOINT` frame we (rightly) removed.

### How it subsumes #43

After a canon checkpoint at `W`, `members_at_frontier` for any later event folds
from the **checkpointed member set** (the new base), not from genesis. The settled
membership is *in* the base, so there is nothing behind a seal to strand. The
reachability gap disappears — not as a patch, but as a consequence of doing
retention correctly.

## Open questions (none fatal)

- **Liveness.** A canon checkpoint needs the *whole current member set* to sign, so
  a down member stalls checkpointing — but that is the *same* CP availability
  tradeoff as finality itself, and it piggybacks on **eviction**: a permanently-down
  member is voted out, shrinking the set the next checkpoint needs. Checkpoint
  liveness rides machinery we already have.
- **Choosing `W`.** Monotonic: later checkpoints supersede earlier; members converge
  on a common *finalized* frontier as the cut (only settled history is checkpointed,
  so it is safe + deterministic). Exact convergence rule under slightly-divergent
  frontiers needs pinning down.
- **Byzantine posture.** A bad member can *halt* a checkpoint (won't sign, or signs
  garbage ⇒ no all-match) but cannot *forge* one (needs all to agree on one hash).
  Halts, doesn't corrupt — exactly mesh's existing safety posture.
- **Cut shape across N chains.** `W` is a cut across every member's self-rooted
  chain; the frontier is a set of per-member heads. Serializing/agreeing that cut
  is straightforward but wants a precise spec.
- **Delivered-watermark form.** Member set is tiny; the delivered set could grow —
  does it need its own compacted form (an accumulator), or is a watermark + a small
  recent set enough?

## Future elegance (pinned, not required)

The checkpoint above is buildable today with plain ed25519 + sha256 and needs *no*
new cryptography. Everything below only makes the *certificate smaller/prettier* —
it is an optimization axis, not a prerequisite, and the "signed self-describing
boundary" shape is the socket all of it plugs into.

The dream that started this: a witnessing that *survives its own compaction*, so
the proof travels with the collapsed state instead of being discarded. Where the
cryptography actually is:

- **Rung 1 — BLS aggregate signatures.** Collapse the M per-member checkpoint
  attestations into *one* short signature verifiable against the combined key.
  Doesn't path-contract the graph; shrinks the certificate. New dep (pairings),
  modest.
- **Rung 2 — transitive signatures on linear noise-spines.** *Directed* transitive
  signatures over an arbitrary DAG are research-grade (and the hard, semi-open
  case). BUT the volume we want to shed — a member's heartbeat run — is a **linear
  self-parent spine**, and on a *line* the practical *undirected* transitive schemes
  (Micali–Rivest) are sound: an undirected chain has one shape, and direction is
  recovered from the two *retained endpoints* (their height order). So we might
  path-contract the bulk (linear noise) with an off-the-shelf primitive by *never*
  doing the general-DAG transformation. **Open problem:** heartbeats also carry
  `refs` (cross-member witnessing) — the self-parent spine and the cross-witnessing
  have to separate cleanly for this to hold. Wants a real cryptographer's review.
- **Rung 3 — recursive SNARKs / proof-carrying data.** The genuine
  "survives-any-transformation" answer: the boundary carries a constant-size proof
  that the settled state is the correct finalized fold of a validly-signed,
  all-witnessed history — verifiable with no underlying events. Real and deployed
  (Nova/Halo-style folding), but heavy: a proving-system dependency + per-checkpoint
  proving cost, a large jump for a `no_std` substrate. The elegant endgame.

Building Rung 0 (the `Checkpoint` `SystemOp`) does not foreclose any of these; each
just changes *what kind of signature/proof sits on the boundary.*

## Relationship to existing docs

- `DESIGN.md` — the substrate rationale (self-rooted logs, grafting-as-witnessing,
  all-members finality).
- `DESIGN-ephemeral-membership.md` — join / heartbeat / eviction (v0.3).
- `DESIGN-retention.md` — the seal-and-prune model this doc is intended to replace.
