# HANDOFF — mesh RSM v0 build (read this first)

Transient working note for resuming the RSM implementation in a fresh session.
Not a committed artifact; delete when v0 lands.

## Where we are
Reshaping mesh into a **replicated state machine** (dumb core + consumer state
machine). Design is LOCKED and shared with the fleet. Building **v0 directly**
(admission-final; see below). **Steps 1–2 + step 3a (ancestry-relative fold) +
BOTH oracles are done:** the real control-SM + step 5 (two-node control
round-trip, `control-roundtrip-test/`, GREEN) AND the chat-SM + step 6 (two-node
chat smoke, `chat-smoke-test/`, GREEN — including the rejection path: a
non-member's post is not delivered until `member-add`). Both consumers now run
end-to-end across two composed nodes, so the dumb core is proven to generalize.
**Remaining to land v0:** conflict/stranded surfacing (step 4 — SM-validation
rejection is currently a silent skip; make it emit `conflict`/`stranded`);
Interface 2/3 polish (the `mesh` request iface + emitted `dag-node` stream +
`event-status`, still on the old mesh-api NOTIFY/delivery). See the
`mesh-rsm-reshape` memory for the current REMAINING list + the fixed-packr caveat
(composed artifacts only *run* when composed with `/home/colin/work/pack/target/
release/packr … --output …`, not the 0.12.2 nix binary).

## Read first (the durable sources)
- **`DESIGN-rsm.md`** — the pinned contract (5 principles, 3 interfaces, admission-final
  v0, conflict detection kept, deferred conflict-prone bundle).
- **Memory `mesh-rsm-reshape`** (`.claude/.../memory/mesh-rsm-reshape.md`) — full state,
  the fleet division, every decision, and the exact step-2 first moves.
- **Memory `fleet-mail-setup`** — how fleet email works (needed because the mail
  mechanics used to ride in the conversation summary, which `/clear` drops).
- **`state-machine.pact`** — the locked Interface-1 contract, and `sm-trivial/` — a
  working reference SM to compose against.

## Step 1 (DONE): composition proven
`state-machine.pact` validated; `sm-trivial/` (exports the interface) + `sm-smoke/`
(imports it) build; `packr compose sm-smoke/smoke.compose.toml` links them (4 links,
hash-match). The node-calls-into-a-composed-SM direction works.

## Step 2 (DONE): dumb core + composed SM import + admission-final fold
- Node `packr-guest` 0.11.0 → 0.12.6; `imports { state-machine {…} }` + `#[import_from]`
  bindings added (validate/apply/initial-state/members; `members` dormant, declared for
  hash stability — DCE drops the uncalled wasm import, so it is NOT wired in compose).
- **Stripped** across event.rs/wire.rs/dag.rs/codec.rs/lib.rs: `SystemOp` +
  the `system` envelope field (+ its wire byte — event encoding changed); `fold_membership`,
  `members_at_frontier`, `consensus_members`, `is_finalized`, `ordered_finalized`,
  `base_members`, `apply_evictions`, the ingest membership gate; eviction / finality-
  heartbeat / `last_heard` / N=2-shutdown; `FRAME_INTRODUCE`/`FRAME_DEPART` +
  Introduce/Depart/self-serve-join (handshake is now identity-proof-only, permissive);
  receive-side witnessing grafts. Kept: Event sign/verify + canonical encoding, DAG,
  witness queries (`ancestors_of`/`events_that_see`/`witnesses` — `#[allow(dead_code)]`
  until step 3 wires Interface 2/3), gossip, persistence. `topo_sort` → `ordered()` (local).
- **Fold rewired:** `deliver_committed` folds `dag.ordered()` through the SM's
  `validate`/`apply` from `initial_state`; `finalized = admitted`; a validating payload
  is delivered immediately, a non-validating event is skipped (conflict/stranded
  surfacing is step 4). Membership is now an SM payload authored via `Submit`
  (Introduce/Depart commands decline with "author it via Submit").
- **Green:** wasm build + `clippy -D warnings` clean; 19 host `cargo test --lib` pass;
  `packr compose node-compose.toml` (node ⊕ sm-trivial, 3 links) → `wasm-tools validate`
  OK + `packr verify --host-only` OK (SM import fully internalized). Toolchain = the
  locally-rebuilt packr 0.12.6 with the compose fix (see the packr-compose-bug memory).

## Step 3 (NEXT): emitted stream + the `mesh` interface
- Interface 2 (`mesh`): node implements `author` / `current-state` / `event-status` /
  `witnesses` / `ancestry` as message-server requests (wire `witnesses`/`ancestors_of`
  in — they're the `#[allow(dead_code)]` methods now).
- Interface 3: emit the `finalized` / `conflict` / `stranded` `dag-node` stream instead
  of (or alongside) the current NOTIFY/delivery; the executor linearizes locally.
- Then step 4 (ancestry-relative validate + conflict detection/surfacing), steps 5–6
  (compose against the real control-SM + chat-SM; oracle = sentinel round-trip + chat smoke).

### Stale until steps 5–6
The event wire format + handshake changed, and membership/eviction are gone, so
`evict-test` / `membership-test` / the Introduce/Depart paths in `app-test`/`join-*`
are behaviorally obsolete (they still compile — separate network clients). Re-home onto
the control-SM round-trip in step 5.

## Base
v0.4 full-retention landed: 2 local jj commits, **UNPUSHED** (`pzmkoovo` code +
`qymupwsw` DESIGN-rsm.md).

## Fleet
On hold per memory `hold-fleet-comms-until-landed` — no replies until v0 lands. There
are unanswered *confirmation* mails in `mesh-dev@` (nothing needing action). To pull
the thread: `/home/colin/work/actors/inbox/cli/inbox read mesh-dev@colinrozzi.com --since 110 --full`.
Both SM drafts: `/tmp/CONTROL-SM-design.md` (sentinel-dev) + `/home/colin/work/manager/chat-sm-design.md` (manager).
