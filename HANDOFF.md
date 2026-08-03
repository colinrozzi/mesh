# HANDOFF — mesh RSM v0 build (read this first)

Transient working note for resuming the RSM implementation in a fresh session.
Not a committed artifact; delete when v0 lands.

## Where we are
Reshaping mesh into a **replicated state machine** (dumb core + consumer state
machine). Design is LOCKED and shared with the fleet. Built **v0 directly**
(admission-final). **v0 IS LANDED — the full DESIGN-rsm.md contract runs, proven
end-to-end by two oracles.** All six build steps done:
- **Interface 1** (state-machine, composed) — proven against TWO real, unrelated
  SMs: `control-sm/` (membership + command/response journal) and `chat-sm/`
  (OR-Set membership + text log). So the dumb core is proven to generalize.
- **Interface 2** (`mesh`, node answers requests): `author` is PRE-VALIDATED
  (Err = the SM's reason); `current-state` / `event-status` / `witnesses` /
  `ancestry` are the QUERY verbs. Bound over the TCP app protocol (src/wire.rs).
- **Interface 3** (node emits): `finalized` dag-node stream (FRAME_FINALIZED) +
  fail-loud `conflict` (never fires for honest conflict-free v0 peers — the
  checked safety net); `stranded` frame defined for the deferred bundle.
- **Oracles GREEN:** `control-roundtrip-test/` (genesis→join→command→response
  over bidirectional gossip; asserts finalized dag-node deps, event-status,
  ancestry/witnesses, and byte-identical current-state CONVERGENCE across both
  nodes) + `chat-smoke-test/` (2 nodes, delivery both ways + author-time
  rejection of a non-member post until `member-add`).

**Deferred (not v0-blocking):** (1) the **message-server binding** of Interface
2/3 — the co-located-app transport still carries the old (author,payload)
delivery + Submit/Register; upgrading it to the query verbs + dag-node stream is
mechanical transport-mirroring with NO new semantics, and needs a theater
executor actor to test (the TCP binding is fully proven). (2) The conflict-prone
bundle (witness-finality-as-gate, kick, rebase/convergence helpers, compaction) —
arrives with the first conflict-prone consumer. See the `mesh-rsm-reshape` memory.

**Fixed-packr caveat:** composed artifacts only *run* when composed with
`/home/colin/work/pack/target/release/packr … --output …` (== published 0.12.7),
not the 0.12.2 nix binary. Every oracle recomposes with it.

## Post-v0 hardening (2026-08-03)
v0 is **PUSHED** (main == `oqqqkwty`). Integration work landed on top (see the
`mesh-rsm-reshape` memory for the blow-by-blow):
- **Confluence flagship** (`confluence-test/`, pushed) — concurrent authoring across
  a partition; found+fixed 3 substrate bugs (gossip relay, non-confluent fold →
  two-pass finality/application split, no anti-entropy).
- **Fail-loud conflict injection** (`conflict-injection-test/`, pushed) — dishonest
  peer forges a non-member event; node fires `conflict` + `stranded`, keeps it inert.
- **Scale test + fold memoization** (`scale-test/`, **2 UNPUSHED commits**: `wkuqyzlx`
  perf + `mxywvqsx` test). N-node line topology, wide concurrent frontier. It measured
  the fold wall (40 events = 22s author + 74s converge) and drove the fix: per-event
  **finality is memoized** (immutable per principle 1) in `final_json`/`ensure_finality`,
  so `current-state`/`event-status`/`deliver_committed` read a shared cache instead of
  re-folding per event — kills the O(events³) term. After: 0.85s/0.61s (~25×/~122×),
  still byte-for-byte confluent to 8 nodes / 200 events. Residual is quadratic, floored
  by per-event ancestry validation + theater's full-DAG-JSON-per-call persistence, not
  the fold. **Suite (all green):** control-roundtrip, chat-smoke, confluence,
  conflict-injection, scale + 19 host + 6 + 6 SM unit tests + clippy.
- **STALE:** `multi-node-test` (+ likely `join-test`/`evict-test`/`membership-test`/
  `app-test`) point at RAW `mesh.wasm`, which post-reshape can't instantiate alone
  (`unknown import: state-machine::initial-state`). Pre-existing; re-home onto a
  composite when touched.

**Next / open:** push the 2 scale commits (Colin's go); lift the fleet-comms hold now
that v0 is landed+pushed; pin `packr-guest 0.12.7`.

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
