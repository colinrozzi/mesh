# CLAUDE.md — mesh

Theater-native (WASM, `#![no_std]`) substrate: a gossip-replicated Merkle-DAG event
log, reshaped into a **replicated state machine** (a dumb core + a consumer-provided
state machine) — see `docs/DESIGN-rsm.md` for the locked design and `HANDOFF.md` for
where the build is right now.

## Repo layout
- Root: `src/` — the node (core); `mesh-api/` — the app protocol; `state-machine.pact`
  — the Interface-1 contract; `mesh-client/` — the system SDK (v2, the guest-side
  `Session` an executor drives its node through: RPC actions + the finalized stream).
- `tests/networks/` — **the state machines** (a "network" = a mesh running an SM). Each
  `networks/<name>/` holds the SM + its protocol crate + `*-compose.toml` (the node⊕SM
  artifact `mesh_<name>.wasm`):
  - `counter` — minimal, conflict-free, **typed state** (the generics example; `s =
    CounterState` record).
  - `echo` — request/response, conflict-free.
  - `chat` — OR-Set membership + text log (manager's; consumer fixture, pending handoff).
  - `control` — authz + command/response journal (sentinel's; consumer fixture).
  - `bank` — the FIRST **conflict-prone** SM (a transfer needs a balance).
- `tests/scenarios/` — **the tests** that drive a network + assert:
  - `counter` / `echo` / `cluster` — executor-driven (over RPC + the message-server
    stream), each with its `*-system` executor + `*-system-test`. **Start here** to see
    how to build on mesh: 1-node, 2-node request/response, N-node orchestrator+observer.
  - `control-roundtrip` / `chat-smoke` / `confluence` / `conflict-injection` / `scale` —
    substrate property tests (driven via `testkit`'s std Client).
  - `double-spend` — the bank frontier LIVE: two concurrent transfers from one wallet both
    finalize → the network converges on a *negative* balance (admission-final is consistent
    but not sufficient). The future acceptance test for the deferred witness-finality bundle.
- `tests/testkit/` — shared std test harness.
- `docs/` — `DESIGN-rsm.md` (live), `composition.md` (packr compose/packaging), +
  `history/` (superseded designs).

## Build & test
- **Build node (wasm):** `cargo build --release --target wasm32-unknown-unknown`
  The crate is `#![no_std]` for wasm — a host build pulls in std and conflicts with the
  guest panic handler, so **always use the wasm target** for build + clippy.
- **Unit tests (host):** `cargo test --lib` (std is available under `cfg(test)`).
- **Clippy:** `cargo clippy --target wasm32-unknown-unknown -- -D warnings` (CI treats
  warnings as errors).
- **Adjacent crates** are standalone (not a workspace) — build with
  `--manifest-path <path>/Cargo.toml`. SMs/systems (wasm cdylibs) need
  `--target wasm32-unknown-unknown`; the `*-test` runners are std host bins.
- **Compose** a node ⊕ SM with the **official** packr 0.13.0
  (`cargo install packr@0.13.0` → `~/.cargo/bin/packr compose <compose.toml> --output
  <out.wasm>`). REQUIRED now that the node is GENERIC (`state-machine<s>`) — 0.13.0
  carries the M4 compose-time unification that binds `s` to the SM's concrete state.
  The local `/home/colin/work/pack` build is STALE (pre-M4b) and rejects the generic
  link with a hash mismatch. Compose tomls live under `tests/*/`.
- **Integration tests self-spawn theater** (`/home/colin/work/theater/target/release/theater`):
  build the `-test` bin, then run it — e.g.
  `./examples/echo/echo-system-test/target/release/mesh-echo-system-test` or
  `./tests/control-roundtrip-test/target/release/mesh-control-roundtrip-test`. **`pkill -x
  theater` between runs — never `pkill -f`.** Network + process spawning → run with
  `dangerouslyDisableSandbox: true`.

## Composition (packr)
Components compose via **packr**: `.pact` interface files + `packr compose` + one
`[[link]]` per function. The guest side is `packr_guest::pack_types!` + `#[import_from]`
— the SM *exports* the `state-machine` interface, the node *imports* it. `examples/counter/`
is the simplest end-to-end reference (protocol + SM + node compose + executor + test).

## VCS
jj (jujutsu) colocated with git. Commit or push **only when asked**, and **never push
without Colin's direct in-terminal go** (pushing/releasing is outward-facing). End
commit messages with the `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
trailer.

## State, work, and comms — pointers (read on demand)
- **Current work + resume point:** the `mesh-rsm-reshape` memory (auto-surfaced via
  `MEMORY.md`), then `docs/DESIGN-rsm.md`.
- **Project state, decisions, preferences:** the memory system — the `MEMORY.md` index
  auto-loads; the files under `memory/*.md` are read on demand.
- **Fleet email** (this project's identity = `mesh-dev@colinrozzi.com`): quick-ref in
  memory `fleet-mail-setup`; the canonical guide is `/home/colin/work/actors/inbox/CLAUDE.md`
  (agent guide + the "Compatriots" roster) with `inbox/README.md` for the system.
  Outward mail needs Colin's direct go; comms may be **on hold** — check
  `hold-fleet-comms-until-landed`.
