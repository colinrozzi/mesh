# CLAUDE.md — mesh

Theater-native (WASM, `#![no_std]`) substrate: a gossip-replicated Merkle-DAG event
log, reshaped into a **replicated state machine** (a dumb core + a consumer-provided
state machine) — see `docs/DESIGN-rsm.md` for the locked design and `HANDOFF.md` for
where the build is right now.

## Repo layout
- Root: `src/` — the node (core); `mesh-api/` — the app protocol; `state-machine.pact`
  — the Interface-1 contract; `mesh-client/` — the system SDK (v2, the guest-side
  `Session` an executor drives its node through: RPC actions + the finalized stream).
- `tests/` — everything test-and-reference (an example worth making is reused as a test,
  so there is no separate `examples/`):
  - `tests/counter/`, `tests/echo/`, `tests/cluster/` — **the reference systems**, each a
    self-contained stack (SM/protocol + executor + its integration test) that IS the test.
    **Start here** to see how to build on mesh: `counter` (tier-1, single node), `echo`
    (tier-2, two-node request/response), `cluster` (tier-3, an orchestrator that spins up
    N nodes, drives a workload, and observes the whole network to convergence). All over
    RPC (actions) + the message-server stream (events); nodes gossip over TCP.
  - `tests/{control-roundtrip,chat-smoke,confluence,conflict-injection,scale}-test` —
    substrate property tests (driven via `testkit`'s std Client).
  - `tests/bank/` — the currency SM: the FIRST conflict-prone consumer (a transfer needs
    a balance). `bank-double-spend-test` shows the frontier LIVE — two concurrent
    transfers from one wallet both finalize and the network converges on a *negative*
    balance, i.e. admission-final finality is consistent but not sufficient. It is the
    future acceptance test for the deferred witness-finality bundle.
  - `tests/testkit/` — shared std test harness.
  - `tests/fixtures/` — the SMs those property tests compose the node against: the REAL
    consumer SMs `control-sm` + `chat-sm` (sentinel's + manager's; here as fixtures so the
    substrate is exercised against real consumers, slated to move to their own repos) with
    their `*-compose.toml`.
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
- **Compose** a node ⊕ SM with the FIXED packr (== published 0.12.7):
  `/home/colin/work/pack/target/release/packr compose <compose.toml> --output <out.wasm>`
  (the 0.12.2 nix binary produces artifacts that fail to load — LTO import-strip bug).
  Example composes live next to their example (`examples/*/*-compose.toml`); control/chat
  compose from root.
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
