# CLAUDE.md — mesh

Theater-native (WASM, `#![no_std]`) substrate: a gossip-replicated Merkle-DAG event
log, reshaped into a **replicated state machine** (a dumb core + a consumer-provided
state machine) — see `docs/DESIGN-rsm.md` for the locked design and `HANDOFF.md` for
where the build is right now.

## Repo layout
- `src/` — the node (core); `mesh-api/` — the app protocol; `state-machine.pact` — the
  Interface-1 contract.
- `control-sm/`, `chat-sm/` — the real fleet SMs (sentinel's + manager's; here for now,
  slated to move to their own repos). `control-compose.toml` / `chat-compose.toml` compose them.
- `examples/` — the reference systems, each a self-contained stack (protocol + SM +
  executor + its integration test) that is ALSO a test: `counter/` (tier-1, single node),
  `echo/` (tier-2, two-node request/response). This is where the intended pattern lives.
- `tests/` — substrate integration tests: `testkit` (shared std harness) +
  `control-roundtrip-test`, `chat-smoke-test`, `confluence-test`, `conflict-injection-test`,
  `scale-test`.
- `docs/` — `DESIGN-rsm.md` (live) + `history/` (superseded designs).
- `legacy/` — pre-reshape crates, kept only until wiped; do NOT build against them.

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
- **Current work + resume point:** `HANDOFF.md`, then `docs/DESIGN-rsm.md`.
- **Project state, decisions, preferences:** the memory system — the `MEMORY.md` index
  auto-loads; the files under `memory/*.md` are read on demand.
- **Fleet email** (this project's identity = `mesh-dev@colinrozzi.com`): quick-ref in
  memory `fleet-mail-setup`; the canonical guide is `/home/colin/work/actors/inbox/CLAUDE.md`
  (agent guide + the "Compatriots" roster) with `inbox/README.md` for the system.
  Outward mail needs Colin's direct go; comms may be **on hold** — check
  `hold-fleet-comms-until-landed`.
