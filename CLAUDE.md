# CLAUDE.md — mesh

Theater-native (WASM, `#![no_std]`) substrate: a gossip-replicated Merkle-DAG event
log. Being reshaped into a **replicated state machine** (a dumb core + a
consumer-provided state machine) — see `DESIGN-rsm.md` for the locked design and
`HANDOFF.md` for where the build is right now.

## Build & test
- **Build (wasm):** `cargo build --release --target wasm32-unknown-unknown`
  The crate is `#![no_std]` for wasm — a host build pulls in std and conflicts with the
  guest panic handler, so **always use the wasm target** for build + clippy.
- **Unit tests (host):** `cargo test --lib` (std is available under `cfg(test)`).
- **Clippy:** `cargo clippy --target wasm32-unknown-unknown -- -D warnings` (CI treats
  warnings as errors).
- **Adjacent crates** are standalone (not a workspace) — build/test with
  `--manifest-path <dir>/Cargo.toml`: `example-app`, `mesh-client-pkg`, `mesh-api`,
  `mesh-client`, `sm-trivial`, `sm-smoke`, and the `*-test` clients.
- **Integration tests self-spawn theater** (`/home/colin/work/theater/target/release/theater`):
  e.g. `cd multi-node-test && cargo build --release && ./target/release/mesh-multi-node-test`
  (also `join-test`, `evict-test`, `membership-test`). **`pkill -x theater` between runs —
  never `pkill -f`.** These do network + process spawning → run with
  `dangerouslyDisableSandbox: true`. `smoke` needs a node already running.

## Composition (packr)
Components compose via **packr**: `.pact` interface files + `packr compose` + one
`[[link]]` per function. Toolchain: `/nix/store/…packr-0.12.2/bin/{pact,packr}`.
`mesh-client-pkg/CONSUMER.md` is the reference pattern; the guest side is
`packr_guest::pack_types!` + `#[import_from]`.

## VCS
jj (jujutsu) colocated with git. Commit or push **only when asked**, and **never push
without Colin's direct in-terminal go** (pushing/releasing is outward-facing). End
commit messages with the `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
trailer.

## State, work, and comms — pointers (read on demand)
- **Current work + resume point:** `HANDOFF.md`, then `DESIGN-rsm.md`.
- **Project state, decisions, preferences:** the memory system — the `MEMORY.md` index
  auto-loads; the files under `memory/*.md` are read on demand.
- **Fleet email** (this project's identity = `mesh-dev@colinrozzi.com`): quick-ref in
  memory `fleet-mail-setup`; the canonical guide is `/home/colin/work/actors/inbox/CLAUDE.md`
  (agent guide + the "Compatriots" roster) with `inbox/README.md` for the system.
  Outward mail needs Colin's direct go; comms may be **on hold** — check
  `hold-fleet-comms-until-landed`.
