# Composing mesh components (packr)

How mesh pieces are assembled from `.wasm` components with **packr**. Two uses today:
composing a **node ⊕ SM** into a runnable node artifact (live — every `*-compose.toml`),
and packaging a **client/SDK as a composable component** consumers compose (the v1
pattern, distilled here for whenever mesh-client-v2 wants it). Distilled from the
retired `mesh-client-pkg/CONSUMER.md` + what building the RSM node surface taught us.

## The model
A component **exports** an interface; a consumer **imports** it and links against the
prebuilt `.wasm` — no vendored `mod`, the interface is authored once and reused.
Mechanism: a `.pact` interface file + `packr compose` + **one `[[link]]` per
function**. Guest side is `packr_guest::pack_types!` with `#[import_from]` (to call an
imported interface) / `#[export]` (to provide one).

- **Node ⊕ SM:** the SM *exports* `state-machine` (initial-state/validate/apply), the
  node *imports* it. See `state-machine.pact` and `examples/counter/counter-compose.toml`
  (the simplest end-to-end reference).
- **Client/SDK as a component:** the client component *exports* a `mesh`-style
  interface and **owns the residual host I/O** (e.g. `message-server-host` is NOT
  linked — the component keeps it, theater supplies it), so a consumer composes the
  client to get typed calls without touching the transport.

## Use the FIXED packr
Compose with `/home/colin/work/pack/target/release/packr` (== published **0.12.7**),
**not** the 0.12.2 nix binary — the old one has an LTO import-strip bug that produces
artifacts which *compose* fine but *fail to load* in theater (`No handler provides
interface …`). `--output` is required on the newer packr:

```sh
/home/colin/work/pack/target/release/packr compose <compose.toml> --output <out.wasm>
```

## Gotchas (each cost a debug cycle at some point)
- **`wasm` paths are relative to the MANIFEST file's directory, not your cwd.** (So an
  example compose in `examples/counter/` reaches the node at `../../target/…`.)
- **`export` is the provider's interface function name VERBATIM** — the hyphenated name
  from the `.pact` (`export = "node-config"`), never the underscored Rust identifier.
- **Declare the COMPLETE interface.** The hash-checked link covers every binding, so a
  partial interface declaration is rejected *at compose time*. You only need
  `#[import_from]` bindings for the functions you actually call.
- **Skew is caught at compose time**, not runtime: a component built against a different
  interface signature fails with a clear hash-mismatch, not a failed-to-convert later.
- **Custom exports survive composition.** A packr-composed entry component keeps its own
  non-lifecycle exports (verified: the node's `my:mesh.*` RPC exports internalize + pass
  `wasm-tools validate`) — so a node can be both a composed SM host and an RPC callee.
- **packr-guest 0.12.1+**: `#[import_from]` decodes returns via `FromValue`, so
  `Result`/`Option` returns bind directly (older versions needed a `TryFrom<Value>`
  newtype shim).

## Release artifact model
Ship, from one versioned release, the set that must be built from the same source:
the **component `.wasm`**, the **node `.wasm`**, and the **`.pact`** interface. Same
version ⇒ node ↔ client compatible; the compose-time hash check enforces it.

## Note for mesh-client-v2
The v1 client shipped as a composable *component* because it owned the message-server
transport. v2's surface is different — the executor calls theater **RPC** (author +
read verbs) and receives the **message-server stream** directly — so v2 may instead be
a plain Rust **library** (rlib) of helpers (`author`, `await_finalized`, the dag-node
stream decoder — see `examples/echo/echo-system` for the inline versions) that an
executor links normally, with no composition step. If v2 *does* ship as a component,
the pattern above applies unchanged.
