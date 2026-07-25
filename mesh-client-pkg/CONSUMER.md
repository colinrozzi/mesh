# Composing the mesh client into your actor

The mesh client ships as a **compiled packr component** — you don't build it, you
compose your actor with its prebuilt `.wasm`. This is the composition model: the
mesh interface is authored once and reused across actors, no vendored `mod mesh`.

## 1. Get the artifacts (one mesh release)

From the `mesh vX.Y.Z` GitHub release, download:

- **`mesh_client_pkg.wasm`** — the client component you compose into your actor.
- **`mesh.wasm`** — the mesh **node** (the substrate actor) you supervise as a
  child. Built from the same source/version, so node↔client are compatible.
- **`mesh.pact`** — the interface signature (reproduced below).

## 2. Declare the `mesh` interface in your actor

You must declare the **complete** `mesh` interface — the hash-checked link covers
every binding, so a partial declaration is rejected at compose time. You only need
`#[import_from]` bindings for the functions you actually call.

```rust
packr_guest::pack_types! {
    imports {
        mesh {
            submit: func(node: string, payload: list<u8>) -> result<list<u8>, string>,
            introduce: func(node: string, member: list<u8>) -> result<list<u8>, string>,
            depart: func(node: string) -> result<list<u8>, string>,
            register: func(node: string, app-id: string) -> result<bool, string>,
            delivery: func(msg: list<u8>) -> option<tuple<list<u8>, list<u8>>>,
            node-config: func(seed: string, listen: string, members: list<string>, dial: list<tuple<string, string>>) -> string,
        }
    }
    exports { /* your actor's own exports */ }
}
```

## 3. Bind the functions you call

With **packr-guest 0.12.1+**, `#[import_from]` decodes returns via `FromValue`, so
`Result`/`Option` returns bind directly — no shim:

```rust
use packr_guest::import_from;

#[import_from("mesh", name = "submit")]
fn mesh_submit(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String>;

#[import_from("mesh", name = "delivery")]
fn mesh_delivery(msg: Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)>;
```

(On packr-guest ≤ 0.12.0 a `Result`/`Option` return needed a one-line
`TryFrom<Value>` newtype bridge to the `FromValue` impl; 0.12.1 removed that need.)

## 4. Compose

```toml
# your-actor.compose.toml
[[component]]
name  = "my-actor"
wasm  = "my_actor.wasm"
entry = true

[[component]]
name = "mesh-client"
wasm = "mesh_client_pkg.wasm"

# One [[link]] per mesh function you call. message-server-host is NOT linked —
# the mesh-client package owns it; it stays residual for theater to supply.
[[link]]
consumer = "my-actor"
import   = "mesh.submit"
provider = "mesh-client"
export   = "submit"
```

```sh
packr compose your-actor.compose.toml -o my_actor_composed.wasm
```

- **`wasm` paths are relative to the MANIFEST file's directory, not your cwd.**
- **`export` is the provider's interface function name verbatim** — the hyphenated
  name from `mesh.pact` (e.g. `export = "node-config"`, `export = "decode-command"`),
  not an underscored Rust identifier.

Theater loads `my_actor_composed.wasm` like any actor. A skewed client (built
against a different `mesh` signature) is rejected at compose time with a clear
hash-mismatch error, not a runtime failed-to-convert.

## 4b. (Optional) the control plane — `mesh-control`

The same package exports a second, opt-in interface, `mesh-control`: the app-level
Command / Response / Lifecycle envelope carried *inside* a Submit payload (see
`mesh-control.pact`). Declare the complete interface, bind what you use, and add a
`[[link]]` per function (`import = "mesh-control.decode-command"`, `export =
"decode-command"`, …). `encode-*` build a payload you then `submit`; `control-kind`
+ `decode-*` parse a payload you got from `delivery` (`decode-*` return
`option<tuple<...>>`, which binds directly on 0.12.1+). Typical loop:

```rust
if let Some((from, body)) = mesh_delivery(msg) {
    match mesh_control_kind(&body) {
        Some(1) => { let (corr, target, cmd, args) = mesh_decode_command(&body)?; /* handle */ }
        Some(2) => { /* decode_response */ }
        Some(3) => { /* decode_lifecycle */ }
        _ => {}
    }
}
```

## 5. Run a node

Supervise `mesh.wasm` as a child, handing it an InitConfig built by `node-config`.
See the top-level README / `example-app` for the spawn + register + submit flow.
