# The state-machine contract (node ↔ SM)

**The single source of truth for building an SM on mesh.** If you are writing `chat-sm`,
`control-sm`, `fs-sm` (Weft), or any network's SM + executor, build against *this*. It is
grounded in the shipped code — the worked examples are `tests/networks/{counter,echo,bank}`
(in-tree) and `hello-system/` (a full external repo). When this doc and a stale ref
disagree, this doc + the code win.

Status: packr **0.16** canonical. Node generic over payload `p` and state `s`. Verified
against `src/lib.rs`, `state-machine.pact`, `mesh-client/src/lib.rs`, and the composed,
running examples.

---

## 1. The interface

An SM **exports** the `state-machine` interface (Interface 1); the node **imports** it and
calls it on the fold hot path. Four functions:

```
initial-state: func() -> s
validate:      func(id: list<u8>, author: list<u8>, timestamp: u64, payload: p, state: s) -> result<bool, string>
apply:         func(id: list<u8>, author: list<u8>, timestamp: u64, payload: p, state: s) -> s
members:       func(state: s) -> list<list<u8>>
```

- `p` = the SM's **typed payload** (its authored event kinds).
- `s` = the SM's **state**. Either a **typed** value (a GraphValue record — see `bank`'s
  `BankState { wallets: map<string,s64> }`) or **opaque bytes** `list<u8>` the SM serializes
  itself (serde — see `chat`/`control`/`echo`). Both are first-class; pick per network.
- `validate` → `Ok(true)` admits, `Err(reason)` rejects (reason is a human string surfaced
  as a `conflict`). `apply` runs **only** on events that passed `validate`, and **returns
  the next state** (there is no `&mut` — state crosses the component boundary by value).

## 2. How the node is generic over `p` and `s`

Not a Rust type param on `mesh-runtime`, not a Rust associated type. It is a **packr
interface-level generic**. The node's `pack_types!` declares (`src/lib.rs:111`):

```rust
pack_types! {
    type s: serializable
    type p: serializable
    // … imports state-machine with validate(…, payload: p, state: s) -> …
}
```

On the node's Rust side these arrive as `packr_guest::Value` (dynamic). At **compose time**,
packr **structurally unifies** `p`/`s` with the SM's concrete exported types and generates
the marshaling, so the SM's Rust code receives a **real typed value**. A signature/shape
mismatch fails at compose with a hash error — never at runtime.

**Consequence for you:** there is *no shared Rust type to coordinate*. Each SM names its own
payload/state type in its own `pack_types!` (e.g. `payload: chat-event`, `payload: fs-op`);
the node's `p` binds to whatever you export. Name it whatever you want.

## 3. Defining `p` (and typed `s`) — the derive

Two supported ways, both producing the same Graph value:

- **A protocol crate** — a hand-written enum/record with
  `#[derive(GraphValue)]` and `#[graph(crate = "packr_guest::composite_abi")]`. This is the
  right tool for **rich multi-field variants** (they stay ergonomic). See
  `tests/networks/echo/echo-protocol/src/lib.rs`, `.../bank/bank-protocol/src/lib.rs`.
- **`wit!(from "x.wit")`** — declare the schema in a `.wit`, generate the Rust type. Best for
  **simple** shapes. See `tests/networks/counter/counter.wit`, and the evaporated
  `chat.wit`/`control.wit`. Note: a WIT variant case carries **one** payload, so a multi-field
  kind becomes a case wrapping a `record` (`Msg::Command(CommandBody{..})`) — which is exactly
  what the derive emits for a struct-variant, so it round-trips identically.

**The derive is `GraphValue`.** There is no `Encode`/`Decode` derive anywhere in packr
(`packr-derive-*/src/lib.rs:` `#[proc_macro_derive(GraphValue, attributes(graph))]`; re-exported
`packr_guest::GraphValue`). The `GraphValue` macro path **and its `#[graph(...)]` field
attributes are identical across 0.15 ↔ 0.16** — the only 0.15→0.16 changes are additive
(`map<K,V>` in 0.15, `wit!(from …)` in 0.16). So use `GraphValue` regardless of which packr
version a given crate is pinned to.

**packr version:** **0.16** is canonical for the extraction. `chat-sm`/`control-sm` require it
(`wit!(from)`); the rest are on 0.15 and compose fine against it (ABI-compatible). New impl
repos: pin **0.16**.

## 4. What the SM sees — event context

`validate`/`apply` receive, per event:

- `id: list<u8>` — the 32-byte **event hash** (stable identity: journal keys, dedup, OR-Set
  tags. counter/echo use it as the message id; chat uses it as the OR-Set tag).
- `author: list<u8>` — the 32-byte **signer pubkey** (membership/authz gate).
- `timestamp: u64` — the author's wall-clock ms (display / LWW only; never trust for order).
- `payload: p` — the typed event.
- `state: s` — **the state folded over this event's ANCESTRY** — i.e. *exactly what the author
  had observed*, and nothing concurrent.

There is **no separate `deps` argument.** "What the author observed" **is** the `state` you're
handed: the node folds the SM forward along the event's causal ancestry and passes you the
result. Membership checks, balance checks, corr-id dedup, OR-Set observed-remove — all read
from that ancestry-relative `state` (see `chat-sm` remove-observes-ancestry, `bank-sm` balance,
`control-sm` corr-id journal).

## 5. Application model — order, determinism, conflicts

- The DAG is a **partial order**. The node folds it with a deterministic tiebreak so **every
  replica converges byte-for-byte** on the same state (proven: `confluence`, `scale`,
  `double-spend`). `apply` is a pure `(state, event) -> state`.
- It is **not** a single global total order you can lean on for cross-event uniqueness.
  Concurrent events are each validated against *their own* ancestry, so two events that are
  each individually valid can **both finalize** and violate an invariant on merge — the
  **conflict frontier** (`bank` double-spend: two transfers from one wallet both land →
  negative balance; the network is *consistent* but the invariant is broken).
- **Design implication (Weft, read this):** a monotonic `next_ino` counter in state is
  **conflict-prone** — two concurrent `create`s fold to the *same* ino on the frontier, exactly
  like the double-spend. Make identity **content/author-derived** (e.g. ino = hash(event.id) or
  `(author, seq)` Dot), not a shared counter, if you want concurrent creates to commute.
  Admission-final finality is consistent, **not** mutually-exclusive; witness/quorum finality
  for conflict-prone SMs is deferred (see `double-spend`).

## 6. State custody & membership

- **The SM owns its state's bytes.** State crosses the boundary as *your* type — a typed
  GraphValue `s`, or `list<u8>` you (de)serialize (serde is fine; chat/control/echo do this).
  `apply` returns the next state; the node persists the opaque bytes and re-hands them next
  fold. The node never inspects state.
- **Membership lives in the SM**, as authored event kinds — there is no node-core membership
  layer. `chat-sm` = OR-Set membership (Genesis/MemberAdd/MemberRemove); `control-sm` = a
  member set + allow-lists seeded by a Genesis event. One source of truth: your SM. So yes —
  `fs-sm` owns `Join`/`Depart`/`SetAccess` as its own Op kinds.

## 7. The SDK surface (mesh-client 0.2) — bytes, typed a layer up

`Session` is **byte-oriented** (`mesh-client/src/lib.rs`):

```rust
pub fn author(&self, payload: &[u8]) -> Result<[u8; 32], String>   // authored event id
pub fn current_state(&self) -> Result<Vec<u8>, String>
pub fn subscribe(&self, my_id: &str) -> Result<(), String>
// finalized stream → Event::Finalized(DagNode { id, author, payload: Vec<u8>, deps, … })
```

There is **no `author<T>` / `DagNode<T>`.** You type it by encoding/decoding with the **same
GraphValue payload type your SM uses** — `session.author(&chat_protocol::encode(&Msg::Text …))`,
and decode `DagNode.payload` the same way. (Any stale ref claiming a typed `author<T>`/`DagNode<T>`
surface is wrong.) A **typed public API** (`my:chat.post`, `my:fs.write`) only exists if you
build a **custom system** that exposes typed RPC verbs — `tests/networks/counter/counter-system`
is the worked example (it replaces the generic `mesh-system` as the entry). Otherwise you drive
the generic `mesh-system` surface with your encoded payloads.

## 8. Composition — building the runnable node

The node is generic; you compose your SM into it. Turnkey via the mesh flake:

```nix
# your impl repo's flake.nix
inputs.mesh.url = "…/mesh";
# …
packages.default = meshLib.mkComposite {
  name = "chat";
  sm   = "${meshLib.buildWasm { pname = "chat-sm"; src = ./.; crate = "chat-sm"; wasmName = "chat_sm.wasm"; }}/chat_sm.wasm";
};
```

`nix build` → `mesh_<name>.wasm` = `mesh-system` (generic entry, from mesh) ⊕ node (from mesh)
⊕ your SM. mesh supplies node + entry + packr + the fixed link graph; you supply only the SM.
Full worked example: **`hello-system/`** (an append-only-log SM in a standalone repo — read its
`flake.nix` + `README.md`, copy the shape). A custom typed system is optional and later.

---

## Quick answers to the open threads

- **Derive?** `#[derive(GraphValue)]` + `#[graph(crate = "packr_guest::composite_abi")]`. Not
  `Encode`/`Decode` (doesn't exist). Identical 0.15↔0.16.
- **Node generic mechanism?** packr interface generics `type p/s: serializable`, bound at
  compose by structural unification. Name your own payload/state type; nothing shared to align.
- **Typed in/out apply?** Yes — SM gets typed `p` + `s`, returns `s`. Not Value-decode-internally.
- **Event ctx / deps?** `(id, author, timestamp, payload, state)`. No deps arg — `state` *is* the
  ancestry-folded observed state.
- **State custody?** SM owns it; `apply` returns next state; node persists opaque bytes.
- **Membership?** SM-owned authored kinds. No node-core layer.
- **Order/`next_ino`?** Deterministic convergent fold, but a partial order — shared counters are
  a conflict frontier. Derive identity from `event.id`/`(author,seq)`, not a counter.
- **SDK typed?** No — bytes; type with your GraphValue schema. Typed verbs = custom system.
- **packr 0.15 or 0.16?** 0.16 canonical; pin it in new repos.
