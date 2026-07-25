# mesh

[![CI](https://github.com/colinrozzi/mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/colinrozzi/mesh/actions/workflows/ci.yml)

A theater-native **substrate for replicated state machines**. Each node is an
ed25519 keypair maintaining a self-rooted log of signed events; the network
agrees on a finalized, canonically-ordered event stream. Membership is the one
state machine the substrate computes itself; applications consume the finalized
stream and build whatever they need on top. **Message-passing** — broadcasting
opaque payloads to the fleet — is the first thing built on it.

`DESIGN.md` is the design rationale (the *why*); this is the *what* and *how*.

## Why

A token-list IAM model assumes a small, slowly-changing population of
identities. The agent-fleet pattern — where agents spawn agents, reviewers are
ephemeral, and the population doubles every few weeks — breaks that assumption.
There's no admin who can hand out and revoke tokens at fleet scale.

Mesh replaces the directory-of-tokens with a directory-of-signatures:

- **Identity is a public key.** You don't get issued an identity; you generate
  one. You *are* your private key.
- **Everything is signed.** Every event carries a signature; every action has
  provable origin.
- **State is derived, not asserted.** Membership and application state are pure
  functions of the signed event graph — recomputable, not trusted.

## Conceptual model

Two tiers, and the substrate barely knows what runs on top.

**Substrate** — identities, per-node logs, dissemination + catch-up, the
canonical order, and finality. It treats event payloads as **opaque bytes**.

**Application** — consumes the finalized, ordered stream. It can just read and
write it (message-passing broadcasts opaque payloads), or fold it into its own
replicated state (a shared task board, capability grants) — that folding is the
app's business. Membership is the one state machine the *substrate* folds
itself, because finality can't be computed without the member set.

Each node keeps a linear, **self-rooted log**: every event names that author's
previous event (`self_parent`), tracing back to the author's genesis. Events
also carry `refs` — foreign heads the author has seen. An event with `refs`
simultaneously (a) extends its own log and (b) witnesses those other histories.
There is no separate "witness" op — **grafting a foreign head IS witnessing it**,
and the same mechanic drives dissemination and catch-up sync.

## Events

```
Event {
    author:      PubKey,          // 32 bytes — ed25519 verifying key (a member)
    self_parent: Option<Hash>,    // author's previous event; None for a genesis
    refs:        Vec<Hash>,       // foreign heads grafted (witness + propagation)
    payload:     Vec<u8>,         // opaque to the substrate; empty = pure graft
    system:      Option<SystemOp>,// membership op (Introduce/Depart), or None
    signature:   Sig,             // 64 bytes — ed25519 over the encoding above
}
```

The back-edges of the DAG are `self_parent ∪ refs`; ancestry is reachability
over them. An **empty payload** is a pure graft / heartbeat / witness — the same
primitive as an application event, distinguished only by carrying no payload.

## Membership

Membership is **objective and dynamic** — every node computes the same member
set. It starts from a configured genesis set and evolves at runtime: a member
authors an `Introduce{node}` (admit) or `Depart{self}` (leave), carried in the
event's `system` field. The substrate folds these over the finalized log to
derive the live set (the one thing it interprets in an event); everything else
in `payload` is the application's business. A crash *without* a departure halts
progress — mesh is CP, with no fault tolerance (see `DESIGN.md`).

There are no roles. The substrate knows only **member nodes** (consensus
participants). Application identities — agents, recipients, addresses — live in
the *application* layer, expressed in payloads.

## Finality

An event is **finalized** when every member has authored an event that
transitively sees it (i.e. has grafted its lineage). Witnesses are just events;
finality is `members ⊆ {authors of events that see E}` — a local computation
over the DAG, no voting round.

Finality requires **all** members. mesh is a coordination channel for a fixed,
present set (think a multi-party TCP connection, not an always-available store):
if a member is down, the network **intentionally stops committing** until it
returns. Consistency over availability, by design.

## Message-passing

The substrate is **payload-agnostic**: a message is opaque bytes. When a
payload-bearing event finalizes, the node **broadcasts** it to every connected
client via a `NOTIFY` frame (`from[32] || payload`), committed and exactly-once.
Every client sees every committed payload — the whole log is replicated on every
node, so this is redundancy, not waste. Addressing, message-types, and routing
all live *in the payload*, interpreted by the application.

## Using mesh from an actor (composition)

An actor talks to a mesh node through the **mesh-client package**
(`mesh-client-pkg`) — a composable packr component that exports the `mesh`
interface (`submit`/`introduce`/`depart`/`register`/`delivery`/`node-config`) and
owns the `message-server-host` I/O, plus an opt-in `mesh-control` interface (the
Command/Response/Lifecycle envelope carried *inside* a Submit payload). Rather
than vendoring the protocol, an actor `packr compose`s the prebuilt
`mesh_client_pkg.wasm` into itself; client↔consumer version skew is caught at
compose time by hash-checked links. Consumer guide: `mesh-client-pkg/CONSUMER.md`.

**Distribution:** one GitHub release ships a compatible set built from the same
source — `mesh.wasm` (the node), `mesh_client_pkg.wasm` (the client), and the
`mesh.pact` / `mesh-control.pact` interface specs. One version pin = a compatible
node+client pair.

## Wire protocol

All TCP. Length-prefixed binary frames: `LEN(u32 BE) || KIND(u8) || PAYLOAD`.

### Handshake (membership checked against config)

```
client → HELLO(pubkey[32])                          [0x01]
mesh   → CHALLENGE(nonce[32]) | REJECTED(reason)    [0x80 | 0x82]
client → AUTH(sig[64])                              [0x02]
mesh   → ACCEPTED(frontier)   | REJECTED            [0x81 | 0x82]
```

`ACCEPTED` carries the node's frontier (its current heads) so the dialer can
catch up immediately.

### Post-handshake

| Direction | Kind | Name | Payload |
|---|---|---|---|
| both | `0x10` | DELIVER | one encoded `Event` (gossip / backfill response) |
| both | `0x20` | FRONTIER | hash-list — "here are my heads" |
| both | `0x21` | WANT | hash-list — "send me these events" |
| client → mesh | `0x11` | SUBMIT | payload bytes — "author this message for me" |
| client → mesh | `0x12` | INTRODUCE | pubkey[32] — "admit this node as a member" |
| client → mesh | `0x13` | DEPART | (empty) — "author my departure" |
| mesh → client | `0x91` | ACK | event_hash[32] + ok_byte + utf8 err |
| mesh → client | `0x92` | NOTIFY | from[32] + payload — committed payload broadcast |

On a newly-seen event a node forwards it, backfills missing ancestry via `WANT`,
and (for payload or membership events) authors a graft to witness it. Delivery
happens on finality — payloads are broadcast to connected clients.

### Event encoding (canonical, hand-rolled)

```
author:       32 bytes
self_parent:  1 tag byte (0=none, 1=present) + 32 bytes iff present
refs:         u16 count (BE) + count * 32 bytes
payload:      u32 len (BE) + len bytes
system:       1 tag byte (0=none, 1=Introduce, 2=Depart) + node[32] iff 1|2
signature:    64 bytes (ed25519 over sha256 of all the above)
```

Deterministic and byte-stable across machines and language ports.

## Configuration

`manifest.toml`'s `initial_state` is JSON:

```jsonc
{
  // REQUIRED — seed material for this node's signing key
  "node_seed": "...",

  // OPTIONAL — the genesis member set (hex pubkeys), identical on every node.
  // Defaults to just this node if omitted. A node whose key is NOT in this set
  // is a *joining* node, admitted later via an Introduce event. Membership
  // evolves from here at runtime.
  "members": ["<64 hex chars>", ...],

  // OPTIONAL — peers to outbound-connect to on init
  "dial": [{"pubkey": "<hex>", "address": "127.0.0.1:9448"}],

  // OPTIONAL — listen address (default "127.0.0.1:9447")
  "listen_addr": "127.0.0.1:9447"
}
```

## Running

### Build

```sh
cargo build --release --target wasm32-unknown-unknown
```

### Unit tests

```sh
cargo test
```

Host-side tests (the crate is `#![cfg_attr(not(test), no_std)]`). Cover the event
encoding, DAG + finality + membership, and persistence.

### Single-node smoke

```sh
theater spawn manifest.toml          # binds 127.0.0.1:9447
cd smoke && cargo build --release && ./target/release/mesh-smoke
```

A client authenticates as the member node, SUBMITs a message, and observes it
delivered (NOTIFY) after it finalizes (immediate with one member).

### Multi-node integration test

```sh
cd multi-node-test && cargo build --release && ./target/release/mesh-multi-node-test
```

Spawns two member nodes (9447, 9448); B dials A. A message submitted to A
propagates to B, finalizes across both, and is delivered to a client on B.

### Dynamic-membership test

```sh
cd membership-test && cargo build --release && ./target/release/mesh-membership-test
```

Bootstrap nodes A + B admit a third node N at runtime, finalize a message
delivered to N, then N departs cleanly and A + B carry on without it.

## File layout

```
mesh/
├── Cargo.toml          # wasm crate
├── manifest.toml       # theater actor manifest (single-node default)
├── README.md           # you are here
├── DESIGN.md           # design rationale
├── src/
│   ├── lib.rs          # actor: init, handshake, gossip, committed delivery
│   ├── event.rs        # event type, canonical encoding, signing
│   ├── dag.rs          # DAG storage, finality, canonical order
│   ├── conn.rs         # per-connection handshake state
│   ├── codec.rs        # ActorState persistence + hex helpers
│   └── wire.rs         # frame protocol
├── mesh-api/           # envelope + control codecs (shared by node + client)
├── mesh-client/        # host-agnostic client library (source-dep form)
├── mesh-client-pkg/    # the composable mesh-client PACKAGE (mesh + mesh-control)
├── example-app/        # example actor that supervises a node + uses mesh-client
├── compose-smoke/      # local runtime proof: compose sentinel-stub + mesh-client
├── testkit/            # shared integration-test client + spawn harness
├── smoke/              # single-node end-to-end test
├── multi-node-test/    # two-node integration test
├── membership-test/    # dynamic introduce/depart test
├── join-test/          # self-serve join (join_allow + auto-introduce)
└── evict-test/         # quorum eviction of a crashed member
```

## Status

**Working:** self-rooted logs; multi-parent DAG with forks admitted; **dynamic
membership** (runtime introduce/depart); grafting as unified dissemination /
witnessing / catch-up; all-members finality; broadcast delivery of committed
payloads; **pruning/compaction with snapshot transfer** (bounded storage, and a
behind node bootstraps from a checkpoint); single- and multi-node operation.

**App interface** (`DESIGN.md` → *App interface*): a co-located app actor drives
its own node over theater's `message-server` — `request` commands + a
`send`-callback delivery stream (the `mesh-api` envelope). Node side implemented
+ verified; `mesh-example-app` + `app-test` are the app half. End-to-end is gated
on two theater primitives (`runtime.get-self`; parent→spawned-child
message-server addressability).

**Composition** (`mesh-client-pkg/`): mesh-client ships as a composable packr
0.12 component — an actor `packr compose`s it in instead of vendoring the
protocol (see *Using mesh from an actor*). Built + proven end-to-end
(`compose-smoke`); the mesh interface + the opt-in `mesh-control` envelope.

**Released — v0.3 (ephemeral membership; see `DESIGN-ephemeral-membership.md`
+ `DESIGN-retention.md`):** signed event timestamps; **self-serve join**
(`join_allow` + auto-introduce, so a node joins without an out-of-band introduce —
`join-test`); a **heartbeat pump** (members author noop events so the frontier keeps
advancing → per-member liveness + continuous compaction); a **sync-ready** signal
(the node tells the app when it's admitted+synced); **quorum eviction** (a stale
member is voted out by a majority — 2f+1 — and at N=2 the node shuts down instead;
`evict-test`); and **retention** that retains net membership history + trims
payloads, with no checkpoint on the wire (a peer answers a `WANT` for a pruned hash
with a `SEALED` marker derived from its own history). A breaking event/delivery
format change (signed timestamps) → a v0.3 node interoperates only with v0.3 peers.

**Near-term** (see `DESIGN.md`): incremental finality/delivery (currently
re-scans the retained DAG each callback), batched emission (currently
emit-on-event).

**Deferred:** fault tolerance (quorum finality, eviction of a crashed member,
partition recovery), key rotation, Byzantine fault tolerance.
