# mesh

A theater-native distributed identity + message-passing actor. Each node
is an ed25519 keypair; every event is a signed node in a content-addressed
DAG; state (the network's membership) is derived by walking the DAG and
applying state-mutating ops in canonical order. Consensus on state
changes is reached when all current Nodes have witnessed.

## Why

A token-list IAM model assumes a small, slowly-changing population of
identities. The agent-fleet pattern — where agents spawn agents, reviewers
are ephemeral, and the population doubles every few weeks — breaks that
assumption. There's no admin who can hand out and revoke tokens at fleet
scale.

Mesh replaces the directory-of-tokens with a directory-of-signatures:

- **Identity is a public key.** You don't get issued an identity; you
  generate one. You ARE your private key.
- **Membership is an event.** A signed event from an existing Node adds
  someone. Another signed event removes them. No admin step, no central
  registry write.
- **Attribution is automatic.** Every event carries a signature; every
  action has provable origin.
- **Drift is impossible.** The connection's verified pubkey IS the
  identity for the lifetime of the connection. You can't drift from
  yourself.

## Conceptual model

Two layers:

**Dissemination DAG** — the physical thing. Every event of every kind
(NodeIntroduce, MailboxCreate, Revoke, Send, Witness) lives here. Each
event references a single `parent` event hash. Multi-author, can have
concurrent events. This is what propagates across the network and what
witnesses attest to.

**State DAG** — a derived view from the dissemination DAG. Filter for
state-changing events (NodeIntroduce/MailboxCreate/Revoke) that have
reached consensus, order them canonically, and you get a strict linear
chain. *This* determines the applied state of the network (the member
set).

Sends and Witnesses live only in the dissemination DAG — they propagate
and are cryptographically attestable, but they don't enter the state DAG
because they don't change state. Only the three state-mutating ops
extend the state-chain (after reaching consensus).

## Member kinds

Two roles, distinguished by what they may sign:

| Role | May author | Counts toward consensus? |
|---|---|---|
| **Node** | Anything (NodeIntroduce, MailboxCreate, Revoke, Send, Witness) | Yes — Witnesses from Nodes are the consensus votes |
| **Mailbox** | Send only | No — Mailboxes are addressable identities, not consensus participants |

A Node is a long-lived consensus participant (a mesh instance, or any
entity that should witness). A Mailbox is just an addressable endpoint
with credentials — an agent process, a CLI session, anyone who needs to
send and receive but doesn't need to maintain the network.

Mailboxes can be created and revoked freely without affecting the
network's consensus topology. Nodes joining/leaving is a heavier event
(they change who the consensus participants are).

## Events

```
Event {
    parent:    EventHash,    // 32 bytes — the event we extend from
    author:    PubKey,       // 32 bytes — ed25519 verifying key
    op:        Op,
    signature: Sig,          // 64 bytes — ed25519 over signing_hash
}

Op = NodeIntroduce { subject, name }
   | MailboxCreate { subject, name }
   | Revoke        { subject }
   | Send          { recipient, payload }
   | Witness       { also_cite: Vec<EventHash> }
```

Genesis is implicit. The root Node is admitted at the start of the world,
and the first event's `parent` is the `GENESIS_PARENT` sentinel (all
zeros).

## Validation rules

For each ingested event, the actor checks:

1. **Signature** verifies against `author`.
2. **Parent exists** in the DAG (or is GENESIS_PARENT). If not, the event
   is buffered pending parent.
3. **Author is a member** in the state derived at parent. The genesis
   special case: only root may author the first event off `GENESIS_PARENT`.
4. **Op-specific rules**:
   - NodeIntroduce / MailboxCreate / Revoke: author must be a **Node**;
     subject existence rules apply.
   - Send: recipient must be a member.
   - Witness: author must be a Node.

## Consensus + finality

An event `E` is **finalized** iff the set of distinct Node-authors of
Witnesses transitively referencing `E` includes every Node in the
consensus state at `E.parent`.

"Transitively referencing" means via:
- Parent chain: any descendant of `E` saw `E`
- `Witness.also_cite`: a Witness that cites `E` (or transitively cites
  something that does)

Consensus is **sequential** for state changes. At any moment one
state-changing event extends the state-DAG head. When it reaches
finality, the next state-changing event from any Node's queue may
extend. (Concurrent siblings get tie-broken by lowest event hash; the
losing proposal gets re-anchored by its author.)

Sends and Witnesses don't bottleneck on consensus — they just propagate
through the dissemination DAG.

## Single-node case

When mesh runs as the only Node (or with itself as the sole consensus
participant), every state-mutating event submitted by clients triggers an
**inline witness** signed by mesh's own keypair. Consensus is reached
before `on_data` returns — a Mailbox created milliseconds ago can
authenticate immediately on its next connection.

For Sends, the periodic timer-driven Witness fires every
`witness_interval_ms` (default 2000) covering events received since the
last tick.

## Multi-node case

Two or more mesh instances form a network by:

1. Agreeing on `root_pubkey` (the same on every node's init config)
2. Pre-admitting each other's pubkeys via `peer_node_pubkeys` (also
   identical across the network — derives identical genesis state on
   every node)
3. One side opens an outbound connection to the other via `peer_meshes`,
   handshakes from the client side, and joins the broker's connection
   table

After connection, events flow bidirectionally — submitted events on one
mesh are broadcast as DELIVERED frames to all authed connections
including the peer mesh, which ingests and re-broadcasts to its own
connections. Sequential consensus on state changes still applies: a
state-mutating event from any node needs witnesses from every current
Node before finalization.

## Wire protocol

All TCP. Length-prefixed binary frames: `LEN(u32 BE) || KIND(u8) || PAYLOAD`.

### Handshake

```
client → HELLO(pubkey)                                    [0x01]
mesh   → CHALLENGE(nonce[32])  | REJECTED(utf8 reason)    [0x80 | 0x82]
client → AUTH(sig[64])                                    [0x02]
mesh   → ACCEPTED(state_head[32])  | REJECTED             [0x81 | 0x82]
```

After ACCEPTED, the connection's identity is bound to the pubkey from
HELLO for its lifetime.

### Post-handshake frames

| Direction | Kind | Name | Payload |
|---|---|---|---|
| client → mesh | `0x10` | SUBMIT | encoded `Event` |
| mesh → client | `0x90` | DELIVERED | encoded `Event` (broadcast of any ingested event) |
| mesh → client | `0x91` | ACK | event_hash[32] + ok_byte + utf8 err if !ok |
| mesh → client | `0x92` | STATE | state_head[32] (reserved; not yet emitted in v2) |

In multi-node mode, peer-mesh outbound connections post-handshake can
*also* receive DELIVERED frames from the peer (gossip), which the
receiving mesh ingests and re-broadcasts to its own authed connections.

### Event encoding (canonical)

```
parent:       32 bytes
author:       32 bytes
op_kind:      1 byte (0=NodeIntroduce, 1=MailboxCreate, 2=Revoke, 3=Send, 4=Witness)
op_payload:   variable, per kind:
    NodeIntroduce / MailboxCreate: subject[32] + u16(name.len) + name
    Revoke:                        subject[32]
    Send:                          recipient[32] + u32(payload.len) + payload
    Witness:                       u16(also_cite.len) + also_cite[N * 32]
signature:    64 bytes (ed25519 over sha256 of all the above)
```

Hand-rolled binary — deterministic, byte-stable across machines and
language ports.

## Configuration

`manifest.toml`'s `initial_state` is JSON:

```jsonc
{
  // REQUIRED — seed material for this node's signing key
  "node_seed": "...",

  // OPTIONAL — the network's root pubkey. If absent, this node's own
  // pubkey is root. For multi-node, all nodes share the same root_pubkey.
  "root_pubkey": "<64 hex chars>",

  // OPTIONAL — listen address (default "127.0.0.1:9447")
  "listen_addr": "127.0.0.1:9447",

  // OPTIONAL — additional Nodes pre-admitted at genesis. Must be
  // identical across all nodes in the network for genesis state to match.
  "peer_node_pubkeys": [
    {"pubkey": "<hex>", "name": "node-b"}
  ],

  // OPTIONAL — peer meshes to outbound-connect to on init
  "peer_meshes": [
    {"pubkey": "<hex>", "address": "127.0.0.1:9448"}
  ],

  // OPTIONAL — witness emission cadence (default 2000)
  "witness_interval_ms": 2000
}
```

## Running

### Build

```sh
cd /home/colin/work/actors/mesh
cargo build --release --target wasm32-unknown-unknown
```

### Single-node smoke

```sh
theater spawn manifest.toml          # binds 127.0.0.1:9447
cd smoke && cargo build --release
./target/release/mesh-smoke --pubkeys   # show test keys it'll use
./target/release/mesh-smoke             # run the scenario
```

Scenario: root authenticates, creates Mailbox(alice) + Mailbox(bob),
alice + bob authenticate, alice signs a Send to bob, bob receives the
DELIVERED frame.

### Multi-node integration test

```sh
cd multi-node-test && cargo build --release
./target/release/mesh-multi-node-test
```

Spawns two mesh instances on ports 9447 and 9448, each pre-admitting the
other as a Node and B opening an outbound connection to A. Verifies a
Send submitted to A propagates to a test client on B via the cross-mesh
peer connection.

## File layout

```
mesh/
├── Cargo.toml          # wasm crate
├── manifest.toml       # theater actor manifest (single-node default)
├── flake.nix           # nix build + dev shell
├── README.md           # you are here — current v2 spec
├── DESIGN.md           # the design conversation that led to v2
├── src/
│   ├── lib.rs          # actor entry + connection state machine
│   ├── event.rs        # event types, encoding, signing
│   ├── dag.rs          # DAG storage, state derivation, finality, consensus
│   ├── state.rs        # re-exports from dag.rs
│   └── wire.rs         # frame format
├── smoke/              # single-node end-to-end test
└── multi-node-test/    # two-mesh integration test
```

## What's done in v2

- ed25519 keypairs as identity; nonce-challenge handshake
- Full event DAG with parent chains
- Node and Mailbox roles with role-aware validation
- Inline witness emission for sequential consensus on state changes
- Periodic witness ticks for Send-event gossip
- Consensus state derivation (`is_finalized` + `consensus_state_head`)
- Single-node operation (mesh is root, self-witnesses)
- Multi-node operation (shared root, outbound peer connections, cross-mesh
  event propagation)

## What's not yet done

- **Real catch-up sync.** Two meshes joining without identical starting
  state would diverge — there's no HEAD-exchange/walk-back protocol yet.
  For now, all meshes must boot from matching genesis configs.
- **Liveness on Node disconnect.** If a Node disconnects mid-consensus,
  the network waits forever. Need a timeout-and-revoke mechanism.
- **Pruning.** Old events stay in the DAG forever. Subsumption-based
  pruning of older Witnesses is in the design but not implemented.
- **Concurrent non-conflicting state changes.** Currently strict
  sequential; multiple admits from different Nodes can't both progress
  in parallel.
- **Persistence to `theater:simple/store`.** The actor's DAG lives in
  serialized actor state, but no separate event log is persisted yet.

## Future direction

This is the substrate for the agent-fleet identity story. Once mesh is
deployed in production, downstream services (git-server, inbox, tickets,
deploy primitives) can authenticate via mesh-verified identity instead
of carrying their own bearer-token tables. Identity becomes a property
of the network, not a feature of every individual service.

The capability layer (Grant/Revoke per-capability events that ride the
same consensus) is the natural follow-on once the basic identity layer
is hardened.
