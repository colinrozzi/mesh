# mesh

[![CI](https://github.com/colinrozzi/mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/colinrozzi/mesh/actions/workflows/ci.yml)

A theater-native **substrate for replicated state machines**. Each node is an
ed25519 keypair maintaining a self-rooted log of signed events; the network
agrees on a finalized, canonically-ordered event stream; and a deterministic
reducer folds that stream into committed state. **Message-passing is the first
state machine built on it.**

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

**State machine** — a deterministic reducer fed the finalized, ordered event
stream. The network *is* a state machine; message-passing is the first one.

Each node keeps a linear, **self-rooted log**: every event names that author's
previous event (`self_parent`), tracing back to the author's genesis. Events
also carry `refs` — foreign heads the author has seen. An event with `refs`
simultaneously (a) extends its own log and (b) witnesses those other histories.
There is no separate "witness" op — **grafting a foreign head IS witnessing it**,
and the same mechanic drives dissemination and catch-up sync.

## Events

```
Event {
    author:      PubKey,        // 32 bytes — ed25519 verifying key (a member)
    self_parent: Option<Hash>,  // author's previous event; None for a genesis
    refs:        Vec<Hash>,     // foreign heads grafted (witness + propagation)
    payload:     Vec<u8>,       // opaque to the substrate; empty = pure graft
    signature:   Sig,           // 64 bytes — ed25519 over the encoding above
}
```

The back-edges of the DAG are `self_parent ∪ refs`; ancestry is reachability
over them. An **empty payload** is a pure graft / heartbeat / witness — the same
primitive as an application event, distinguished only by carrying no payload.

## Membership

Membership is **objective and static** — every node is configured with the same
member-pubkey set. The substrate never derives membership from the log, so it
interprets *nothing* in events; payloads are entirely the reducer's business.

There are no roles. The substrate knows only **member nodes** (consensus
participants). Application identities — message recipients, agents, mailboxes —
live in the *reducer* layer, expressed in payloads.

## Finality

An event is **finalized** when every member has authored an event that
transitively sees it (i.e. has grafted its lineage). Witnesses are just events;
finality is `members ⊆ {authors of events that see E}` — a local computation
over the DAG, no voting round.

Finality requires **all** members. mesh is a coordination channel for a fixed,
present set (think a multi-party TCP connection, not an always-available store):
if a member is down, the network **intentionally stops committing** until it
returns. Consistency over availability, by design.

## Message-passing (the first reducer)

A message payload is `recipient[32] || body`. The reducer folds finalized
messages into per-recipient inboxes. When a message finalizes, the node delivers
it to connected clients via a `NOTIFY` frame (committed, exactly-once delivery).
Empty and malformed payloads are deterministically ignored.

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
| client → mesh | `0x11` | SUBMIT | payload bytes — "author this for me" |
| mesh → client | `0x91` | ACK | event_hash[32] + ok_byte + utf8 err |
| mesh → client | `0x92` | NOTIFY | from[32] + body — committed message delivery |

On a newly-seen event a node forwards it, backfills missing ancestry via `WANT`,
and (for payload events) authors a graft to witness it. Delivery happens on
finality, via the reducer.

### Event encoding (canonical, hand-rolled)

```
author:       32 bytes
self_parent:  1 tag byte (0=none, 1=present) + 32 bytes iff present
refs:         u16 count (BE) + count * 32 bytes
payload:      u32 len (BE) + len bytes
signature:    64 bytes (ed25519 over sha256 of all the above)
```

Deterministic and byte-stable across machines and language ports.

## Configuration

`manifest.toml`'s `initial_state` is JSON:

```jsonc
{
  // REQUIRED — seed material for this node's signing key
  "node_seed": "...",

  // OPTIONAL — other member pubkeys (hex). These ∪ this node = the member set.
  "members": ["<64 hex chars>", ...],

  // OPTIONAL — peers to outbound-connect to on init (a subset of members)
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
encoding, DAG + finality, the reducer, and persistence.

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
│   ├── reducer.rs      # the reducer seam (fold finalized stream → state)
│   ├── message.rs      # message-passing reducer (first app)
│   ├── conn.rs         # per-connection handshake state
│   ├── codec.rs        # ActorState persistence + hex helpers
│   └── wire.rs         # frame protocol
├── smoke/              # single-node end-to-end test
└── multi-node-test/    # two-node integration test
```

## Status

**Working:** self-rooted logs; multi-parent DAG with forks admitted; static
membership; grafting as unified dissemination / witnessing / catch-up; all-
members finality; the reducer seam + message-passing with committed delivery;
single- and multi-node operation.

**Near-term** (see `DESIGN.md`): pruning/compaction (the DAG grows unbounded),
incremental finality + reducer (currently re-folds from genesis each callback),
batched emission (currently emit-on-event).

**Deferred:** dynamic membership, eviction / liveness-on-disconnect, the
introduction problem, key rotation, Byzantine fault tolerance.
