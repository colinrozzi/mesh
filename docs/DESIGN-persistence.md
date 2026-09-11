# Mesh node on-disk persistence (cold-start recovery)

Durable local log so a node can **cold-start with no live peer** — restart on a rebooted host
(or a fresh machine) and re-project its state from local disk, instead of the current
gossip-only model (durable only while ≥1 replica stays up; full-fleet-down = total loss).
Colin greenlit persistence + stable-ids as the priority after set-nick; this is the substrate
half.

## Current reality (why a manifest change alone does nothing)

Verified in code: the node holds its DAG as **theater actor-state** — opaque bytes the system
carries across handler calls. It has **zero** `theater:simple/store` imports, and
`actor.init` calls `node_init(config)` **fresh** (re-authors genesis, empty DAG) on every
(re)start. So:

- Adding a `store` handler + data-dir to the node manifest changes **nothing** — the node
  never writes to a store. (Answers the deploy-side question: it's not a config toggle.)
- On restart the node re-syncs via gossip (needs a peer). Full-down = loss.
- **Self-chain hazard, not just replicas:** a fresh `init` re-authors genesis → a NEW
  `self_head` each boot. A node doesn't just lose the room; it loses its own chain identity.
  This is why persistence + stable-ids are the same work item.

So cold-start durability is a **substrate build** (this doc), not a manifest change. chat-dev's
always-on keeper-node is the correct interim mitigation until it lands.

## Design — the system persists, the node stays pure

The node is I/O-free (a non-entry component can't call host imports), so the **system**
(`mesh-system`, the entry) owns the store I/O — same split as tcp/timer/message-server. The
node's state is already opaque bytes the system holds; the system just needs to durably keep
them and rehydrate on boot.

**Write path:** after a handler that MUTATES node state (`author`, `on-bytes`/`tick` that
grew the DAG or moved finality), `mesh-system` writes the node's opaque bytes to the store.

**Cold-start path:** `mesh-system.init` first reads the store; if persisted node bytes exist,
it **rehydrates** the node from them (a `node.resume(bytes)` entry — no fresh genesis, keeps
`self_head` + the full DAG + finality); only if the store is empty does it fall back to
`node_init(config)` (true first boot). Recovery = the node re-folds `current_state` from its
reloaded DAG, exactly as it does live — no new projection logic.

**What to persist (v0):** the whole node-state blob (admitted DAG + finality + self_head +
delivered). It's what `NodeState` already serializes; simplest correct thing. Cost: rewriting
the blob per mutation grows with the log. **v1 optimization** (not v0): an append-only
finalized-event log + periodic snapshot, so writes are O(delta) not O(log). Ship the simple
blob first, measure, then optimize.

## Interface / tickets (greenlit; manager coordinates fleet sequencing)

1. **node: `resume(bytes) -> state`** — rehydrate from persisted node bytes (validate/repair,
   don't re-author genesis). `init` stays the first-boot path.
2. **mesh-system: store I/O** — import `theater:simple/store` (get/put); read-then-rehydrate in
   `actor.init`; write the node blob after mutating handlers.
3. **manifest: add the `store` handler + a durable data-dir** to the node standup (the
   deploy-side change the manager makes — but only meaningful once 1+2 land).
4. **cold-start test** — author on a single node with NO peers, kill theater, restart from the
   same data-dir, assert `current_state` + `self_head` survive (the exact gap chat-dev named).
5. **v1 (later): incremental log + snapshot** if the full-blob write cost bites at room scale.

## Boundary + pairing

Pairs with **stable-ids**: persisting `self_head` is what makes a node's identity stable across
restarts (no re-genesis), which stable-ids builds on. Persistence is the substrate mechanism;
the manager owns wiring the store handler + data-dir into the standup and sequencing the fleet
impact. Cold-start durability = 1+2+3 together; the keeper-node bridges until then.

## Constraint: packr first-class map/set vs a folded-state snapshot

The coming packr wire break (first-class `Value::Map`/`Value::Set`, new node kinds, **no
dual-read** — see memory `mapset-wire-break`) does **not** touch v0 or the v1 log: the persist
target is the `NodeState` JSON blob (admitted DAG + finality + self_head + delivered), which is
`list<>`/opaque-payload-bytes + serde_json framing — zero packr map/set, so it re-decodes across
the break untouched. `resume` re-**folds** SM state from those persisted events; it never reads a
persisted folded state.

The one exposure to guard: **never cache the folded SM state on disk.** A folded state (e.g.
chat's `ChatState`) is full of map/set, so a cached-folded-state snapshot would be a packr-encoded
map/set blob subject to the break (old-format snapshots stop decoding). This also re-introduces the
certified-checkpoint complexity v0.4 deliberately shed (full retention). Both push the same way:
the persist unit is **events**, and resume **re-folds**. If a folded-state checkpoint is ever added
as a cold-start fold-cost optimization, it MUST be packr-format-versioned and land after/with
first-class map/set — not before. (Flagged by chat-dev 2026-08-29.)
