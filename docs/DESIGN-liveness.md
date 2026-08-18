# Mesh node liveness signal (for supervision)

mesh-dev's slice of prod mesh-node supervision: **what a node exposes so a supervisor can
detect a SILENT WEDGE** (process alive, logic stuck, no crash — the smtp-acceptor failure
class). sentinel owns the poll cadence, the wedge classification, and the restart decision;
mesh owns exposing a cheap, honest health signal. This is **design-now**; it executes with
the chat/weft persistent-prod stand-up (no persistent mesh service exists yet).

## The problem it closes

Crash-supervision catches LOUD deaths (the actor exits, theater/systemd sees it). The
dangerous case is the SILENT wedge: the process is healthy, the tick/gossip loop is stuck,
nothing crashes, and it's found only when the node stops answering. Active probing is the
net-new — not more crash-catching.

## The signal: a cheap `health` query — NO DAG fold

A node serves `health()` returning a small fixed struct, computed from the counters in
`NodeState` (JSON blob sizes + flags) — **not** a `current_state`-style fold. Two liveness
layers fall out of one query:

- **Coarse (does it answer?):** the health query returning at all proves the actor + its
  message-server/RPC surface are alive. A timeout is itself a wedge signal.
- **Fine (is the LOGIC progressing?):** the returned counters, sampled across ticks.

### Fields

| field | source | what it catches | cost |
|---|---|---|---|
| `ready: bool` | `ready_sent` | node finished init and is serving | O(1) |
| `tick_seq: u64` | **net-new** monotonic counter, bumped every `tick` | **THE crux** — the heartbeat loop is *running*, not just the process alive. Frozen `tick_seq` while the query still answers = logic-wedged tick loop → restart | O(1) |
| `event_count: u64` | DAG size | DAG is ingesting/growing (gossip alive) | cheap |
| `finalized_count: u64` | finality map | finality is progressing | cheap |
| `self_head: hash` | `self_head_hex` | this node is still authoring on its own chain | O(1) |
| `pending_count: u64` | orphan buffer size | runaway missing-deps storm (can't complete ancestry) | cheap |
| `connections: u64` | connections map | isolation (0 peers) vs a real wedge — lets sentinel *not* false-positive an idle-but-isolated node | cheap |

`event_count`/`finalized_count`/`pending_count` are blob sizes; if parsing them per poll ever
costs, memoize them as maintained counters in `NodeState` (O(1)). Start with the parse.

### The one pricier field — membership

sentinel also wants "still in `consensus_members`." That needs the `members` projection (a
fold — already served as `Session::members()`, see the SM contract). Keep it OUT of the hot
`health()` struct; sentinel polls `members()` at a LOWER cadence and checks `self ∈ members`.
`health()` stays O(1); membership is a separate, cheaper-than-full-state read.

### Why `tick_seq` is the crux

The node is I/O-free — the SYSTEM drives it: the timer's `handle-tick` → `node.tick`. Bumping
`tick_seq` on each `tick` means: if the timer wedges, or the system stops driving, or
`node.tick` hangs, `tick_seq` FREEZES while the process stays up. That is exactly the silent
wedge, made observable with one counter. `event_count` frozen alone is ambiguous (could be
idle/isolated — that's why `connections` is in the struct); `tick_seq` frozen is not — the
heartbeat itself stopped.

## Interface

- Node: a `health` export (like `current-state`, but from counters — no fold) + maintain
  `tick_seq`.
- mesh-system: `my:mesh.health` RPC (mirrors `my:mesh.current-state`), and a `tick_seq` bump
  in its `handle-tick` path.
- SDK: `Session::health() -> Result<Health, String>` (a GraphValue struct, the fields above).
- **Boundary:** mesh EXPOSES the signal; sentinel OWNS the poll interval, the
  frozen-across-N-ticks threshold, the isolation-vs-wedge classification, and restart +
  context-capture. No policy in the node.

## Tickets (execute WITH the chat/weft prod stand-up, not now)

1. **node: `tick_seq` + `health` export** — maintain a monotonic tick counter; a `health`
   export returning the struct from `NodeState` counters (no fold).
2. **mesh-system: `my:mesh.health` RPC** + bump `tick_seq` in `handle-tick`.
3. **mesh-client: `Session::health()`** — the GraphValue `Health` struct + one RPC call.
4. **Doc the contract** — which fields sentinel reads, and the "answer-timeout = wedge,
   frozen `tick_seq` = wedge, frozen `event_count` + peers>0 + expected-activity = suspect"
   decision inputs (sentinel owns the actual policy).

## Sequencing

Design is locked here so it isn't rediscovered at 2am during an incident. Execution ships
with the first persistent prod node (chat/weft). Until then: nothing in the node changes.
