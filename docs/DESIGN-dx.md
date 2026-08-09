# DESIGN-dx.md — goal architecture: one actor, three layers

Status: **goal architecture** (agreed 2026-08-08). Supersedes the earlier
executor-drives-node / two-actor sketch. Companion to `DESIGN-rsm.md` (the substrate).

## The shape

One deployed actor per network participant, built by packr-composing **three layers**.
Only the middle layer(s) are authored per-network; the core is generic and consumed.

```
one actor = <name>-system.wasm      (packr: system ⊕ core ⊕ SM)

  SYSTEM   — the entry actor + the SOLE I/O shell               (authored: behavior)
    · owns every theater handler: init / tcp on-data / timer tick / handle-send
    · owns every host import: tcp, timer, message-server, log
    · holds the network's BEHAVIOR (reactions) + the external edge (ingress/egress)
    · drives the core through the "node" interface
        │
        ▼  imports the `node` interface
  CORE     — a PURE network engine, no host I/O                 (generic: consumed)
    · DAG · fold · finality · the gossip protocol
    · ingest / tick / author RETURN (finalized events, outbound gossip) as DATA
        │
        ▼  imports the `state-machine` interface
  STATE MACHINE — PURE rules                                    (authored: rules)
    · validate / apply / members / initial-state
    · owns the payload wire types (GraphValue)
```

## Why these three — the two lines that define the split

- **Safety vs. liveness.** The **state machine** is the *rules* — what events are valid,
  how state folds, who's a member. Safety: nothing invalid ever enters the record. The
  **system** is the *behavior* — when to author, how to react. Liveness: something good
  eventually happens. Rules are enforced everywhere and deterministically; behavior is a
  participant's choice and may be best-effort (the SM backstops it).
- **Pure vs. effectful.** The **core and the SM are pure and composed** — no I/O,
  deterministic, replay-safe, testable as functions. The **system is the single impure
  shell** that touches the world. Everything deterministic lives inside the composite;
  everything effectful lives in the system.

Heuristic for "does this belong in the SM or the system?": *would every correct
participant take this action?* If yes → it's a rule/semantics → SM. If it's one
participant's policy → behavior → system.

## The two composed interfaces

- **core → SM: `state-machine` (Interface 1)** — unchanged from `DESIGN-rsm.md`:
  `initial-state` / `validate` / `apply` / `members`. Pure, on the fold hot path,
  composed synchronously; compose unifies the core's generic `s` := the SM's state.
- **system → core: the `node` interface (Interface 0)** — replaces today's theater-RPC
  verbs + message-server stream. All pure; the "stream" collapses into return values:
  - `init(config) -> node-state`
  - `author(payload) -> (event, outbound[])`
  - `ingest(peer, bytes) -> (finalized[], outbound[])`
  - `tick() -> (finalized[], outbound[])`
  - `current-state() -> state` · `event-status(id) -> status`

  `outbound` = "gossip to send to peer P"; the system performs the actual `tcp.send`.
  `finalized` = the newly-committed events the system reacts to.

## How a beat works

```
incoming gossip → system.handle-send → core.ingest(peer, bytes) → (finalized[], outbound[])
                                          → system sends outbound; reacts to finalized (maybe core.author)
timer tick     → core.tick()          → (finalized[], outbound[]) → system sends / reacts
external input → core.author(payload) → (event, outbound[])       → system sends
```

Reactions (e.g. echo's "on a finalized Request, author a Response") live in the
system: it watches the finalized events the core returns (the *permanent record*),
checks responsibility ("am I the one who should answer?") and idempotency ("does this
Request already have a Response?"), and authors via `core.author`. No frontier-vs-replay
gating problem — the system only ever sees committed events.

## What a consumer authors

Two units (both get generic scaffolding from the tool):

1. **The state machine** — the rules + wire types. A pure component composed into the
   core. `validate` / `apply` / `members` over a typed or bytes state.
2. **The system** — the behavior + external edge. Built on a **generic system
   framework** (the reborn `mesh-client`) that owns the theater lifecycle, gossip
   routing, connection management, config, and the drive loop. The network-specific part
   is just `react(finalized, state) -> actions` plus whatever external I/O it exposes.

Non-reactive networks (chat, bank, counter) have a near-empty `react` — the system is
almost entirely the generic framework.

## What retires

- the separate node actor and the `supervisor.spawn` parent→child dance;
- the theater-RPC verbs and the message-server finalized stream between system and node;
- the `mesh-client::Session` wire-quirk wrapping (double-wrapped results, in-band error
  tunneling, `no_arg`, hand-rolled stream-frame decoding) — all accidental complexity of
  the old actor boundary;
- the per-network compose recipe as an *authored* file (it becomes generated).

## The theater constraint that shapes this (verified)

Theater dispatches **every incoming I/O callback only to the entry component**, by export
name (`on-data`, `handle-tick`, `handle-send`, `init`) — a non-entry composed component
cannot receive its own callbacks. Host imports resolve through a shared linker (calling
*out* is less restricted), but *incoming* strictly hits the entry. Therefore the entry
must be the single I/O owner → the **system is the entry**, and the **core must be
I/O-free** (it emits outbound as data rather than owning TCP handlers). This constraint
is *why* the core goes fully pure — it's not just aesthetics.

## Migration scope (honest)

- **Meaty:** the core's networking moves from *owning TCP handlers* to *emitting
  send-intents as data*; connection ownership moves up into the system; the `node`
  interface is defined and the RPC/stream surface + `Session` retire.
- **Safe / untouched:** fold, finality, DAG, SM composition, the generics — the hard,
  valuable core.
- **Fallback** if the pure-core refactor is too much appetite now: keep the two-actor
  RPC path and absorb its glue into a `mesh-client` executor prelude. Less clean, far
  less work — a way to get most of the DX win without the rearchitecture.

## Open / deferred

- **`act` in the SM (autonomous networks).** We chose behavior-in-the-system. A pure
  `act(state, event) -> [event]` faculty in the SM — reactions as part of the network's
  *definition*, enacted by the runtime — remains a possible future for driverless /
  self-reacting networks. Not needed now.
- **Responsibility / dedup.** N replicas all see the same finalized event; each system
  checks "am I responsible" before authoring. Explicit responsibility tags vs.
  deterministic derived events — decide when we build the first multi-responder network.
- **Naming — resolved.** With behavior in the system, the middle layer is genuinely just
  a state machine again; the name stays.

## Motivation — the current-state audit (condensed)

Why bother: today a consumer authors five things (protocol, SM, executor, compose, test)
where three of them aren't real layers.

- **Executors are majority glue.** counter/echo/cluster systems: 196–232 LOC each, ~40%
  app, ~25% glue, ~18% ceremony — the `pack_types!` import wall (26 lines), `#[import]`
  stubs (12), `session()`, config-parse, and spawn/timer/subscribe are copy-pasted
  verbatim across all three (~64 lines of ceremony before any app logic).
- **SM interface is re-declared per SM** — the same `pack_types!` state-machine block +
  four thin delegating `#[export]` wrappers in every SM; only `list<u8>` vs. a typed
  state record varies.
- **Compose is 100% derivable** — every recipe is node-entry + one SM + the same links;
  only the SM name/path change. 4 of 5 still carry stale `/home/colin/work/pack` /
  0.12.x comments, and none link `state-machine.members` (a gap to resolve).
- **Test runners duplicate** `spawn()` verbatim ×3 plus ~240 lines of manifest-template
  boilerplate.

The goal architecture collapses this to: author the SM + the system (on a generic
framework), everything else generated or provided.
