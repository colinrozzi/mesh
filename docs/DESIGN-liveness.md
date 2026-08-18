# Mesh node self-watchdog (fail loud on wedges)

mesh-dev's slice of prod supervision, per Colin's re-scope: **make a silent wedge a LOUD
crash inside the node**, so the existing crash-supervision path catches it and sentinel stays
loud-deaths-only. No external liveness signal, no cross-team polling — each node owns its own
liveness and fails loud. Design-now; executes with the chat/weft persistent-prod stand-up.

## The problem it closes

A silent wedge (the smtp-acceptor class): process healthy, logic stuck, nothing crashes,
found only when it stops answering. The fix: the node **detects its own no-progress and
panics**, converting the silent wedge into a normal actor crash that theater + sentinel's
crash-supervision already handle.

## The one constraint that shapes the design: the node is single-threaded WASM

The mail's ideal — "a watchdog that runs INDEPENDENT of the stuck loop and panics" — does not
fully hold inside a WASM guest. A theater actor is **single-threaded + event-driven**: it runs
only inside a handler call (`tick` / `on-bytes` / `author`), and nothing else in the guest
runs while a handler is executing. So there are two wedge classes, and the in-node watchdog
only covers one:

- **SOFT wedge — logic stalls, handlers still RETURN** (frontier stops advancing, WANT never
  satisfied, store poisoned, deps never resolve). The heartbeat (`tick`) still fires. **This
  is the smtp-acceptor class, and the in-node pet-or-panic catches it fully.**
- **HARD wedge — a handler infinite-loops / deadlocks, the thread is STUCK.** No in-guest code
  can fire to check-and-panic (single-threaded). This needs a **host-level** watchdog —
  **already covered:** theater's **epoch trap** interrupts a runaway handler → supervised
  crash (confirmed by theater-dev). No mesh work, no new theater work.

So both classes are covered: mesh delivers the SOFT-wedge fail-loud (the named failure class);
the HARD-loop case is already caught by theater's epoch trap. Stating the split matters — a
"self-watchdog" that silently can't catch a deadlock would be worse than one whose scope is
stated, and here the deadlock case has a real owner (theater), not a silent gap.

## The mechanism: pet-or-panic in the heartbeat

Each `tick`, the node checks a set of **progress invariants** against its `NodeState`
counters; if any is violated for **K consecutive ticks**, it `panic!()`s — theater traps it
as an actor crash → loud death → crash-supervision restarts it. The "pet" is normal progress;
the "panic" is sustained no-progress. Cheap (counters, no DAG fold).

### Pet-or-panic invariants (each with a K-tick tolerance to avoid false crashes)

| invariant | wedge it catches | why K-tick tolerance |
|---|---|---|
| frontier/event-count advances **while peers>0 and WANT outstanding** | gossip/apply loop stalled with work pending | idle/isolated is legitimately no-progress — gate on peers+outstanding-work, not raw stillness |
| WANT set is **bounded** (not monotonically growing over K ticks) | can't fetch deps → stuck forever | transient WANT churn is normal |
| pending/orphan buffer is **bounded** | ancestry never completing | short-lived orphans are normal |
| store reads/writes **succeed** (not a persistent error) | poisoned store | one transient error isn't fatal |

**Conservative by default:** a false panic = an unnecessary restart, so K and the thresholds
skew toward "only crash on a clear, sustained stall." The node OWNS these knobs (self-liveness
policy lives in the node, not sentinel).

## Where it lives

- The check runs in the node's `tick` path (the heartbeat the system already drives via the
  timer's `handle-tick` → `node.tick`). It needs a couple of cheap progress markers in
  `NodeState`: `last_progress_tick`, a monotonic `tick_seq`, and last-seen WANT/pending sizes.
- On violation: `panic!()` in the node (theater surfaces the trap as a crash). No I/O needed —
  the crash IS the signal.
- **Boundary:** the node self-detects + self-crashes; sentinel does nothing new (its
  crash-supervision already restarts a crashed actor). Zero cross-team coupling — the whole
  point of the re-scope.

## Tickets (execute WITH the chat/weft prod stand-up, not now)

1. **node: progress markers** — `tick_seq`, `last_progress_tick`, last WANT/pending sizes in
   `NodeState`.
2. **node: pet-or-panic in `tick`** — evaluate the invariants; `panic!()` on K-consecutive
   violation. Conservative defaults; the knobs are node config.
3. **Tuning + a test** — a wedge-injection test (force a stall) asserting the panic fires, and
   an idle/isolated test asserting it does NOT (no false crash).
4. **HARD infinite-loops — already handled by theater** (epoch trap → supervised crash;
   confirmed by theater-dev). No mesh ticket, no new theater work — noted here so the split is
   documented and the deadlock case has a stated owner.

## Sequencing

Design-locked now so it isn't rediscovered at 2am. Executes with the first persistent prod
node (chat/weft). Until then the node is unchanged. Supersedes the earlier external
liveness-signal spec (dropped per the re-scope).
