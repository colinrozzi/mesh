//! `cluster-system` — the tier-3 reference: a cluster ORCHESTRATOR + OBSERVER.
//!
//! One theater actor that stands up a whole mesh and watches it:
//!   - **spins up N nodes** — supervisor-spawns N counter nodes, each with a distinct
//!     identity + listen addr, wired (by the harness) to gossip in a line.
//!   - **drives a workload** — RPC-`author`s a burst of increments on every node, so
//!     events originate all across the cluster.
//!   - **observes the network** — subscribes to *every* node's finalized stream (so it
//!     sees finalizations network-wide) and polls `current-state` across all nodes each
//!     tick, logging a live view (per-node counts, finalizations seen, converged?).
//!
//! This is the executor pattern one rung up: not driving one node, but orchestrating a
//! cluster and tracking it through the same two surfaces — RPC for actions, the
//! message-server stream for events. The workload is `counter` (Inc commutes, so the
//! whole cluster converges on the sum — a trivially checkable network invariant).

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mesh_client::Session;
use packr_guest::{export, import, pack_types, GraphValue, Value};

/// Decode a typed count notification (a big-endian i64) — the watch stream's payload.
fn decode_count(b: &[u8]) -> Option<i64> {
    <[u8; 8]>::try_from(b).ok().map(i64::from_be_bytes)
}

packr_guest::setup_guest!();

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
        }
        theater:simple/timer {
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
        theater:simple/message-server-host {
            register: func() -> result<_, string>,
        }
        theater:simple/rpc {
            call: func(actor-id: string, function: string, params: value, options: value) -> value,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
        theater:simple/message-server-client.handle-send: func(state: actor-state, params: tuple<list<u8>>) -> result<actor-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
#[import(module = "theater:simple/runtime", name = "self")]
fn runtime_self() -> String;
#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(manifest: String, init_state: Option<Value>, wasm_bytes: Option<Vec<u8>>) -> Result<String, String>;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;
#[import(module = "theater:simple/rpc", name = "call")]
fn rpc_call(actor_id: String, function: String, params: Value, options: Value) -> Value;

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ClusterState {
    pub my_id: String,
    pub node_ids: Vec<String>,
    pub incrs_per_node: u64,
    pub expected: i64,
    /// Finalizations observed across the whole network (an event is seen once per node
    /// that finalizes it — so this counts network-wide finalization, not unique events).
    pub finalizations: u64,
    pub armed: bool,
    pub done: bool,
}

#[derive(serde::Deserialize)]
struct ClusterConfig {
    node_manifest: String,
    /// One mesh InitConfig JSON per node (seed / listen / dial) — the harness wires the
    /// gossip topology and hands it in, so this actor needs no crypto.
    node_inits: Vec<String>,
    incrs_per_node: u64,
}

// ---- driving each node through its TYPED counter interface (my:counter.*) ----

fn session(node_id: &str) -> Session {
    Session::new(node_id.to_string(), rpc_call)
}

fn author_inc(node_id: &str) -> Result<(), String> {
    session(node_id).call("my:counter.increment", Value::from(1i64)).map(|_| ())
}

fn node_count(node_id: &str) -> Result<i64, String> {
    let v = session(node_id).call("my:counter.count", Value::from(Vec::<u8>::new()))?;
    i64::try_from(v).map_err(|e| format!("count decode: {:?}", e))
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(ClusterState, ()), String> {
    let cfg: ClusterConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse cluster config: {}", e))?
        }
        _ => return Err("missing cluster config".to_string()),
    };
    let n = cfg.node_inits.len();
    log(format!("[cluster] init — standing up {} nodes", n));

    let my_id = runtime_self();
    if let Err(e) = message_server_register() {
        log(format!("[cluster] register failed: {}", e));
    }

    // Spin up the cluster: one node child per InitConfig (gossip wired by the harness).
    let mut node_ids = Vec::with_capacity(n);
    for init in &cfg.node_inits {
        match supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(init.clone())), None) {
            Ok(id) => node_ids.push(id),
            Err(e) => log(format!("[cluster] spawn node failed: {}", e)),
        }
    }
    log(format!("[cluster] spawned {} nodes", node_ids.len()));

    // Drive + observe from ticks — the children aren't routable until spawn returns and
    // gossip needs a moment to connect.
    if let Err(e) = timer_set_interval("cluster".to_string(), 2500) {
        log(format!("[cluster] set-interval failed: {}", e));
    }

    let expected = (node_ids.len() as i64) * (cfg.incrs_per_node as i64);
    Ok((
        ClusterState {
            my_id,
            node_ids,
            incrs_per_node: cfg.incrs_per_node,
            expected,
            finalizations: 0,
            armed: false,
            done: false,
        },
        (),
    ))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: ClusterState, _timer: String) -> Result<(ClusterState, ()), String> {
    if state.done {
        return Ok((state, ()));
    }

    if !state.armed {
        // Phase 1: subscribe to every node's stream, then drive the workload.
        for node in &state.node_ids {
            let _ = session(node).call("my:counter.watch", Value::String(state.my_id.clone()));
        }
        let mut authored = 0u64;
        for node in &state.node_ids {
            for _ in 0..state.incrs_per_node {
                if author_inc(node).is_ok() {
                    authored += 1;
                }
            }
        }
        log(format!(
            "[cluster] subscribed to {} nodes; drove {} increments (expected count {})",
            state.node_ids.len(),
            authored,
            state.expected
        ));
        return Ok((ClusterState { armed: true, ..state }, ()));
    }

    // Phase 2: poll the network view — per-node counts + convergence.
    let mut counts = Vec::with_capacity(state.node_ids.len());
    for node in &state.node_ids {
        counts.push(node_count(node).unwrap_or(-1));
    }
    let converged = counts.iter().all(|c| *c == state.expected);
    log(format!(
        "[cluster] NETWORK VIEW: {} nodes, counts={:?}, finalizations_seen={}, converged={}",
        state.node_ids.len(),
        counts,
        state.finalizations,
        converged
    ));
    if converged {
        log(format!(
            "[cluster] CLUSTER NETWORK CONVERGED: count={} on all {} nodes",
            state.expected,
            state.node_ids.len()
        ));
        return Ok((ClusterState { done: true, ..state }, ()));
    }
    Ok((state, ()))
}

/// A finalized dag-node arrived from one of the nodes — the network event feed.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: ClusterState, msg: Vec<u8>) -> Result<(ClusterState, ()), String> {
    let Some(c) = decode_count(&msg) else {
        return Ok((state, ()));
    };
    let n = state.finalizations + 1;
    log(format!("[cluster] count-update #{} — count={}", n, c));
    Ok((ClusterState { finalizations: n, ..state }, ()))
}
