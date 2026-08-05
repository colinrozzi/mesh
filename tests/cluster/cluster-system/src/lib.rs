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

use counter_protocol::Cmd;
use packr_guest::{export, import, pack_types, GraphValue, Value, ValueType};

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

#[derive(serde::Deserialize, Default)]
struct CounterView {
    count: i64,
}

// ---- RPC plumbing (same convention as the single-node executors) ----

fn no_options() -> Value {
    Value::Option { inner_type: ValueType::Bool, value: None }
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::String(s) => s,
        o => format!("{:?}", o),
    }
}

fn unwrap_result(v: Value) -> Result<Value, String> {
    match v {
        Value::Result { value: Ok(b), .. } => Ok(*b),
        Value::Result { value: Err(b), .. } => Err(value_to_string(*b)),
        Value::Variant { tag: 0, mut payload, .. } if !payload.is_empty() => Ok(payload.remove(0)),
        Value::Variant { tag: 1, payload, .. } => {
            Err(payload.into_iter().next().map(value_to_string).unwrap_or_else(|| "rpc error".to_string()))
        }
        other => Ok(other),
    }
}

fn node_rpc(node_id: &str, func: &str, params: Value) -> Result<Value, String> {
    unwrap_result(unwrap_result(rpc_call(node_id.to_string(), func.to_string(), params, no_options()))?)
}

fn author_inc(node_id: &str) -> Result<(), String> {
    let payload = Value::from(counter_protocol::encode(&Cmd::Inc(1)));
    let ret = node_rpc(node_id, "my:mesh.author", payload)?;
    match ret {
        Value::Tuple(items) if items.len() == 2 => {
            if matches!(items.into_iter().next(), Some(Value::Bool(true))) {
                Ok(())
            } else {
                Err("author rejected".to_string())
            }
        }
        other => Err(format!("author: unexpected ret {:?}", other)),
    }
}

fn subscribe(node_id: &str, my_id: &str) {
    let _ = node_rpc(node_id, "my:mesh.subscribe", Value::String(my_id.to_string()));
}

fn node_count(node_id: &str) -> Result<i64, String> {
    let ret = node_rpc(node_id, "my:mesh.current-state", Value::from(Vec::<u8>::new()))?;
    let bytes = Vec::<u8>::try_from(ret).map_err(|e| format!("{:?}", e))?;
    let view: CounterView = serde_json::from_slice(&bytes).unwrap_or_default();
    Ok(view.count)
}

/// Decode `(sm-event id, author, payload)` from a finalized dag-node frame.
/// `[len u32][kind 0x93][id 32][author 32][ts u64][ndeps u16][deps..][payload]`.
fn decode_finalized(frame: &[u8]) -> Option<([u8; 32], [u8; 32])> {
    if frame.len() < 79 || frame[4] != 0x93 {
        return None;
    }
    let id: [u8; 32] = frame[5..37].try_into().ok()?;
    let author: [u8; 32] = frame[37..69].try_into().ok()?;
    Some((id, author))
}

fn short(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(12);
    for &b in bytes.iter().take(6) {
        s.push_str(&format!("{:02x}", b));
    }
    s
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
            subscribe(node, &state.my_id);
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
    let Some((id, author)) = decode_finalized(&msg) else {
        return Ok((state, ()));
    };
    let n = state.finalizations + 1;
    log(format!("[cluster] finalize #{} — event {} authored by node {}", n, short(&id), short(&author)));
    Ok((ClusterState { finalizations: n, ..state }, ()))
}
