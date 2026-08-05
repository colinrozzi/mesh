//! `counter-system` — the reference RSM **executor** (the "system").
//!
//! A theater actor that supervises its *own* counter node (a child in its
//! supervision tree) and drives it, demonstrating the two-surface model:
//!   - **TAKE ACTION / ASK QUESTIONS → theater RPC.** `author(payload)` and the read
//!     verbs (`current-state`, `event-status`) are synchronous RPC calls to the node
//!     by actor-id. No TCP, no wire, no signing — the node owns its identity.
//!   - **WATCH WHAT HAPPENS → the message-server stream.** After `subscribe(self)`,
//!     the node pushes each finalized dag-node to `handle-send`; that is the live
//!     event feed the executor's read model is built from.
//!
//! Flow: `init` learns our id (`runtime.self`), registers for the stream, spawns the
//! node child, and arms a timer (spawn returns before the child is routable). The
//! first tick subscribes, authors a burst of increments (and one that must be
//! REJECTED, proving validation), reads back `current-state` over RPC, and logs a
//! one-line verdict an integration test greps for. `handle-send` counts the finalized
//! events arriving over the stream.

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
            // `self` is declared as an #[import] binding only (WIT keyword).
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
        // packr delivers `params: tuple<list<u8>>` POSITIONALLY → fn(state, msg).
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
pub struct SysState {
    pub my_id: String,
    pub node_id: String,
    pub node_manifest: String,
    /// Finalized events seen over the message-server stream.
    pub stream_events: u64,
    /// False until the first tick has driven the node.
    pub armed: bool,
}

#[derive(serde::Deserialize)]
struct SysConfig {
    node_manifest: String,
    node_seed: String,
    node_listen: String,
}

/// The counter SM's state shape, so we can read the count back from `current-state`.
#[derive(serde::Deserialize, Default)]
struct CounterView {
    count: i64,
    ops: u64,
}

// ---- RPC helpers ----

/// `none` for the RPC call options (default timeout).
fn no_options() -> Value {
    Value::Option { inner_type: ValueType::Bool, value: None }
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::String(s) => s,
        o => format!("{:?}", o),
    }
}

/// Strip ONE result layer (theater wraps returns as `result<value, string>`, encoded
/// as either `Value::Result` or a `result`-typed `Value::Variant`). A non-result
/// value passes through.
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

/// Call an RPC verb and return the node's `ret` value. Two result layers are peeled:
/// the RPC transport result, then the node's `result<(state, ret)>` export wrapper.
fn node_rpc(node_id: &str, func: &str, params: Value) -> Result<Value, String> {
    let out = rpc_call(node_id.to_string(), func.to_string(), params, no_options());
    unwrap_result(unwrap_result(out)?)
}

/// `author(payload) -> hash` over RPC. The node carries success/rejection in-band as
/// `tuple<ok, data>` (data = hash, or the SM reason on a rejection).
fn author(node_id: &str, cmd: &Cmd) -> Result<Vec<u8>, String> {
    let payload = Value::from(counter_protocol::encode(cmd));
    let ret = node_rpc(node_id, "my:mesh.author", payload)?;
    match ret {
        Value::Tuple(items) if items.len() == 2 => {
            let mut it = items.into_iter();
            let ok = matches!(it.next(), Some(Value::Bool(true)));
            let data = Vec::<u8>::try_from(it.next().unwrap())
                .map_err(|e| format!("author data decode: {:?}", e))?;
            if ok {
                Ok(data)
            } else {
                Err(String::from_utf8_lossy(&data).into_owned())
            }
        }
        other => Err(format!("author: unexpected ret {:?}", other)),
    }
}

/// `current-state() -> bytes` over RPC. A no-arg verb still needs a non-empty param
/// (an empty tuple does not reach the guest as `tuple<state>`); an empty byte list
/// flattens to `tuple<state, list>` cleanly and the node ignores it.
fn current_state(node_id: &str) -> Result<Vec<u8>, String> {
    let ret = node_rpc(node_id, "my:mesh.current-state", Value::from(Vec::<u8>::new()))?;
    Vec::<u8>::try_from(ret).map_err(|e| format!("current-state decode: {:?}", e))
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SysState, ()), String> {
    let cfg: SysConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse system config: {}", e))?
        }
        _ => return Err("missing system config".to_string()),
    };
    log("[counter-system] init".to_string());

    let my_id = runtime_self();
    if let Err(e) = message_server_register() {
        log(format!("[counter-system] register failed: {}", e));
    }

    // Spawn our counter node child with a mesh InitConfig (seed + listen; no peers).
    let node_init = format!(
        r#"{{"node_seed":"{}","listen_addr":"{}"}}"#,
        cfg.node_seed, cfg.node_listen
    );
    let node_id = supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(node_init)), None)
        .map_err(|e| format!("spawn node: {}", e))?;
    log(format!("[counter-system] spawned node {}", node_id));

    // Defer driving to the first tick — the child isn't routable until spawn returns.
    if let Err(e) = timer_set_interval("drive".to_string(), 2500) {
        log(format!("[counter-system] set-interval failed: {}", e));
    }

    Ok((
        SysState {
            my_id,
            node_id,
            node_manifest: cfg.node_manifest,
            stream_events: 0,
            armed: false,
        },
        (),
    ))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SysState, _timer: String) -> Result<(SysState, ()), String> {
    if state.armed {
        return Ok((state, ()));
    }
    let node = state.node_id.clone();

    // WATCH: subscribe self to the finalized stream (pushed to handle-send).
    match node_rpc(&node, "my:mesh.subscribe", Value::String(state.my_id.clone())) {
        Ok(_) => log("[counter-system] subscribed to the finalized stream".to_string()),
        Err(e) => log(format!("[counter-system] subscribe failed: {}", e)),
    }

    // ACT: author a burst of increments (sum = 10) + one that MUST be rejected.
    let incs = [1i64, 2, 3, 4];
    let mut expected = 0i64;
    for n in incs {
        match author(&node, &Cmd::Inc(n)) {
            Ok(_) => expected += n,
            Err(e) => log(format!("[counter-system] author Inc({}) failed: {}", n, e)),
        }
    }
    // This one is invalid (non-positive) — the node must reject it at author time.
    match author(&node, &Cmd::Inc(-5)) {
        Ok(_) => log("[counter-system] BUG: Inc(-5) was accepted".to_string()),
        Err(e) => log(format!("[counter-system] Inc(-5) rejected as expected: {}", e)),
    }

    // ASK: read the folded state back over RPC and verify.
    match current_state(&node) {
        Ok(bytes) => {
            let view: CounterView = serde_json::from_slice(&bytes).unwrap_or_default();
            if view.count == expected {
                log(format!(
                    "[counter-system] COUNTER-SYSTEM OK: count={} ops={} (expected {})",
                    view.count, view.ops, expected
                ));
            } else {
                log(format!(
                    "[counter-system] COUNTER-SYSTEM FAIL: count={} expected {}",
                    view.count, expected
                ));
            }
        }
        Err(e) => log(format!("[counter-system] current-state failed: {}", e)),
    }

    Ok((SysState { armed: true, ..state }, ()))
}

/// A finalized dag-node arrived over the message-server stream. Tier 1 counts them
/// (proving push works); the count read-model itself comes from `current-state`.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: SysState, msg: Vec<u8>) -> Result<(SysState, ()), String> {
    let n = state.stream_events + 1;
    log(format!("[counter-system] STREAM finalized event #{} ({} bytes)", n, msg.len()));
    Ok((SysState { stream_events: n, ..state }, ()))
}
