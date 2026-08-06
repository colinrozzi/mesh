//! `counter-system` — the reference RSM **executor** (the "system").
//!
//! A theater actor that supervises its *own* counter node and drives it through the
//! **`mesh-client` SDK** (`Session`), demonstrating the two-surface model:
//!   - **TAKE ACTION / ASK QUESTIONS → theater RPC** (`session.author`, `.current_state`).
//!   - **WATCH WHAT HAPPENS → the message-server stream** (`Session::decode_event` in
//!     `handle-send`, after `session.subscribe(self)`).
//!
//! Note how little is left: the executor declares its host bindings + actor interface
//! and writes app logic; every RPC/stream quirk lives in the SDK. Flow: `init` learns
//! our id, registers, spawns the node child, arms a timer; the first tick subscribes,
//! authors a burst of increments (+ one that must be REJECTED), reads back
//! `current-state`, and logs a verdict; `handle-send` counts finalized events.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use counter_protocol::Cmd;
use mesh_client::{Event, Session};
use packr_guest::{decode, export, import, pack_types, GraphValue, Value};

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
/// counter-sm now uses a TYPED state, so `current-state` returns the Graph-ABI
/// structural encoding — decode it via `GraphValue`, not JSON.
#[derive(Default, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
struct CounterView {
    count: i64,
    ops: u64,
}

/// Our node, driven through the SDK.
fn session(node_id: &str) -> Session {
    Session::new(node_id.to_string(), rpc_call)
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
        SysState { my_id, node_id, node_manifest: cfg.node_manifest, stream_events: 0, armed: false },
        (),
    ))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SysState, _timer: String) -> Result<(SysState, ()), String> {
    if state.armed {
        return Ok((state, ()));
    }
    let s = session(&state.node_id);

    // WATCH: subscribe self to the finalized stream (pushed to handle-send).
    match s.subscribe(&state.my_id) {
        Ok(()) => log("[counter-system] subscribed to the finalized stream".to_string()),
        Err(e) => log(format!("[counter-system] subscribe failed: {}", e)),
    }

    // ACT: author a burst of increments (sum = 10) + one that MUST be rejected.
    let mut expected = 0i64;
    for n in [1i64, 2, 3, 4] {
        match s.author(&counter_protocol::encode(&Cmd::Inc(n))) {
            Ok(_) => expected += n,
            Err(e) => log(format!("[counter-system] author Inc({}) failed: {}", n, e)),
        }
    }
    match s.author(&counter_protocol::encode(&Cmd::Inc(-5))) {
        Ok(_) => log("[counter-system] BUG: Inc(-5) was accepted".to_string()),
        Err(e) => log(format!("[counter-system] Inc(-5) rejected as expected: {}", e)),
    }

    // ASK: read the folded state back over RPC and verify.
    match s.current_state() {
        Ok(bytes) => {
            let view = decode(&bytes)
                .ok()
                .and_then(|v| CounterView::try_from(v).ok())
                .unwrap_or_default();
            if view.count == expected {
                log(format!(
                    "[counter-system] COUNTER-SYSTEM OK: count={} ops={} (expected {})",
                    view.count, view.ops, expected
                ));
            } else {
                log(format!("[counter-system] COUNTER-SYSTEM FAIL: count={} expected {}", view.count, expected));
            }
        }
        Err(e) => log(format!("[counter-system] current-state failed: {}", e)),
    }

    Ok((SysState { armed: true, ..state }, ()))
}

/// A finalized dag-node arrived over the stream — decoded by the SDK.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: SysState, msg: Vec<u8>) -> Result<(SysState, ()), String> {
    match Session::decode_event(&msg) {
        Some(Event::Finalized(node)) => {
            let n = state.stream_events + 1;
            log(format!("[counter-system] STREAM finalized event #{} ({} payload bytes)", n, node.payload.len()));
            Ok((SysState { stream_events: n, ..state }, ()))
        }
        _ => Ok((state, ())),
    }
}
