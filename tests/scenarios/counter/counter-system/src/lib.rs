//! `counter-system` (scenario driver) — an actor that talks to a counter participant
//! purely through its DOMAIN interface, demonstrating the sealed typed API.
//!
//! It spawns a `mesh_counter` participant (counter-system ⊕ node ⊕ counter-SM) and drives
//! it with counter concepts only — no substrate verbs, no manual encoding:
//!   - `my:counter.watch(self)`   → receive typed count updates on `handle-send`
//!   - `my:counter.increment(n)`  → returns the resulting count
//!   - `my:counter.count()`       → the current value
//!
//! It never sees event-status, raw frames, or gossip. Flow: `init` spawns the node + arms
//! a timer; the first tick watches, authors a burst (+ one rejected `increment(-5)`), and
//! reads back `count`; `handle-send` decodes the typed count notifications.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mesh_client::Session;
use packr_guest::{export, import, pack_types, GraphValue, Value};

packr_guest::setup_guest!();

/// Decode a typed count notification (a big-endian i64) — the watch stream's payload.
fn decode_count(b: &[u8]) -> Option<i64> {
    <[u8; 8]>::try_from(b).ok().map(i64::from_be_bytes)
}

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
pub struct SysState {
    pub my_id: String,
    pub node_id: String,
    /// The last count seen over the `watch` stream.
    pub watched_count: i64,
    /// How many typed count notifications we've received.
    pub watch_events: u64,
    pub armed: bool,
}

#[derive(serde::Deserialize)]
struct SysConfig {
    node_manifest: String,
    node_seed: String,
    node_listen: String,
}

fn session(node_id: &str) -> Session {
    Session::new(node_id.to_string(), rpc_call)
}

/// Call a typed verb that returns an `i64` (increment / count).
fn call_i64(s: &Session, func: &str, params: Value) -> Result<i64, String> {
    s.call(func, params).and_then(|v| i64::try_from(v).map_err(|e| format!("{:?}", e)))
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SysState, ()), String> {
    let cfg: SysConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse system config: {}", e))?
        }
        _ => return Err("missing system config".to_string()),
    };
    log("[counter-driver] init".to_string());

    let my_id = runtime_self();
    if let Err(e) = message_server_register() {
        log(format!("[counter-driver] register failed: {}", e));
    }

    let node_init = format!(r#"{{"node_seed":"{}","listen_addr":"{}"}}"#, cfg.node_seed, cfg.node_listen);
    let node_id = supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(node_init)), None)
        .map_err(|e| format!("spawn node: {}", e))?;
    log(format!("[counter-driver] spawned counter participant {}", node_id));

    if let Err(e) = timer_set_interval("drive".to_string(), 2500) {
        log(format!("[counter-driver] set-interval failed: {}", e));
    }

    Ok((SysState { my_id, node_id, watched_count: 0, watch_events: 0, armed: false }, ()))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SysState, _timer: String) -> Result<(SysState, ()), String> {
    if state.armed {
        return Ok((state, ()));
    }
    let s = session(&state.node_id);

    // WATCH: ask to be told when the value changes (typed count updates on handle-send).
    match s.call("my:counter.watch", Value::String(state.my_id.clone())) {
        Ok(_) => log("[counter-driver] watching the count".to_string()),
        Err(e) => log(format!("[counter-driver] watch failed: {}", e)),
    }

    // COMMANDS: a burst summing to 10, plus one the SM must reject.
    let mut expected = 0i64;
    for n in [1i64, 2, 3, 4] {
        match call_i64(&s, "my:counter.increment", Value::from(n)) {
            Ok(c) => {
                expected += n;
                log(format!("[counter-driver] increment({}) -> count={}", n, c));
            }
            Err(e) => log(format!("[counter-driver] increment({}) failed: {}", n, e)),
        }
    }
    match call_i64(&s, "my:counter.increment", Value::from(-5i64)) {
        Ok(c) => log(format!("[counter-driver] increment(-5) rejected (count still {})", c)),
        Err(e) => log(format!("[counter-driver] increment(-5) error: {}", e)),
    }

    // QUERY: the current value.
    match call_i64(&s, "my:counter.count", Value::from(Vec::<u8>::new())) {
        Ok(c) if c == expected => log(format!("[counter-driver] COUNTER OK: count={} (expected {})", c, expected)),
        Ok(c) => log(format!("[counter-driver] COUNTER FAIL: count={} expected {}", c, expected)),
        Err(e) => log(format!("[counter-driver] count failed: {}", e)),
    }

    Ok((SysState { armed: true, ..state }, ()))
}

/// A typed count notification arrived over the `watch` stream.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: SysState, msg: Vec<u8>) -> Result<(SysState, ()), String> {
    match decode_count(&msg) {
        Some(c) => {
            let n = state.watch_events + 1;
            log(format!("[counter-driver] WATCH update #{}: count={}", n, c));
            Ok((SysState { watched_count: c, watch_events: n, ..state }, ()))
        }
        None => Ok((state, ())),
    }
}
