//! `counter-system` — the counter network's system (the theater entry actor).
//!
//! The pattern for building on mesh: compose `counter-system (entry) ⊕ node ⊕ counter-SM`
//! into one actor, and expose ONLY the counter's domain interface:
//!   - `increment(n)` / `reset()` — commands
//!   - `count() -> i64`          — the current value
//!   - `watch(actor-id)`         — "tell me when the value changes" (typed count updates)
//!
//! Substrate mechanics — event finality, raw dag-node frames, gossip — are sealed inside
//! the node and NEVER exposed here. A caller sees counter concepts, nothing else. The
//! generic node-driving glue lives in `mesh-runtime`; this crate wires its imports and adds
//! the typed verbs (each a translator over `node.author` / `node.current-state`).
//!
//! Notifications are "push the new count when it changes": after any interaction that could
//! move the count, the system recomputes it and `run_notify`s watchers (deduped on value).

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mesh_runtime::{
    run_author, run_current_state, run_init, run_notify, run_on_close, run_on_connect,
    run_on_data, run_tick, run_watch, rpc_err, rpc_ok, Host, NodeApi, SysState,
};
use packr_guest::{export, import, import_from, pack_types, pact, Value};
use theater_guest::StateCell;

packr_guest::setup_guest!();

/// In-module-state cell (theater engine-axis). SysState is defined in mesh-runtime and used
/// via `StateCell` DIRECTLY here in the cdylib: a `#[derive(State)]`-emitted `get-state`
/// export would be dead-stripped from a dependency rlib, so the cell + the get-state export
/// must live in this entry crate (theater-dev's ruling). The `run_*` helpers are stateless
/// over `&mut SysState`; the exports below drive them through the cell.
static STATE: StateCell<SysState> = StateCell::new();

// Generate Cmd + CounterState from the shared counter.pact (shared, via pact!(from …)) — the same schema
// the SM folds; no protocol crate. The codecs are one-liners over the Graph ABI.
pact!(from "../counter.pact");

fn encode(cmd: Cmd) -> Vec<u8> {
    packr_guest::encode(&Value::from(cmd)).unwrap_or_default()
}
fn decode_state(bytes: &[u8]) -> Option<CounterState> {
    packr_guest::decode(bytes).ok().and_then(|v| CounterState::try_from(v).ok())
}
fn encode_count(n: i64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

pack_types! {
    imports {
        theater:simple/self {
            log: func(msg: string),
        }
        theater:simple/tcp {
            listen: func(address: string) -> result<string, string>,
            connect: func(address: string) -> result<string, string>,
            activate: func(connection-id: string) -> result<_, string>,
            set-active: func(connection-id: string, mode: string) -> result<_, string>,
            send: func(connection-id: string, data: list<u8>) -> result<u64, string>,
            close: func(connection-id: string) -> result<_, string>,
        }
        theater:simple/timer {
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
            now: func() -> u64,
        }
        theater:simple/message-server-host {
            register: func() -> result<_, string>,
            send: func(actor-id: string, msg: list<u8>) -> result<_, string>,
        }
        // The composed pure core (node.pact) — declared in FULL (interface hash).
        node {
            init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>,
            // Declared for interface-hash completeness (the node exports it); counter-system
            // doesn't drive persistence, so it's unlinked + uncalled (a dead import).
            resume: func(bytes: list<u8>, config: string) -> result<tuple<list<u8>, list<u8>>, string>,
            on-connect: func(state: list<u8>, conn: string, dialed: bool, peer: string) -> tuple<list<u8>, list<list<u8>>>,
            on-bytes: func(state: list<u8>, conn: string, data: list<u8>, now: u64) -> tuple<list<u8>, list<list<u8>>>,
            on-close: func(state: list<u8>, conn: string) -> list<u8>,
            tick: func(state: list<u8>) -> tuple<list<u8>, list<list<u8>>>,
            author: func(state: list<u8>, payload: list<u8>, now: u64) -> tuple<list<u8>, bool, list<u8>, list<list<u8>>>,
            subscribe: func(state: list<u8>, app-id: string) -> tuple<list<u8>, list<list<u8>>>,
            current-state: func(state: list<u8>) -> list<u8>,
            current-members: func(state: list<u8>) -> list<list<u8>>,
            event-status: func(state: list<u8>, id: list<u8>) -> u8,
        }
    }
    exports {
        // In-module-state model: theater-facing exports take own args only, no state slot.
        theater:simple/actor.init: func(config: value) -> result<_, string>,
        theater:simple/actor.get-state: func() -> value,
        theater:simple/tcp-client.handle-connection: func(connection-id: string) -> result<_, string>,
        theater:simple/tcp-client.on-data: func(connection-id: string, data: list<u8>) -> result<_, string>,
        theater:simple/tcp-client.on-close: func(connection-id: string, reason: string) -> result<_, string>,
        theater:simple/timer.handle-tick: func(timer-name: string) -> result<_, string>,
        // The counter's domain interface — the ONLY surface a caller sees.
        my:counter.increment: func(input: value) -> value,
        my:counter.reset: func(input: value) -> value,
        my:counter.count: func(input: value) -> value,
        my:counter.watch: func(input: value) -> value,
    }
}

#[import(module = "theater:simple/self", name = "log")]
fn log(msg: String);
#[import(module = "theater:simple/timer", name = "now")]
fn now() -> u64;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "listen")]
fn tcp_listen(address: String) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "connect")]
fn tcp_connect(address: String) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "activate")]
fn tcp_activate(conn_id: String) -> Result<(), String>;
#[import(module = "theater:simple/tcp", name = "set-active")]
fn tcp_set_active(conn_id: String, mode: String) -> Result<(), String>;
#[import(module = "theater:simple/tcp", name = "send")]
fn tcp_send(conn_id: String, data: Vec<u8>) -> Result<u64, String>;
#[import(module = "theater:simple/tcp", name = "close")]
fn tcp_close(conn_id: String) -> Result<(), String>;
#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;
#[import(module = "theater:simple/message-server-host", name = "send")]
fn message_server_send(actor_id: String, msg: Vec<u8>) -> Result<(), String>;

#[import_from("node", name = "init")]
fn node_init(config: String, now: u64) -> Result<(Vec<u8>, Vec<u8>), String>;
#[import_from("node", name = "on-connect")]
fn node_on_connect(state: Vec<u8>, conn: String, dialed: bool, peer: String) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "on-bytes")]
fn node_on_bytes(state: Vec<u8>, conn: String, data: Vec<u8>, now: u64) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "on-close")]
fn node_on_close(state: Vec<u8>, conn: String) -> Vec<u8>;
#[import_from("node", name = "tick")]
fn node_tick(state: Vec<u8>) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "author")]
#[allow(clippy::type_complexity)]
fn node_author(state: Vec<u8>, payload: Vec<u8>, now: u64) -> (Vec<u8>, bool, Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "subscribe")]
fn node_subscribe(state: Vec<u8>, app_id: String) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "current-state")]
fn node_current_state(state: Vec<u8>) -> Vec<u8>;
#[import_from("node", name = "event-status")]
fn node_event_status(state: Vec<u8>, id: Vec<u8>) -> u8;

fn host() -> Host {
    Host {
        log,
        now,
        set_interval: timer_set_interval,
        tcp_listen,
        tcp_connect,
        tcp_activate,
        tcp_set_active,
        tcp_send,
        tcp_close,
        ms_register: message_server_register,
        ms_send: message_server_send,
    }
}

fn node_api() -> NodeApi {
    NodeApi {
        init: node_init,
        on_connect: node_on_connect,
        on_bytes: node_on_bytes,
        on_close: node_on_close,
        tick: node_tick,
        author: node_author,
        subscribe: node_subscribe,
        current_state: node_current_state,
        event_status: node_event_status,
    }
}

/// The current counter value, decoded from the node's folded state.
fn current_count(s: &SysState) -> i64 {
    decode_state(&run_current_state(s, &node_api())).map(|st| st.count).unwrap_or(0)
}

/// Push the current count to watchers if it changed (deduped by `run_notify`).
fn notify(s: &mut SysState) {
    let c = current_count(s);
    run_notify(s, encode_count(c), &host());
}

/// `result<_, string>::ok(())` — success with no payload (theater lifecycle handlers).
fn ok_unit() -> Value {
    rpc_ok(Value::Tuple(Vec::new()))
}

// ---- theater lifecycle (drive the framework through the state cell, then notify) ----

#[export(name = "theater:simple/actor.init")]
fn init(config: Value) -> Value {
    let config = match config {
        Value::String(s) if !s.is_empty() => s,
        _ => return rpc_err("missing init_state (need {\"node_seed\":\"...\"})"),
    };
    match run_init(config, &node_api(), &host()) {
        Ok(state) => {
            STATE.set(state);
            ok_unit()
        }
        Err(e) => rpc_err(&e),
    }
}

/// Hand-written get-state (SysState lives in mesh-runtime, so no `#[derive(State)]` here);
/// keeps the actor inspectable via `get-actor-state`.
#[export(name = "theater:simple/actor.get-state")]
fn get_state() -> Value {
    STATE.with(|s| Value::from(s.clone()))
}

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(conn_id: String) -> Value {
    STATE.with_mut(|s| run_on_connect(s, conn_id, &host(), &node_api()));
    ok_unit()
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(conn_id: String, data: Vec<u8>) -> Value {
    STATE.with_mut(|s| {
        run_on_data(s, conn_id, data, &host(), &node_api());
        notify(s);
    });
    ok_unit()
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(conn_id: String, _reason: String) -> Value {
    STATE.with_mut(|s| run_on_close(s, conn_id, &node_api()));
    ok_unit()
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(_timer_name: String) -> Value {
    STATE.with_mut(|s| {
        run_tick(s, &host(), &node_api());
        notify(s);
    });
    ok_unit()
}

// ---- the counter's domain interface ----

/// `increment(n)` — author a `Cmd::Inc(n)`, notify watchers, return the resulting count.
/// A non-positive `n` is rejected by the SM, so the count is unchanged.
#[export(name = "my:counter.increment")]
fn increment(input: Value) -> Value {
    let n = match i64::try_from(input) {
        Ok(n) => n,
        Err(e) => return rpc_err(&format!("increment: expected s64: {:?}", e)),
    };
    let c = STATE.with_mut(|s| {
        let _ = run_author(s, encode(Cmd::Inc(n)), &host(), &node_api());
        notify(s);
        current_count(s)
    });
    rpc_ok(Value::from(c))
}

/// `reset()` — author a `Cmd::Reset`, notify watchers, return the new count (0).
#[export(name = "my:counter.reset")]
fn reset(_input: Value) -> Value {
    let c = STATE.with_mut(|s| {
        let _ = run_author(s, encode(Cmd::Reset), &host(), &node_api());
        notify(s);
        current_count(s)
    });
    rpc_ok(Value::from(c))
}

/// `count() -> i64` — the current value.
#[export(name = "my:counter.count")]
fn count(_input: Value) -> Value {
    let c = STATE.with(current_count);
    rpc_ok(Value::from(c))
}

/// `watch(actor-id)` — register for typed count updates; the current value is pushed now,
/// and each subsequent change is pushed as it happens.
#[export(name = "my:counter.watch")]
fn watch(input: Value) -> Value {
    let actor = match String::try_from(input) {
        Ok(s) => s,
        Err(e) => return rpc_err(&format!("watch: expected actor-id string: {:?}", e)),
    };
    STATE.with_mut(|s| {
        let c = current_count(s);
        run_watch(s, actor, encode_count(c), &host());
    });
    rpc_ok(Value::Bool(true))
}
