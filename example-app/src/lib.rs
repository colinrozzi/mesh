//! Example app actor: the intended way an agent uses mesh.
//!
//! The app supervises its *own* mesh node (a child in its supervision tree) and
//! drives it over theater's `message-server` — no TCP, no handshake, no signing.
//!
//!   - `init`: learn our actor-id (`runtime.self`), register with the message server
//!     (so we can receive deliveries), and spawn our node child with a mesh
//!     `InitConfig`. We do NOT command the node here — `spawn` returns before the
//!     child's message-server registration lands in the router, so a `request`
//!     fired from inside `init` can't route ("Actor not found"). We arm a timer.
//!   - first `handle-tick`: the node is now reachable and we're live, so we
//!     `Register` ourselves for delivery and `Submit` our greeting.
//!   - `handle-send`: committed payloads arrive here — this handler is the
//!     delivery callback.
//!
//! Two of these apps (wired as a 2-node mesh via config) each receive the
//! other's greeting.
//!
//! NOTE: uses theater's `runtime.self` (added in theater #127) to learn the
//! actor's own id. Because `self` is a WIT keyword the `pack_types!` tokenizer
//! can't express, it's declared as an `#[import]` binding only, not in metadata.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::{export, import, pack_types, GraphValue, Value};

packr_guest::setup_guest!();

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
            // self stubbed
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
        }
        theater:simple/timer {
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
        theater:simple/message-server-host {
            register: func() -> result<_, string>,
            request: func(actor-id: string, msg: list<u8>) -> result<list<u8>, string>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
        // NOTE: `params: tuple<list<u8>>` is the wire contract; packr delivers it
        // POSITIONALLY — the impl is `fn(state, data: list<u8>)`, NOT a 1-tuple
        // `(Vec<u8>,)`. Same convention as handle-request / handle-child-event.
        theater:simple/message-server-client.handle-send: func(state: actor-state, params: tuple<list<u8>>) -> result<actor-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
// `self` is a WIT keyword the pack_types! tokenizer can't express (no %self
// escape), so it's declared only as an #[import] binding, not in the metadata.
#[import(module = "theater:simple/runtime", name = "self")]
fn runtime_self() -> String;
#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(
    manifest: String,
    init_state: Option<Value>,
    wasm_bytes: Option<Vec<u8>>,
) -> Result<String, String>;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;
#[import(module = "theater:simple/message-server-host", name = "request")]
fn message_server_request(actor_id: String, msg: Vec<u8>) -> Result<Vec<u8>, String>;

/// The actor's own id, via theater's `runtime.self` (added in #127).
fn my_actor_id() -> String {
    runtime_self()
}

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct AppState {
    pub label: String,
    pub my_id: String,
    pub node_id: String,
    pub greeting: String,
    /// False until we've registered + submitted (done on the first tick).
    pub armed: bool,
}

#[derive(serde::Deserialize)]
struct AppConfig {
    label: String,
    node_manifest: String,
    node_seed: String,
    node_listen: String,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    dial: Vec<PeerEntry>,
    greeting: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct PeerEntry {
    pubkey: String,
    address: String,
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(AppState, ()), String> {
    let cfg: AppConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse app config: {}", e))?
        }
        _ => return Err("missing app config".to_string()),
    };
    log(format!("[app {}] init", cfg.label));

    let my_id = my_actor_id();
    if let Err(e) = message_server_register() {
        log(format!("[app {}] message-server register failed: {}", cfg.label, e));
    }

    // Spawn our node child, handing it a mesh InitConfig as a Value::String.
    let node_init = build_node_init(&cfg);
    let node_id = supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(node_init)), None)
        .map_err(|e| format!("spawn node: {}", e))?;
    log(format!("[app {}] spawned node {}", cfg.label, node_id));

    // Defer commands to the first tick — the node isn't reachable via the
    // message-server router until after spawn returns. Wait a few seconds so the
    // peer mesh has connected before we submit (an event authored before any peer
    // is present only finalizes later, via catch-up).
    if let Err(e) = timer_set_interval("arm".to_string(), 3000) {
        log(format!("[app {}] set-interval failed: {}", cfg.label, e));
    }

    Ok((
        AppState { label: cfg.label, my_id, node_id, greeting: cfg.greeting, armed: false },
        (),
    ))
}

/// First tick: the node is now reachable and we're live — Register for delivery
/// and Submit our greeting. Idempotent via `armed`.
#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: AppState, _timer: String) -> Result<(AppState, ()), String> {
    if state.armed {
        return Ok((state, ()));
    }
    // Drive the node via the mesh-client library — no envelope/message-server
    // code here. `message_server_request` is our bound host import.
    match mesh_client::register(message_server_request, &state.node_id, &state.my_id) {
        Ok(()) => log(format!("[app {}] registered for delivery", state.label)),
        Err(e) => log(format!("[app {}] register failed: {}", state.label, e)),
    }
    match mesh_client::submit(message_server_request, &state.node_id, state.greeting.as_bytes()) {
        Ok(h) => log(format!("[app {}] submitted ({})", state.label, short_hex(&h))),
        Err(e) => log(format!("[app {}] submit failed: {}", state.label, e)),
    }
    Ok((AppState { armed: true, ..state }, ()))
}

/// A committed payload arrived from our node — this handler is the delivery
/// callback. Log `(from, body)` so an integration test can observe it.
// packr passes params flat: the message-server `params: tuple<list<u8>>` arrives
// as a single positional `msg` arg, not a nested 1-tuple.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: AppState, msg: Vec<u8>) -> Result<(AppState, ()), String> {
    match mesh_client::incoming(&msg) {
        Some(mesh_client::Incoming::Ready) => {
            log(format!("[app {}] READY (node admitted + synced)", state.label))
        }
        Some(mesh_client::Incoming::Delivery { from, body }) => log(format!(
            "[app {}] RECEIVED from {}: {}",
            state.label,
            short_hex(&from),
            String::from_utf8_lossy(&body),
        )),
        None => log(format!("[app {}] malformed message ({} bytes)", state.label, msg.len())),
    }
    Ok((state, ()))
}

/// Build the mesh node's `InitConfig` JSON from our app config, via mesh-client.
fn build_node_init(cfg: &AppConfig) -> String {
    let members: Vec<&str> = cfg.members.iter().map(String::as_str).collect();
    let dial: Vec<(&str, &str)> =
        cfg.dial.iter().map(|p| (p.pubkey.as_str(), p.address.as_str())).collect();
    mesh_client::node_config(&cfg.node_seed, &cfg.node_listen, &members, &dial)
}

fn short_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(12);
    for &b in bytes.iter().take(6) {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
