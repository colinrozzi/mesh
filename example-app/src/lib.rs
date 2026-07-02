//! Example app actor: the intended way an agent uses mesh.
//!
//! The app supervises its *own* mesh node (a child in its supervision tree) and
//! drives it over theater's `message-server` — no TCP, no handshake, no signing.
//!
//!   - `init`: learn our actor-id (`get-self`), register with the message server
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
//! NOTE: `get-self` depends on the theater primitive `runtime.get-self`. It's
//! isolated in `my_actor_id()`; if the real signature differs, that's the only
//! line to reconcile.

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
            get-self: func() -> string,
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
        theater:simple/message-server-client.handle-send: func(state: actor-state, params: tuple<list<u8>>) -> result<actor-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
#[import(module = "theater:simple/runtime", name = "get-self")]
fn runtime_get_self() -> String;
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

/// The single spot that depends on theater's `get-self`. Reconcile here if the
/// primitive lands under a different name/signature.
fn my_actor_id() -> String {
    runtime_get_self()
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
    // message-server router until after spawn returns.
    if let Err(e) = timer_set_interval("arm".to_string(), 500) {
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
    request_ok(&state.node_id, mesh_api::encode_register(&state.my_id), &state.label, "register");
    request_ok(
        &state.node_id,
        mesh_api::encode_submit(state.greeting.as_bytes()),
        &state.label,
        "submit",
    );
    Ok((AppState { armed: true, ..state }, ()))
}

/// A committed payload arrived from our node — this handler is the delivery
/// callback. Log `(from, body)` so an integration test can observe it.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: AppState, params: (Vec<u8>,)) -> Result<(AppState, ()), String> {
    let (msg,) = params;
    match mesh_api::decode_delivery(&msg) {
        Some((from, body)) => log(format!(
            "[app {}] RECEIVED from {}: {}",
            state.label,
            short_hex(&from),
            String::from_utf8_lossy(&body),
        )),
        None => log(format!("[app {}] malformed delivery ({} bytes)", state.label, msg.len())),
    }
    Ok((state, ()))
}

/// Build the mesh node's `InitConfig` JSON from our app config.
fn build_node_init(cfg: &AppConfig) -> String {
    let members = serde_json::to_string(&cfg.members).unwrap_or_else(|_| "[]".to_string());
    let dial = serde_json::to_string(&cfg.dial).unwrap_or_else(|_| "[]".to_string());
    format!(
        r#"{{"node_seed":"{}","listen_addr":"{}","members":{},"dial":{}}}"#,
        cfg.node_seed, cfg.node_listen, members, dial,
    )
}

/// Send a command to our node, logging the ack (or error).
fn request_ok(node_id: &str, cmd: Vec<u8>, label: &str, what: &str) {
    match message_server_request(node_id.to_string(), cmd) {
        Ok(reply) => match mesh_api::decode_ack(&reply) {
            Ok(h) => log(format!("[app {}] {} ok ({})", label, what, short_hex(&h))),
            Err(e) => log(format!("[app {}] {} rejected: {}", label, what, e)),
        },
        Err(e) => log(format!("[app {}] {} request failed: {}", label, what, e)),
    }
}

fn short_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(12);
    for &b in bytes.iter().take(6) {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
