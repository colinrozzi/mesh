//! `echo-system` — the tier-2 reference executor, in two roles.
//!
//! Two theater actors, each supervising its own echo node, driving the distributed
//! request/response loop over the intended surfaces — RPC for actions, the message-
//! server stream for events:
//!   - **server** (react-and-reply): watches the finalized stream; on a `Request`
//!     it does work and `author`s a `Response` naming that request. stream-in → work
//!     → author-out — the core control-plane loop.
//!   - **client** (await-matching-response): `author`s a `Request`, remembers its id
//!     (the returned hash), then watches the stream for the finalized `Response`
//!     whose `req_id` matches — the event-driven form of "await the reply".
//!
//! The two nodes gossip over TCP (client dials server): a Request authored at the
//! client's node reaches the server's node, is reacted to, and the Response comes
//! back — all through the real surfaces, no std test Client. Role is chosen by config.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use echo_protocol::Msg;
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
pub struct SysState {
    pub role: String,
    pub my_id: String,
    pub node_id: String,
    pub request_body: Vec<u8>,
    /// The client's outstanding request id (empty until sent).
    pub pending_req: Vec<u8>,
    pub armed: bool,
}

#[derive(serde::Deserialize)]
struct SysConfig {
    role: String,
    node_manifest: String,
    node_seed: String,
    node_listen: String,
    #[serde(default)]
    dial_pubkey: String,
    #[serde(default)]
    dial_addr: String,
    #[serde(default)]
    request_body: String,
}

// ---- RPC plumbing (same convention as counter-system) ----

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
    let out = rpc_call(node_id.to_string(), func.to_string(), params, no_options());
    unwrap_result(unwrap_result(out)?)
}

/// `author(payload) -> hash`. Success/rejection carried in-band as `tuple<ok, data>`.
fn author(node_id: &str, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    let ret = node_rpc(node_id, "my:mesh.author", Value::from(payload))?;
    match ret {
        Value::Tuple(items) if items.len() == 2 => {
            let mut it = items.into_iter();
            let ok = matches!(it.next(), Some(Value::Bool(true)));
            let data = Vec::<u8>::try_from(it.next().unwrap()).map_err(|e| format!("{:?}", e))?;
            if ok {
                Ok(data)
            } else {
                Err(String::from_utf8_lossy(&data).into_owned())
            }
        }
        other => Err(format!("author: unexpected ret {:?}", other)),
    }
}

fn subscribe(node_id: &str, my_id: &str) {
    match node_rpc(node_id, "my:mesh.subscribe", Value::String(my_id.to_string())) {
        Ok(_) => log("[echo] subscribed to the finalized stream".to_string()),
        Err(e) => log(format!("[echo] subscribe failed: {}", e)),
    }
}

/// Decode a finalized dag-node frame from the stream into `(sm-event id, payload)`.
/// Frame = `[len u32][kind u8=0x93][id 32][author 32][ts u64][ndeps u16][deps..][payload]`.
fn decode_finalized(frame: &[u8]) -> Option<([u8; 32], Vec<u8>)> {
    const FRAME_FINALIZED: u8 = 0x93;
    if frame.len() < 79 || frame[4] != FRAME_FINALIZED {
        return None;
    }
    let id: [u8; 32] = frame[5..37].try_into().ok()?;
    let ndeps = u16::from_be_bytes([frame[77], frame[78]]) as usize;
    let payload_start = 79 + ndeps * 32;
    if frame.len() < payload_start {
        return None;
    }
    Some((id, frame[payload_start..].to_vec()))
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SysState, ()), String> {
    let cfg: SysConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse echo config: {}", e))?
        }
        _ => return Err("missing echo config".to_string()),
    };
    log(format!("[echo/{}] init", cfg.role));

    let my_id = runtime_self();
    if let Err(e) = message_server_register() {
        log(format!("[echo/{}] register failed: {}", cfg.role, e));
    }

    // Build the node InitConfig; the client dials the server so the nodes gossip.
    let node_init = if cfg.dial_pubkey.is_empty() {
        format!(r#"{{"node_seed":"{}","listen_addr":"{}"}}"#, cfg.node_seed, cfg.node_listen)
    } else {
        format!(
            r#"{{"node_seed":"{}","listen_addr":"{}","dial":[{{"pubkey":"{}","address":"{}"}}]}}"#,
            cfg.node_seed, cfg.node_listen, cfg.dial_pubkey, cfg.dial_addr
        )
    };
    let node_id = supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(node_init)), None)
        .map_err(|e| format!("spawn node: {}", e))?;
    log(format!("[echo/{}] spawned node {}", cfg.role, node_id));

    if let Err(e) = timer_set_interval("drive".to_string(), 3000) {
        log(format!("[echo/{}] set-interval failed: {}", cfg.role, e));
    }

    Ok((
        SysState {
            role: cfg.role,
            my_id,
            node_id,
            request_body: cfg.request_body.into_bytes(),
            pending_req: Vec::new(),
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
    subscribe(&state.node_id, &state.my_id);

    let mut pending = state.pending_req.clone();
    if state.role == "client" {
        // ACT: author the request; remember its id to match the reply against.
        let payload = echo_protocol::encode(&Msg::Request { body: state.request_body.clone() });
        match author(&state.node_id, payload) {
            Ok(id) => {
                log(format!("[echo/client] sent request id={}", short(&id)));
                pending = id;
            }
            Err(e) => log(format!("[echo/client] author request failed: {}", e)),
        }
    } else {
        log("[echo/server] ready — awaiting requests on the stream".to_string());
    }

    Ok((SysState { armed: true, pending_req: pending, ..state }, ()))
}

/// A finalized dag-node arrived over the stream — the event feed both roles react to.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: SysState, msg: Vec<u8>) -> Result<(SysState, ()), String> {
    let Some((id, payload)) = decode_finalized(&msg) else {
        return Ok((state, ()));
    };
    let Some(m) = echo_protocol::decode(&payload) else {
        return Ok((state, ()));
    };

    match (state.role.as_str(), m) {
        // SERVER react-and-reply: a request arrived → do work → author a response.
        ("server", Msg::Request { body }) => {
            log(format!("[echo/server] REQUEST id={} → replying", short(&id)));
            let result = body; // echo: reply with the same bytes
            let resp = echo_protocol::encode(&Msg::Response { req_id: id, result });
            match author(&state.node_id, resp) {
                Ok(_) => log("[echo/server] authored response".to_string()),
                Err(e) => log(format!("[echo/server] author response failed: {}", e)),
            }
        }
        // CLIENT await-match: the response to our outstanding request.
        ("client", Msg::Response { req_id, result }) if req_id.as_slice() == state.pending_req.as_slice() => {
            log(format!(
                "[echo/client] ECHO CLIENT GOT RESPONSE for id={}: {}",
                short(&req_id),
                String::from_utf8_lossy(&result)
            ));
        }
        _ => {}
    }
    Ok((state, ()))
}

fn short(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(12);
    for &b in bytes.iter().take(6) {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
