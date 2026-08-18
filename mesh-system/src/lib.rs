//! `mesh-system` — the generic system entry actor (DESIGN-dx.md, "the system").
//!
//! The entry half of the `system ⊕ node ⊕ SM` composite. It owns ALL host I/O — tcp
//! (gossip sockets), timer, message-server — and drives the composed pure `node`
//! component (`node.pact`): it shuttles inbound bytes to the node and PERFORMS the
//! effects the node returns (self-framed `[kind][id][payload]` blobs). A pure node can't
//! read the clock, so the system injects `now()` where events are signed.
//!
//! This is generic: nothing here is network-specific. It also keeps the RPC action verbs
//! and finalized-stream delivery that executors drive today (both retired in M6, when the
//! executor merges into the system).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, import_from, pack_types, GraphValue, Value};

#[cfg(not(test))]
packr_guest::setup_guest!();

/// The system's persisted actor state: the host socket handle + the node's opaque bytes.
#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SysState {
    pub listener_id: String,
    /// The node component's opaque state (see node.pact) — we persist it, never inspect it.
    pub node: Vec<u8>,
}

pack_types! {
    imports {
        theater:simple/runtime {
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
        // The composed pure core (node.pact / DESIGN-dx.md Interface 0). Declared in FULL
        // (the interface hash covers every function) even though a few are only reached via
        // the RPC surface. State is opaque bytes; effects come back as self-framed blobs.
        node {
            init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>,
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
        theater:simple/actor.init: func(state: value) -> result<sys-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: sys-state, connection-id: string) -> result<sys-state, string>,
        theater:simple/tcp-client.on-data: func(state: sys-state, connection-id: string, data: list<u8>) -> result<sys-state, string>,
        theater:simple/tcp-client.on-close: func(state: sys-state, connection-id: string, reason: string) -> result<sys-state, string>,
        theater:simple/timer.handle-tick: func(state: sys-state, timer-name: string) -> result<sys-state, string>,
        // RPC action surface (executor↔system): input = tuple<state, params>, return =
        // result<tuple<state, ret>, string>. Retired in M6 when the executor merges in.
        my:mesh.author: func(input: value) -> value,
        my:mesh.current-state: func(input: value) -> value,
        my:mesh.members: func(input: value) -> value,
        my:mesh.event-status: func(input: value) -> value,
        my:mesh.subscribe: func(input: value) -> value,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
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

// ---- the composed pure node (node.pact) ----
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
#[import_from("node", name = "current-members")]
fn node_current_members(state: Vec<u8>) -> Vec<Vec<u8>>;
#[import_from("node", name = "event-status")]
fn node_event_status(state: Vec<u8>, id: Vec<u8>) -> u8;

// ---- effect + init-plan decoders (mirror the node's encoders) ----

/// Perform each effect the node returned: `[kind:u8][id-len:u16 BE][id][payload]`
/// (0=tcp send, 1=message-server send to app, 2=tcp close).
fn perform(effects: Vec<Vec<u8>>) {
    for e in effects {
        if e.len() < 3 {
            continue;
        }
        let kind = e[0];
        let id_len = u16::from_be_bytes([e[1], e[2]]) as usize;
        if e.len() < 3 + id_len {
            continue;
        }
        let id = String::from_utf8_lossy(&e[3..3 + id_len]).into_owned();
        let payload = e[3 + id_len..].to_vec();
        match kind {
            0 => {
                let _ = tcp_send(id, payload);
            }
            1 => {
                let _ = message_server_send(id, payload);
            }
            2 => {
                let _ = tcp_close(id);
            }
            _ => {}
        }
    }
}

/// Decode the node's init plan: `[listen-len:u16][listen][tick_ms:u64][ndials:u16]` then
/// per dial `[pk-len:u16][pk][addr-len:u16][addr]`. Returns (listen_addr, tick_ms, dials).
fn decode_init_plan(b: &[u8]) -> (String, u64, alloc::vec::Vec<(String, String)>) {
    let mut dials = Vec::new();
    let rd_str = |b: &[u8], p: &mut usize| -> String {
        if *p + 2 > b.len() {
            return String::new();
        }
        let n = u16::from_be_bytes([b[*p], b[*p + 1]]) as usize;
        *p += 2;
        if *p + n > b.len() {
            return String::new();
        }
        let s = String::from_utf8_lossy(&b[*p..*p + n]).into_owned();
        *p += n;
        s
    };
    let mut p = 0usize;
    let listen = rd_str(b, &mut p);
    let tick = if p + 8 <= b.len() {
        let t = u64::from_be_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        t
    } else {
        2000
    };
    let ndials = if p + 2 <= b.len() {
        let n = u16::from_be_bytes([b[p], b[p + 1]]) as usize;
        p += 2;
        n
    } else {
        0
    };
    for _ in 0..ndials {
        let pk = rd_str(b, &mut p);
        let addr = rd_str(b, &mut p);
        dials.push((pk, addr));
    }
    (listen, tick, dials)
}

// ---- theater lifecycle handlers (the entry: own I/O, drive the node) ----

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SysState, ()), String> {
    let config = match state {
        Value::String(s) if !s.is_empty() => s,
        _ => return Err("missing init_state (need {\"node_seed\":\"...\"})".to_string()),
    };
    let (mut node, plan) = node_init(config, now())?;
    let (listen_addr, tick_ms, dials) = decode_init_plan(&plan);

    let listener_id =
        tcp_listen(listen_addr.clone()).map_err(|e| format!("listen failed: {}", e))?;
    log(format!("[mesh-system] listening on {} (id={})", listen_addr, listener_id));
    if let Err(e) = timer_set_interval("tick".to_string(), tick_ms) {
        log(format!("[mesh-system] set-interval failed: {}", e));
    }
    if let Err(e) = message_server_register() {
        if !e.contains("Already registered") {
            log(format!("[mesh-system] message-server register failed: {}", e));
        }
    }

    // Dial each planned peer, then let the node emit its HELLO via on-connect.
    for (pk, addr) in dials {
        match tcp_connect(addr.clone()) {
            Ok(conn_id) => {
                let _ = tcp_activate(conn_id.clone());
                let _ = tcp_set_active(conn_id.clone(), "active".to_string());
                let (n2, out) = node_on_connect(node, conn_id, true, pk);
                node = n2;
                perform(out);
            }
            Err(e) => log(format!("[mesh-system] dial {} failed: {}", addr, e)),
        }
    }

    Ok((SysState { listener_id, node }, ()))
}

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(state: SysState, conn_id: String) -> Result<(SysState, ()), String> {
    let SysState { listener_id, node } = state;
    if tcp_activate(conn_id.clone()).is_err()
        || tcp_set_active(conn_id.clone(), "active".to_string()).is_err()
    {
        let _ = tcp_close(conn_id);
        return Ok((SysState { listener_id, node }, ()));
    }
    let (node, out) = node_on_connect(node, conn_id, false, String::new());
    perform(out);
    Ok((SysState { listener_id, node }, ()))
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(state: SysState, conn_id: String, data: Vec<u8>) -> Result<(SysState, ()), String> {
    let SysState { listener_id, node } = state;
    let (node, out) = node_on_bytes(node, conn_id, data, now());
    perform(out);
    Ok((SysState { listener_id, node }, ()))
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(state: SysState, conn_id: String, _reason: String) -> Result<(SysState, ()), String> {
    let SysState { listener_id, node } = state;
    let node = node_on_close(node, conn_id);
    Ok((SysState { listener_id, node }, ()))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SysState, _timer_name: String) -> Result<(SysState, ()), String> {
    let SysState { listener_id, node } = state;
    let (node, out) = node_tick(node);
    perform(out);
    Ok((SysState { listener_id, node }, ()))
}

// ---- RPC action surface (executor↔system) ----

fn rpc_split(input: Value) -> Result<(SysState, Value), String> {
    match input {
        Value::Tuple(mut items) if !items.is_empty() => {
            let state = SysState::try_from(items.remove(0))
                .map_err(|e| format!("rpc: undecodable sys state: {:?}", e))?;
            let params = if items.is_empty() { Value::Tuple(Vec::new()) } else { items.remove(0) };
            Ok((state, params))
        }
        _ => Err("rpc: expected input tuple<state, ...>".to_string()),
    }
}

fn rpc_ok(state: SysState, ret: Value) -> Value {
    Value::Variant {
        type_name: "result".to_string(),
        case_name: "ok".to_string(),
        tag: 0,
        payload: alloc::vec![Value::Tuple(alloc::vec![Value::from(state), ret])],
    }
}

fn rpc_err(msg: &str) -> Value {
    Value::Variant {
        type_name: "result".to_string(),
        case_name: "err".to_string(),
        tag: 1,
        payload: alloc::vec![Value::String(msg.to_string())],
    }
}

#[export(name = "my:mesh.author")]
fn author_rpc(input: Value) -> Value {
    let (state, params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let payload = match Vec::<u8>::try_from(params) {
        Ok(p) => p,
        Err(e) => return rpc_err(&format!("rpc author: payload not list<u8>: {:?}", e)),
    };
    let SysState { listener_id, node } = state;
    // `ts` is the wall-clock we inject; the node stamps the authored event with exactly it,
    // so it IS the event's canonical timestamp — returned so a caller can apply optimistically
    // with the same ts every replica will fold (mesh-client Session::author).
    let ts = now();
    let (node, ok, data, out) = node_author(node, payload, ts);
    perform(out);
    let new_state = SysState { listener_id, node };
    // A validation rejection is carried IN-BAND as tuple<ok, data, ts> (never result::err,
    // which theater would treat as a fault). data = hash on success, reason on rejection.
    rpc_ok(new_state, Value::Tuple(alloc::vec![Value::Bool(ok), Value::from(data), Value::U64(ts)]))
}

#[export(name = "my:mesh.current-state")]
fn current_state_rpc(input: Value) -> Value {
    let (state, _params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let bytes = node_current_state(state.node.clone());
    rpc_ok(state, Value::from(bytes))
}

#[export(name = "my:mesh.members")]
fn members_rpc(input: Value) -> Value {
    let (state, _params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let members = node_current_members(state.node.clone());
    rpc_ok(state, Value::from(members))
}

#[export(name = "my:mesh.event-status")]
fn event_status_rpc(input: Value) -> Value {
    let (state, params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let id = match Vec::<u8>::try_from(params) {
        Ok(v) => v,
        Err(_) => return rpc_err("rpc event-status: expected a 32-byte hash"),
    };
    let status = node_event_status(state.node.clone(), id);
    rpc_ok(state, Value::U8(status))
}

#[export(name = "my:mesh.subscribe")]
fn subscribe_rpc(input: Value) -> Value {
    let (state, params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let actor_id = match String::try_from(params) {
        Ok(s) => s,
        Err(e) => return rpc_err(&format!("rpc subscribe: actor-id not a string: {:?}", e)),
    };
    let SysState { listener_id, node } = state;
    let (node, out) = node_subscribe(node, actor_id);
    perform(out);
    rpc_ok(SysState { listener_id, node }, Value::Bool(true))
}
