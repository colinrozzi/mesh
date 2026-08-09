//! `mesh-runtime` — the reusable system-entry framework (DESIGN-dx.md, "the system").
//!
//! A mesh network participant is one theater actor = `<net>-system (entry) ⊕ node ⊕ SM`.
//! The generic half — owning host I/O and driving the pure `node` (node.pact) — is the
//! same for every network and lives here. A per-network system crate is the actual entry:
//! it declares `pack_types!` (host + node imports + theater lifecycle exports + its OWN
//! typed verbs), binds its imports into a [`Host`] and [`NodeApi`] fn-pointer struct, and
//! delegates the standard lifecycle to the `run_*` helpers below — then adds whatever
//! typed interface it wants (`my:counter.increment`, `my:chat.post`, …) on top by calling
//! [`run_author`] / [`run_current_state`].
//!
//! This crate is a pure `no_std` rlib: it has NO `#[export]` and NO host imports of its
//! own (theater rejects host calls from a non-entry component anyway). The entry injects
//! the real host/node functions as fn pointers, exactly like `mesh-client` injects `rpc`.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use packr_guest::{GraphValue, Value};

/// The system's persisted actor state: the host socket handle + the node's opaque bytes.
/// Shared by every network's entry (referenced as `sys-state` in its `pack_types!`).
#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SysState {
    pub listener_id: String,
    /// The node component's opaque state (node.pact) — persisted, never inspected here.
    pub node: Vec<u8>,
    /// Actor-ids watching this participant's TYPED notifications (`watch`). Substrate
    /// details never reach them — only the domain notification the system projects.
    pub watchers: Vec<String>,
    /// The last notification pushed, so `run_notify` only pushes on an actual change.
    pub last_notify: Vec<u8>,
}

/// The host functions the entry imports, injected as fn pointers.
pub struct Host {
    pub log: fn(String),
    pub now: fn() -> u64,
    pub set_interval: fn(String, u64) -> Result<String, String>,
    pub tcp_listen: fn(String) -> Result<String, String>,
    pub tcp_connect: fn(String) -> Result<String, String>,
    pub tcp_activate: fn(String) -> Result<(), String>,
    pub tcp_set_active: fn(String, String) -> Result<(), String>,
    pub tcp_send: fn(String, Vec<u8>) -> Result<u64, String>,
    pub tcp_close: fn(String) -> Result<(), String>,
    pub ms_register: fn() -> Result<(), String>,
    pub ms_send: fn(String, Vec<u8>) -> Result<(), String>,
}

/// The composed `node` (node.pact) functions, injected as fn pointers.
#[allow(clippy::type_complexity)]
pub struct NodeApi {
    pub init: fn(String, u64) -> Result<(Vec<u8>, Vec<u8>), String>,
    pub on_connect: fn(Vec<u8>, String, bool, String) -> (Vec<u8>, Vec<Vec<u8>>),
    pub on_bytes: fn(Vec<u8>, String, Vec<u8>, u64) -> (Vec<u8>, Vec<Vec<u8>>),
    pub on_close: fn(Vec<u8>, String) -> Vec<u8>,
    pub tick: fn(Vec<u8>) -> (Vec<u8>, Vec<Vec<u8>>),
    pub author: fn(Vec<u8>, Vec<u8>, u64) -> (Vec<u8>, bool, Vec<u8>, Vec<Vec<u8>>),
    pub subscribe: fn(Vec<u8>, String) -> (Vec<u8>, Vec<Vec<u8>>),
    pub current_state: fn(Vec<u8>) -> Vec<u8>,
    pub event_status: fn(Vec<u8>, Vec<u8>) -> u8,
}

// ---- effect + init-plan decoders (mirror the node's encoders) ----

enum Effect {
    Send(String, Vec<u8>),
    App(String, Vec<u8>),
    Close(String),
}

fn decode_effect(e: &[u8]) -> Option<Effect> {
    if e.len() < 3 {
        return None;
    }
    let kind = e[0];
    let id_len = u16::from_be_bytes([e[1], e[2]]) as usize;
    if e.len() < 3 + id_len {
        return None;
    }
    let id = String::from_utf8_lossy(&e[3..3 + id_len]).into_owned();
    let payload = e[3 + id_len..].to_vec();
    match kind {
        0 => Some(Effect::Send(id, payload)),
        1 => Some(Effect::App(id, payload)),
        2 => Some(Effect::Close(id)),
        _ => None,
    }
}

/// Perform each effect the node returned (`[kind][id-len][id][payload]`).
pub fn perform(effects: Vec<Vec<u8>>, h: &Host) {
    for e in effects {
        match decode_effect(&e) {
            Some(Effect::Send(c, b)) => {
                let _ = (h.tcp_send)(c, b);
            }
            Some(Effect::App(i, b)) => {
                let _ = (h.ms_send)(i, b);
            }
            Some(Effect::Close(c)) => {
                let _ = (h.tcp_close)(c);
            }
            None => {}
        }
    }
}

/// `[listen-len:u16][listen][tick_ms:u64][ndials:u16]` then per dial
/// `[pk-len:u16][pk][addr-len:u16][addr]`.
fn decode_init_plan(b: &[u8]) -> (String, u64, Vec<(String, String)>) {
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

// ---- standard lifecycle: the entry's theater handlers delegate to these ----

/// `actor.init`: create the node, perform its init plan (listen / set-interval / dial).
pub fn run_init(config: String, node: &NodeApi, host: &Host) -> Result<SysState, String> {
    let now = (host.now)();
    let (mut n, plan) = (node.init)(config, now)?;
    let (listen, tick, dials) = decode_init_plan(&plan);
    let listener_id = (host.tcp_listen)(listen.clone()).map_err(|e| format!("listen failed: {}", e))?;
    (host.log)(format!("[mesh-system] listening on {} (id={})", listen, listener_id));
    if let Err(e) = (host.set_interval)(alloc::string::ToString::to_string("tick"), tick) {
        (host.log)(format!("[mesh-system] set-interval failed: {}", e));
    }
    if let Err(e) = (host.ms_register)() {
        if !e.contains("Already registered") {
            (host.log)(format!("[mesh-system] register failed: {}", e));
        }
    }
    for (pk, addr) in dials {
        match (host.tcp_connect)(addr.clone()) {
            Ok(conn) => {
                let _ = (host.tcp_activate)(conn.clone());
                let _ = (host.tcp_set_active)(conn.clone(), alloc::string::ToString::to_string("active"));
                let (n2, out) = (node.on_connect)(n, conn, true, pk);
                n = n2;
                perform(out, host);
            }
            Err(e) => (host.log)(format!("[mesh-system] dial {} failed: {}", addr, e)),
        }
    }
    Ok(SysState { listener_id, node: n, watchers: Vec::new(), last_notify: Vec::new() })
}

/// Register `actor` as a watcher and push it the current typed notification now (so a
/// fresh watcher gets the current value immediately, then subsequent changes via `run_notify`).
pub fn run_watch(mut s: SysState, actor: String, notify_now: Vec<u8>, host: &Host) -> SysState {
    let _ = (host.ms_send)(actor.clone(), notify_now.clone());
    if !s.watchers.contains(&actor) {
        s.watchers.push(actor);
    }
    s.last_notify = notify_now;
    s
}

/// Push a typed notification to every watcher — but only if it actually changed. The
/// entry computes the notification (its domain projection); the substrate never leaks.
pub fn run_notify(mut s: SysState, notify: Vec<u8>, host: &Host) -> SysState {
    if notify != s.last_notify {
        for w in &s.watchers {
            let _ = (host.ms_send)(w.clone(), notify.clone());
        }
        s.last_notify = notify;
    }
    s
}

/// `tcp-client.handle-connection`: an inbound peer connected.
pub fn run_on_connect(s: SysState, conn: String, host: &Host, node: &NodeApi) -> SysState {
    if (host.tcp_activate)(conn.clone()).is_err()
        || (host.tcp_set_active)(conn.clone(), alloc::string::ToString::to_string("active")).is_err()
    {
        let _ = (host.tcp_close)(conn);
        return s;
    }
    let (n, out) = (node.on_connect)(s.node, conn, false, String::new());
    perform(out, host);
    SysState { node: n, ..s }
}

/// `tcp-client.on-data`: inbound gossip/handshake bytes.
pub fn run_on_data(s: SysState, conn: String, data: Vec<u8>, host: &Host, node: &NodeApi) -> SysState {
    let (n, out) = (node.on_bytes)(s.node, conn, data, (host.now)());
    perform(out, host);
    SysState { node: n, ..s }
}

/// `tcp-client.on-close`: a connection dropped.
pub fn run_on_close(s: SysState, conn: String, node: &NodeApi) -> SysState {
    let n = (node.on_close)(s.node, conn);
    SysState { node: n, ..s }
}

/// `timer.handle-tick`: delivery + anti-entropy.
pub fn run_tick(s: SysState, host: &Host, node: &NodeApi) -> SysState {
    let (n, out) = (node.tick)(s.node);
    perform(out, host);
    SysState { node: n, ..s }
}

// ---- action helpers the entry's typed verbs build on ----

/// Author a payload and perform the resulting effects. Returns (new state, ok, data)
/// where data is the 32-byte hash on success or the SM's reason bytes on rejection.
pub fn run_author(s: SysState, payload: Vec<u8>, host: &Host, node: &NodeApi) -> (SysState, bool, Vec<u8>) {
    let (n, ok, data, out) = (node.author)(s.node, payload, (host.now)());
    perform(out, host);
    (SysState { node: n, ..s }, ok, data)
}

/// The folded SM state bytes at the current frontier.
pub fn run_current_state(s: &SysState, node: &NodeApi) -> Vec<u8> {
    (node.current_state)(s.node.clone())
}

/// `unknown`/`pending`/`finalized`/`stranded` for an event hash.
pub fn run_event_status(s: &SysState, id: Vec<u8>, node: &NodeApi) -> u8 {
    (node.event_status)(s.node.clone(), id)
}

/// Register/replace the subscribed app; replays the finalized history to it.
pub fn run_subscribe(s: SysState, app_id: String, host: &Host, node: &NodeApi) -> SysState {
    let (n, out) = (node.subscribe)(s.node, app_id);
    perform(out, host);
    SysState { node: n, ..s }
}

// ---- RPC marshaling helpers (the theater rpc value convention) ----

/// Split an RPC input `tuple<state, params>` into the typed state + raw params.
pub fn rpc_split(input: Value) -> Result<(SysState, Value), String> {
    match input {
        Value::Tuple(mut items) if !items.is_empty() => {
            let state = SysState::try_from(items.remove(0))
                .map_err(|e| format!("rpc: undecodable sys state: {:?}", e))?;
            let params = if items.is_empty() { Value::Tuple(Vec::new()) } else { items.remove(0) };
            Ok((state, params))
        }
        _ => Err(alloc::string::ToString::to_string("rpc: expected input tuple<state, ...>")),
    }
}

/// `result::ok((state, ret))` — theater persists `state`, the caller receives `ret`.
pub fn rpc_ok(state: SysState, ret: Value) -> Value {
    Value::Variant {
        type_name: alloc::string::ToString::to_string("result"),
        case_name: alloc::string::ToString::to_string("ok"),
        tag: 0,
        payload: alloc::vec![Value::Tuple(alloc::vec![Value::from(state), ret])],
    }
}

/// `result::err(msg)`.
pub fn rpc_err(msg: &str) -> Value {
    Value::Variant {
        type_name: alloc::string::ToString::to_string("result"),
        case_name: alloc::string::ToString::to_string("err"),
        tag: 1,
        payload: alloc::vec![Value::String(alloc::string::ToString::to_string(msg))],
    }
}
