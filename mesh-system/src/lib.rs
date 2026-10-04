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
//!
//! IN-MODULE STATE (theater engine-axis): the runtime no longer threads actor state through
//! calls. `SysState` lives in a module-global `#[derive(State)]` cell — seeded in `init`,
//! read/mutated in place from the theater-facing exports. The composed `node` KEEPS its
//! state-threaded composite-internal interface (guest↔guest packr, below the theater
//! boundary): the cell holds the node's opaque bytes and threads them into the node calls
//! exactly as before (the "composed actors: migration stops at the theater boundary" rule).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, import_from, pack_types, GraphValue, Value, ValueType};
use theater_guest::State;

#[cfg(not(test))]
packr_guest::setup_guest!();

/// The system's persisted actor state: the host listener id + the node's opaque bytes.
/// Both are serializable (the listener id is a connection-id string, not a live resource),
/// so the whole struct rides in a `#[derive(State)]` cell — no `StateCell` split needed.
#[derive(Clone, GraphValue, State)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SysState {
    pub listener_id: String,
    /// Durable content-store id (from `store.new()`), where the node blob is persisted for
    /// cold-boot recovery. Manifest pins the store's id + base-path so it's stable across
    /// restarts; empty = no store handler (persistence off, in-memory only).
    pub store_id: String,
    /// The node component's opaque state (see node.pact) — we persist it, never inspect it.
    pub node: Vec<u8>,
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
        // Durable store for cold-boot recovery: persist the node blob on mutate, read it back
        // on init. Optional — absent handler => store.new() errors => persistence stays off.
        theater:simple/store {
            new: func() -> result<string, string>,
            get: func(store-id: string, content-ref: string) -> result<list<u8>, string>,
            get-by-label: func(store-id: string, label: string) -> result<option<string>, string>,
            store-at-label: func(store-id: string, label: string, content: list<u8>) -> result<string, string>,
        }
        // The composed pure core (node.pact / DESIGN-dx.md Interface 0). Declared in FULL
        // (the interface hash covers every function) even though a few are only reached via
        // the RPC surface. State is opaque bytes; effects come back as self-framed blobs.
        // This is a guest↔guest composite-internal interface — BELOW the theater boundary —
        // so it KEEPS its state-threaded shape through the in-module-state migration.
        node {
            init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>,
            resume: func(events: list<list<u8>>, config: string) -> result<tuple<list<u8>, list<u8>>, string>,
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
        // Theater-facing exports — in-module-state model: own args only, no state param/return.
        theater:simple/actor.init: func(config: value) -> result<_, string>,
        // Auto-generated by #[derive(State)]; declared so `has_export` finds it.
        theater:simple/actor.get-state: func() -> value,
        theater:simple/tcp-client.handle-connection: func(connection-id: string) -> result<_, string>,
        theater:simple/tcp-client.on-data: func(connection-id: string, data: list<u8>) -> result<_, string>,
        theater:simple/tcp-client.on-close: func(connection-id: string, reason: string) -> result<_, string>,
        theater:simple/timer.handle-tick: func(timer-name: string) -> result<_, string>,
        // RPC action surface (executor↔system): input = the verb's own params, return =
        // result<ret, string> (the caller peels the transport wrapper + this result). No
        // state slot — the SysState cell holds it. Retired in M6 when the executor merges in.
        my:mesh.author: func(input: value) -> value,
        my:mesh.current-state: func(input: value) -> value,
        my:mesh.members: func(input: value) -> value,
        my:mesh.event-status: func(input: value) -> value,
        my:mesh.subscribe: func(input: value) -> value,
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
#[import(module = "theater:simple/store", name = "new")]
fn store_new() -> Result<String, String>;
#[import(module = "theater:simple/store", name = "get")]
fn store_get(store_id: String, content_ref: String) -> Result<Vec<u8>, String>;
#[import(module = "theater:simple/store", name = "get-by-label")]
fn store_get_by_label(store_id: String, label: String) -> Result<Option<String>, String>;
#[import(module = "theater:simple/store", name = "store-at-label")]
fn store_at_label(store_id: String, label: String, content: Vec<u8>) -> Result<String, String>;

// ---- the composed pure node (node.pact) ----
#[import_from("node", name = "init")]
fn node_init(config: String, now: u64) -> Result<(Vec<u8>, Vec<u8>), String>;
#[import_from("node", name = "resume")]
fn node_resume(events: Vec<Vec<u8>>, config: String) -> Result<(Vec<u8>, Vec<u8>), String>;
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

// ---- result helpers (in-module-state exports return their own result, no state slot) ----

/// `result<T, string>::ok(v)` — packr-abi 0.24 encodes it as `Value::Result` (not a Variant).
fn ok_result(v: Value) -> Value {
    Value::Result {
        ok_type: v.infer_type(),
        err_type: ValueType::String,
        value: Ok(Box::new(v)),
    }
}

/// `result<_, string>::ok(())` — success with no payload.
fn ok_unit() -> Value {
    ok_result(Value::Tuple(Vec::new()))
}

/// `result<_, string>::err(msg)`.
fn err_result(msg: &str) -> Value {
    let unit = Value::Tuple(Vec::new());
    Value::Result {
        ok_type: unit.infer_type(),
        err_type: ValueType::String,
        value: Err(Box::new(Value::String(msg.to_string()))),
    }
}

// ---- effect + init-plan decoders (mirror the node's encoders) ----

/// Perform each effect the node returned: `[kind:u8][id-len:u16 BE][id][payload]`
/// (0=tcp send, 1=message-server send to app, 2=tcp close, 4=persist event, 5=persist index).
/// `store_id` is the durable store (empty = persistence off). Dials (kind 3) are handled by the
/// caller (they re-enter the node), not here.
fn perform(effects: Vec<Vec<u8>>, store_id: &str) {
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
            // Incremental persistence: store the event (id = its hash) / the boot index (id =
            // the index label) under its label. Content-addressed + immutable, so each event is
            // written once; no whole-DAG snapshot. No-op if persistence is off.
            4 | 5 if !store_id.is_empty() => {
                if let Err(err) = store_at_label(store_id.to_string(), id, payload) {
                    log(format!("[mesh-system] persist (kind {}) failed: {}", kind, err));
                }
            }
            _ => {}
        }
    }
}

/// Split the node's tick effects into (re-dials, rest). A Dial (kind 3) can't go through
/// `perform` — it re-enters the node (`on-connect`) and mutates the cell — so pull those
/// out and run them via `execute_dial`; everything else passes through unchanged. A Dial is
/// `[3][addr-len:u16 BE][addr][pubkey]`: id = dial address, payload = peer pubkey hex.
fn take_dials(effects: Vec<Vec<u8>>) -> (Vec<(String, String)>, Vec<Vec<u8>>) {
    let mut dials = Vec::new();
    let mut rest = Vec::new();
    for e in effects {
        if e.len() >= 3 && e[0] == 3 {
            let id_len = u16::from_be_bytes([e[1], e[2]]) as usize;
            if e.len() >= 3 + id_len {
                let address = String::from_utf8_lossy(&e[3..3 + id_len]).into_owned();
                let pubkey = String::from_utf8_lossy(&e[3 + id_len..]).into_owned();
                dials.push((pubkey, address));
                continue;
            }
        }
        rest.push(e);
    }
    (dials, rest)
}

/// Execute a self-healing re-dial the node asked for on tick: connect, then let the node
/// emit its HELLO via `on-connect(dialed=true)`. Mirrors the init dial sequence; not
/// expressible as a `perform` effect because it re-enters the node and mutates the cell.
fn execute_dial(pubkey: String, address: String, store_id: &str) {
    match tcp_connect(address.clone()) {
        Ok(conn_id) => {
            let _ = tcp_activate(conn_id.clone());
            let _ = tcp_set_active(conn_id.clone(), "active".to_string());
            let out = SysState::with_mut(|s| {
                let node = core::mem::take(&mut s.node);
                let (node, out) = node_on_connect(node, conn_id, true, pubkey);
                s.node = node;
                out
            });
            perform(out, store_id);
        }
        Err(e) => log(format!("[mesh-system] re-dial {} failed: {}", address, e)),
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

// ---- durable persistence (cold-boot recovery) ----
//
// INCREMENTAL model: the node emits a Persist effect per admitted event (stored once under a
// label = its 64-char hash hex, immutable + content-addressed) and a PersistIndex effect (the
// concatenated hash list) when the admitted set grows — both performed in `perform()`. There is
// NO whole-DAG snapshot. On cold boot we load the index, fetch each event blob by its hash
// label, and hand the blobs to `node_resume` to rebuild. This replaces the old O(n²)
// whole-state-snapshot-per-event (71KB of chat → 2.1GB) with O(1)-per-event writes.

/// The store label the boot index (admitted event-hash list) lives under. Must match the node's.
const PERSIST_INDEX_LABEL: &str = "node-index";

/// Load the persisted event blobs for cold-boot resume: read the index, chunk it into 64-char
/// hash labels, fetch each event. None on first boot (no index) or persistence-off.
fn load_events(store_id: &str) -> Option<Vec<Vec<u8>>> {
    if store_id.is_empty() {
        return None;
    }
    let index_ref = store_get_by_label(store_id.to_string(), PERSIST_INDEX_LABEL.to_string()).ok()??;
    let index = store_get(store_id.to_string(), index_ref).ok()?;
    let index = String::from_utf8(index).ok()?;
    let mut events = Vec::new();
    for chunk in index.as_bytes().chunks(64) {
        let label = String::from_utf8_lossy(chunk).into_owned();
        if let Ok(Some(ev_ref)) = store_get_by_label(store_id.to_string(), label.clone()) {
            if let Ok(bytes) = store_get(store_id.to_string(), ev_ref) {
                events.push(bytes);
            } else {
                log(format!("[mesh-system] resume: event blob {} unreadable", label));
            }
        } else {
            log(format!("[mesh-system] resume: event {} missing from store", label));
        }
    }
    Some(events)
}

// ---- theater lifecycle handlers (the entry: own I/O, drive the node) ----

#[export(name = "theater:simple/actor.init")]
fn init(config: Value) -> Value {
    let config = match config {
        Value::String(s) if !s.is_empty() => s,
        _ => return err_result("missing init_state (need {\"node_seed\":\"...\"})"),
    };

    // Durable store (optional): a store handler in the manifest gives a stable id via new();
    // absent/denied => store_id="" => persistence off (in-memory, warm-restart via replay only).
    let store_id = store_new().unwrap_or_default();

    // Cold-boot recovery: resume from the persisted blob if present (keeps identity + chain +
    // frontier), else first-boot init (fresh genesis).
    let (mut node, plan) = match load_events(&store_id) {
        Some(events) => match node_resume(events, config.clone()) {
            Ok(v) => {
                log("[mesh-system] resumed from persisted events".to_string());
                v
            }
            Err(e) => return err_result(&format!("resume failed: {}", e)),
        },
        None => match node_init(config, now()) {
            Ok(v) => v,
            Err(e) => return err_result(&e),
        },
    };
    let (listen_addr, tick_ms, dials) = decode_init_plan(&plan);

    let listener_id = match tcp_listen(listen_addr.clone()) {
        Ok(id) => id,
        Err(e) => return err_result(&format!("listen failed: {}", e)),
    };
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
                perform(out, &store_id);
            }
            Err(e) => log(format!("[mesh-system] dial {} failed: {}", addr, e)),
        }
    }

    // No explicit persist here: the node persists each event incrementally via Persist effects
    // (the genesis lands on the first tick). SysState holds the store id for perform() to use.
    SysState::set(SysState { listener_id, store_id, node });
    ok_unit()
}

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(conn_id: String) -> Value {
    if tcp_activate(conn_id.clone()).is_err()
        || tcp_set_active(conn_id.clone(), "active".to_string()).is_err()
    {
        let _ = tcp_close(conn_id);
        return ok_unit();
    }
    let (out, store_id) = SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        let (node, out) = node_on_connect(node, conn_id, false, String::new());
        s.node = node;
        (out, s.store_id.clone())
    });
    perform(out, &store_id);
    ok_unit()
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(conn_id: String, data: Vec<u8>) -> Value {
    let now = now();
    let (out, store_id) = SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        let (node, out) = node_on_bytes(node, conn_id, data, now);
        s.node = node;
        // Ingested gossip grows the DAG — the node emits a Persist effect per new event, stored
        // incrementally by perform() below (no whole-DAG snapshot).
        (out, s.store_id.clone())
    });
    perform(out, &store_id);
    ok_unit()
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(conn_id: String, _reason: String) -> Value {
    SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        s.node = node_on_close(node, conn_id);
    });
    ok_unit()
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(_timer_name: String) -> Value {
    let (out, store_id) = SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        let (node, out) = node_tick(node);
        s.node = node;
        (out, s.store_id.clone())
    });
    // Re-dials re-enter the node, so they can't ride `perform` — split and run them after.
    let (dials, rest) = take_dials(out);
    perform(rest, &store_id);
    for (pubkey, address) in dials {
        execute_dial(pubkey, address, &store_id);
    }
    ok_unit()
}

// ---- RPC action surface (executor↔system): read/mutate the SysState cell ----

#[export(name = "my:mesh.author")]
fn author_rpc(input: Value) -> Value {
    let payload = match Vec::<u8>::try_from(input) {
        Ok(p) => p,
        Err(e) => return err_result(&format!("rpc author: payload not list<u8>: {:?}", e)),
    };
    // `ts` is the wall-clock we inject; the node stamps the authored event with exactly it,
    // so it IS the event's canonical timestamp — returned so a caller can apply optimistically
    // with the same ts every replica will fold (mesh-client Session::author).
    let ts = now();
    let (ok, data, out, store_id) = SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        let (node, ok, data, out) = node_author(node, payload, ts);
        s.node = node;
        // The authored event is persisted incrementally via a Persist effect in `out`.
        (ok, data, out, s.store_id.clone())
    });
    perform(out, &store_id);
    // A validation rejection is carried IN-BAND as tuple<ok, data, ts> (never result::err,
    // which theater would treat as a fault). data = hash on success, reason on rejection.
    ok_result(Value::Tuple(alloc::vec![Value::Bool(ok), Value::from(data), Value::U64(ts)]))
}

#[export(name = "my:mesh.current-state")]
fn current_state_rpc(_input: Value) -> Value {
    let bytes = SysState::with(|s| node_current_state(s.node.clone()));
    ok_result(Value::from(bytes))
}

#[export(name = "my:mesh.members")]
fn members_rpc(_input: Value) -> Value {
    let members = SysState::with(|s| node_current_members(s.node.clone()));
    ok_result(Value::from(members))
}

#[export(name = "my:mesh.event-status")]
fn event_status_rpc(input: Value) -> Value {
    let id = match Vec::<u8>::try_from(input) {
        Ok(v) => v,
        Err(_) => return err_result("rpc event-status: expected a 32-byte hash"),
    };
    let status = SysState::with(|s| node_event_status(s.node.clone(), id));
    ok_result(Value::U8(status))
}

#[export(name = "my:mesh.subscribe")]
fn subscribe_rpc(input: Value) -> Value {
    let actor_id = match String::try_from(input) {
        Ok(s) => s,
        Err(e) => return err_result(&format!("rpc subscribe: actor-id not a string: {:?}", e)),
    };
    let (out, store_id) = SysState::with_mut(|s| {
        let node = core::mem::take(&mut s.node);
        let (node, out) = node_subscribe(node, actor_id);
        s.node = node;
        (out, s.store_id.clone())
    });
    perform(out, &store_id);
    ok_result(Value::Bool(true))
}
