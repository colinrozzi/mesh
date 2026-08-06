//! mesh v0 (RSM) — a dumb DAG core + a composed consumer state machine.
//!
//! See DESIGN-rsm.md. The core is **identity + gossip + a partial-order (DAG)
//! event log + witness queries** — nothing more. It holds no membership, finality,
//! or ordering logic: every admitted event is folded through the composed state
//! machine's `validate`/`apply`, and in v0 **finality is admission** (both v0
//! consumers are conflict-free, so an admitted event's validity is permanent).
//!
//! Actor flow:
//!   - init: derive key, author this node's genesis, listen, dial configured peers.
//!   - connections authenticate by **identity proof only** (HELLO → CHALLENGE →
//!     AUTH → ACCEPTED) — the transport is membership-permissive; the SM gates
//!     membership. Then FRONTIER/WANT exchange catches peers up.
//!   - DELIVER gossips events; a newly-seen event is forwarded and (once it folds
//!     cleanly through the SM) its payload is delivered to the app. WANT backfills.
//!   - SUBMIT lets an app author a payload event (its SM's own event bytes).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use ed25519_dalek::{Signer, SigningKey};
use packr_guest::{export, import, import_from, pack_types, GraphValue, Value};
use sha2::{Digest, Sha256};

#[cfg(not(test))]
packr_guest::setup_guest!();

mod codec;
mod conn;
mod dag;
mod event;
mod wire;

use mesh_api as api;

use codec::{
    connections_from_json, connections_to_json, dag_from_json, dag_to_json, events_from_json,
    events_to_json, finality_from_json, finality_to_json, from_hex32, hashes_from_json,
    hashes_to_json, hex,
};
use conn::{ConnState, Phase};
use dag::Dag;
use event::{Event, Hash, PubKey};
use wire::{
    decode_hashes, encode_ack, encode_auth, encode_challenge, encode_conflict, encode_deliver,
    encode_finalized, encode_hash_list_body, encode_hashes, encode_hello, encode_query_reply,
    encode_rejected, try_parse_frame, ParsedFrame, FRAME_ACCEPTED, FRAME_AUTH, FRAME_CHALLENGE,
    FRAME_DELIVER, FRAME_FRONTIER, FRAME_HELLO, FRAME_QUERY, FRAME_SUBMIT, FRAME_WANT, Q_ANCESTRY,
    Q_STATE, Q_STATUS, Q_WITNESSES, STATUS_FINALIZED, STATUS_PENDING, STATUS_STRANDED,
    STATUS_UNKNOWN,
};

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ActorState {
    pub listener_id: String,
    pub listen_addr: String,
    /// This node's signing key (hex), derived from `node_seed`.
    pub signing_key_hex: String,
    /// This node's own chain head (hex event hash).
    pub self_head_hex: String,
    /// Persisted DAG (admitted events).
    pub dag_json: String,
    /// Persisted orphan buffer (events awaiting missing dependencies).
    pub pending_json: String,
    /// Event hashes of payloads already delivered to clients (delivery dedup).
    pub delivered_json: String,
    /// Memoized per-event finality (hex hash → final?), persisted so each tick only
    /// decides newly-admitted events instead of re-folding the whole DAG.
    pub final_json: String,
    pub connections_json: String,
    /// The co-located app actor's id (theater actor-id) to `send` delivered
    /// payloads to, set by a Register command. Empty = no app subscribed.
    pub app_id: String,
    /// True once the one-shot Ready signal has been sent to the app. Prevents
    /// re-emitting it.
    pub ready_sent: bool,
}

pack_types! {
    // The node is GENERIC over the SM's state type `s` (interface-level generic,
    // packr 0.13 M4). `s` is erased at the wire and the node binds it to the dynamic
    // `Value` — so ONE universal node composes with any SM: compose unifies `s` to the
    // SM's concrete state (a typed record, or `list<u8>` for a byte-state SM).
    type s: serializable
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
            receive: func(connection-id: string, max-bytes: u32) -> result<list<u8>, string>,
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
        // The consumer state machine (DESIGN-rsm.md Interface 1), composed IN via
        // `packr compose`. The node calls these synchronously on the fold hot path.
        // Full contract in `state-machine.pact`; the COMPLETE interface is declared
        // (the hash covers all four) even though `members` is dormant while v0 is
        // uniformly admission-final.
        state-machine {
            initial-state: func() -> s,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: s) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: s) -> s,
            members: func(state: s) -> list<list<u8>>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: actor-state, connection-id: string) -> result<actor-state, string>,
        theater:simple/tcp-client.on-data: func(state: actor-state, connection-id: string, data: list<u8>) -> result<actor-state, string>,
        theater:simple/tcp-client.on-close: func(state: actor-state, connection-id: string, reason: string) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
        theater:simple/message-server-client.handle-request: func(state: actor-state, params: tuple<string, list<u8>>) -> result<tuple<actor-state, tuple<option<list<u8>>>>, string>,
        // RPC action surface (theater:simple/rpc): the executor drives the node
        // here — take-an-action / ask-a-question. Dynamic `value` in/out; the call
        // convention is input = tuple<state, params>, return = result<tuple<state,
        // ret>, string> (theater persists the returned state). Live events are NOT
        // here — those push over the message-server stream.
        my:mesh.author: func(input: value) -> value,
        my:mesh.current-state: func(input: value) -> value,
        my:mesh.event-status: func(input: value) -> value,
        my:mesh.subscribe: func(input: value) -> value,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
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
#[import(module = "theater:simple/tcp", name = "receive")]
fn tcp_receive(conn_id: String, max_bytes: u32) -> Result<Vec<u8>, String>;
#[import(module = "theater:simple/tcp", name = "close")]
fn tcp_close(conn_id: String) -> Result<(), String>;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/timer", name = "now")]
fn now_ms() -> u64;
#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;
#[import(module = "theater:simple/message-server-host", name = "send")]
fn message_server_send(actor_id: String, msg: Vec<u8>) -> Result<(), String>;

// ---- composed state machine (DESIGN-rsm.md Interface 1) ----
// The pure fold the node drives: `validate`/`apply` against the ancestry-relative
// state, `members` for the (dormant) finality utility, `initial_state` at genesis.
// `s` (the SM state) is bound to the dynamic `Value`: the node never inspects state,
// it only shuttles it between validate/apply, so binding the erased generic to Value
// keeps the node universal (any SM's concrete state marshals through).
#[import_from("state-machine", name = "initial-state")]
fn sm_initial_state() -> Value;
#[import_from("state-machine", name = "validate")]
fn sm_validate(
    id: Vec<u8>,
    author: Vec<u8>,
    timestamp: u64,
    payload: Vec<u8>,
    state: Value,
) -> Result<bool, String>;
#[import_from("state-machine", name = "apply")]
fn sm_apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Vec<u8>, state: Value) -> Value;
#[import_from("state-machine", name = "members")]
fn sm_members(state: Value) -> Vec<Vec<u8>>;

const LISTEN_ADDR: &str = "127.0.0.1:9447";
const TICK_TIMER: &str = "tick";
const DEFAULT_INTERVAL_MS: u64 = 2000;

// ---- init ----

#[derive(serde::Deserialize)]
struct InitConfig {
    node_seed: String,
    /// Peers to outbound-connect to on init: pubkey + address.
    #[serde(default)]
    dial: Vec<PeerEntry>,
    #[serde(default)]
    listen_addr: Option<String>,
    #[serde(default)]
    tick_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct PeerEntry {
    pubkey: String,
    address: String,
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(ActorState, ()), String> {
    log(String::from("[mesh] init (v0-rsm)"));
    let cfg: InitConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse init_state: {}", e))?
        }
        _ => return Err("missing init_state (need {\"node_seed\":\"...\"})".to_string()),
    };
    let tick_ms = cfg.tick_ms.unwrap_or(DEFAULT_INTERVAL_MS);
    let listen_addr = cfg.listen_addr.clone().unwrap_or_else(|| LISTEN_ADDR.to_string());

    let mut h = Sha256::new();
    h.update(cfg.node_seed.as_bytes());
    let key_bytes: [u8; 32] = h.finalize().into();
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let self_pubkey = signing_key.verifying_key().to_bytes();

    // Every node is a self-rooted log: it authors an (empty) genesis so it has a
    // chain head and a frontier to gossip. Membership no longer gates this — a
    // node's standing is entirely the SM's business (an empty-payload graft is an
    // inert no-op to the SM).
    let mut dag = Dag::new();
    let self_head = author_genesis(&mut dag, &signing_key).event_hash();

    let listener_id =
        tcp_listen(listen_addr.clone()).map_err(|e| format!("listen failed: {}", e))?;
    log(format!(
        "[mesh] listening on {} (id={}); self={}",
        listen_addr,
        listener_id,
        hex(&self_pubkey),
    ));
    if let Err(e) = timer_set_interval(TICK_TIMER.to_string(), tick_ms) {
        log(format!("[mesh] set-interval failed: {}", e));
    }
    // Register with the message server so a co-located app actor can drive us
    // (Submit/Register) and receive delivered payloads. Theater auto-registers
    // actors that declare the handler, so "Already registered" is the normal case.
    if let Err(e) = message_server_register() {
        if !e.contains("Already registered") {
            log(format!("[mesh] message-server register failed: {}", e));
        }
    }

    // Dial peers, handshake from the client side, register them as authed.
    let mut conns: BTreeMap<String, ConnState> = BTreeMap::new();
    for p in &cfg.dial {
        match open_peer_connection(&p.address, &signing_key, &dag) {
            Ok(conn_id) => {
                log(format!("[mesh] dialed peer {} (conn {})", p.address, conn_id));
                conns.insert(conn_id, ConnState::authed(p.pubkey.clone()));
            }
            Err(e) => log(format!("[mesh] dial {} failed: {}", p.address, e)),
        }
    }

    Ok((
        ActorState {
            listener_id,
            listen_addr,
            signing_key_hex: hex(&key_bytes),
            self_head_hex: hex(&self_head),
            dag_json: dag_to_json(&dag),
            pending_json: "[]".to_string(),
            delivered_json: "[]".to_string(),
            final_json: "{}".to_string(),
            connections_json: connections_to_json(&conns),
            app_id: String::new(),
            ready_sent: false,
        },
        (),
    ))
}

/// Dial a peer and run the client side of the handshake, then kick off catch-up
/// (WANT what its ACCEPTED frontier shows we lack, and announce our own
/// frontier). Returns the live, active connection id.
fn open_peer_connection(
    address: &str,
    signing_key: &SigningKey,
    dag: &Dag,
) -> Result<String, String> {
    let conn_id =
        tcp_connect(address.to_string()).map_err(|e| format!("connect {}: {}", address, e))?;
    let pk = signing_key.verifying_key().to_bytes();

    tcp_send(conn_id.clone(), encode_hello(&pk)).map_err(|e| format!("send HELLO: {}", e))?;
    let (kind, nonce) = recv_one_frame(&conn_id)?;
    if kind != FRAME_CHALLENGE || nonce.len() != 32 {
        let _ = tcp_close(conn_id);
        return Err(format!("expected CHALLENGE, got {:#x}", kind));
    }
    let sig = signing_key.sign(&nonce).to_bytes();
    tcp_send(conn_id.clone(), encode_auth(&sig)).map_err(|e| format!("send AUTH: {}", e))?;
    let (kind, payload) = recv_one_frame(&conn_id)?;
    if kind != FRAME_ACCEPTED {
        let _ = tcp_close(conn_id);
        return Err(format!("peer rejected: {:#x}", kind));
    }

    // Catch up against the peer's advertised frontier, and announce ours.
    for want in decode_hashes(&payload).into_iter().filter(|h| !dag.has(h)) {
        let _ = tcp_send(conn_id.clone(), encode_hashes(FRAME_WANT, &[want]));
    }
    let _ = tcp_send(conn_id.clone(), encode_hashes(FRAME_FRONTIER, &all_heads(dag)));

    tcp_set_active(conn_id.clone(), "active".to_string())
        .map_err(|e| format!("set-active: {}", e))?;
    Ok(conn_id)
}

/// Blocking read of one full frame (used during the synchronous dial handshake).
fn recv_one_frame(conn_id: &str) -> Result<(u8, Vec<u8>), String> {
    let mut buf = Vec::with_capacity(4);
    while buf.len() < 4 {
        let chunk = tcp_receive(conn_id.to_string(), 4 - buf.len() as u32)
            .map_err(|e| format!("receive len: {}", e))?;
        if chunk.is_empty() {
            return Err("closed during frame length".to_string());
        }
        buf.extend_from_slice(&chunk);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 || len > 16 * 1024 * 1024 {
        return Err(format!("bad frame length: {}", len));
    }
    let mut body = Vec::with_capacity(len);
    while body.len() < len {
        let chunk = tcp_receive(conn_id.to_string(), (len - body.len()) as u32)
            .map_err(|e| format!("receive body: {}", e))?;
        if chunk.is_empty() {
            return Err("closed during frame body".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    Ok((body[0], body[1..].to_vec()))
}

// ---- connection lifecycle ----

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(state: ActorState, conn_id: String) -> Result<(ActorState, ()), String> {
    if tcp_activate(conn_id.clone()).is_err()
        || tcp_set_active(conn_id.clone(), "active".to_string()).is_err()
    {
        let _ = tcp_close(conn_id);
        return Ok((state, ()));
    }
    let mut conns = connections_from_json(&state.connections_json);
    conns.insert(conn_id.clone(), ConnState::awaiting_hello());
    log(format!("[mesh] conn {} opened", conn_id));
    Ok((ActorState { connections_json: connections_to_json(&conns), ..state }, ()))
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(state: ActorState, conn_id: String, data: Vec<u8>) -> Result<(ActorState, ()), String> {
    let signing_key = SigningKey::from_bytes(&from_hex32(&state.signing_key_hex)?);
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev); // re-buffer or resolve persisted orphans
    }
    let mut conns = connections_from_json(&state.connections_json);
    let mut self_head: Option<Hash> = if state.self_head_hex.is_empty() {
        None
    } else {
        Some(from_hex32(&state.self_head_hex)?)
    };
    let mut delivered: BTreeSet<Hash> =
        hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);

    let mut conn_state = match conns.remove(&conn_id) {
        Some(c) => c,
        None => {
            log(format!("[mesh] on-data for unknown conn {}", conn_id));
            let _ = tcp_close(conn_id);
            return Ok((state, ()));
        }
    };
    conn_state.recv_buf_mut().extend_from_slice(&data);
    let mut should_close = false;

    loop {
        let frame = match try_parse_frame(conn_state.recv_buf()) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                log(format!("[mesh] conn {} parse error: {}", conn_id, e));
                let _ = tcp_send(conn_id.clone(), encode_rejected(&e));
                let _ = tcp_close(conn_id.clone());
                should_close = true;
                break;
            }
        };
        let kept = conn_state.recv_buf()[frame.total_len..].to_vec();
        *conn_state.recv_buf_mut() = kept;

        match conn_state.phase.clone() {
            Phase::AwaitingHello => match step_hello(&conn_id, &frame) {
                Step::Advance(p) => conn_state.phase = p,
                Step::Close => {
                    should_close = true;
                    break;
                }
            },
            Phase::AwaitingAuth { pubkey_hex, nonce_hex } => {
                match step_auth(&conn_id, &frame, &pubkey_hex, &nonce_hex, &dag)? {
                    Step::Advance(p) => {
                        conn_state.phase = p;
                        // Announce our frontier; a behind peer WANTs what it lacks
                        // and we answer with the event (full history is retained,
                        // so every event we've admitted is still here to serve).
                        let _ = tcp_send(
                            conn_id.clone(),
                            encode_hashes(FRAME_FRONTIER, &all_heads(&dag)),
                        );
                    }
                    Step::Close => {
                        should_close = true;
                        break;
                    }
                }
            }
            Phase::Authed { .. } => {
                self_head = handle_authed_frame(
                    &mut dag,
                    &conns,
                    &conn_id,
                    &signing_key,
                    self_head,
                    &frame,
                    &mut finality,
                );
            }
        }
    }

    if !should_close {
        conns.insert(conn_id.clone(), conn_state);
    }
    // Deliver any newly-admitted messages to TCP clients and the co-located app.
    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality);
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            connections_json: connections_to_json(&conns),
            self_head_hex: self_head.map(|h| hex(&h)).unwrap_or_default(),
            ready_sent,
            ..state
        },
        (),
    ))
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(state: ActorState, conn_id: String, reason: String) -> Result<(ActorState, ()), String> {
    log(format!("[mesh] conn {} closed: {}", conn_id, reason));
    let mut conns = connections_from_json(&state.connections_json);
    conns.remove(&conn_id);
    Ok((ActorState { connections_json: connections_to_json(&conns), ..state }, ()))
}

/// Periodic tick: a delivery safety-net + anti-entropy + one-shot Ready. No
/// heartbeat, no eviction — admission-final finality means there is nothing to pump
/// or reap, but gossip is best-effort so we re-announce our frontier so any peer
/// that missed an event pulls it on a later tick (eventual convergence).
#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: ActorState, _timer_name: String) -> Result<(ActorState, ()), String> {
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    let conns = connections_from_json(&state.connections_json);
    let mut delivered: BTreeSet<Hash> =
        hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);

    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality);

    // Anti-entropy: re-advertise our frontier to every authed peer. A peer missing
    // any of these heads answers with WANT, so a one-shot-at-dial gossip miss (or a
    // partition heal) reconciles within a few ticks instead of stranding forever.
    let frontier = all_heads(&dag);
    if !frontier.is_empty() {
        for (cid, cs) in &conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                let _ = tcp_send(cid.clone(), encode_hashes(FRAME_FRONTIER, &frontier));
            }
        }
    }
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            ready_sent,
            ..state
        },
        (),
    ))
}

// ---- app control API (message-server, co-located link) ----

/// A co-located app actor drives its node here: Submit / Register. No handshake,
/// no external signing — the app is our supervisor, so we author under our own
/// key. Returns an ack (event hash, or an error string) as the `request`
/// response. Delivered payloads flow back to the app via `send`.
type RequestReply = (Option<Vec<u8>>,);

// packr passes handler params flat (like on_data's state/conn_id/data), so the
// message-server `params: tuple<string, list<u8>>` arrives as two positional
// args (request_id, data) — NOT one nested tuple.
#[export(name = "theater:simple/message-server-client.handle-request")]
fn handle_request(
    state: ActorState,
    _request_id: String,
    body: Vec<u8>,
) -> Result<(ActorState, RequestReply), String> {
    let signing_key = SigningKey::from_bytes(&from_hex32(&state.signing_key_hex)?);
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    let conns = connections_from_json(&state.connections_json);
    let mut self_head: Option<Hash> = if state.self_head_hex.is_empty() {
        None
    } else {
        Some(from_hex32(&state.self_head_hex)?)
    };
    let mut delivered: BTreeSet<Hash> =
        hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let mut app_id = state.app_id.clone();

    let ack = match api::decode_command(&body) {
        Some(api::Command::Submit(payload)) => {
            let (h, result) = run_command(&mut dag, &conns, &signing_key, self_head, payload, &mut finality);
            self_head = h;
            ack_bytes(result)
        }
        // Membership is no longer a substrate command — it is an SM payload the app
        // authors via Submit (the control-SM validates join/depart). These legacy
        // verbs are declined so an old caller fails loud rather than silently.
        Some(api::Command::Introduce(_)) | Some(api::Command::Depart) => {
            api::encode_ack(false, &[0u8; 32], "membership is an SM payload — author it via Submit")
        }
        Some(api::Command::Register(id)) => {
            // Subscribe this app for delivery, then flush the retained payload
            // history to it so it doesn't miss anything committed before it
            // registered. Admission-final: every admitted payload is delivered.
            app_id = id.clone();
            for h in dag.ordered() {
                if let Some(ev) = dag.events.get(&h) {
                    if ev.payload.is_empty() {
                        continue;
                    }
                    let _ = message_server_send(id.clone(), api::encode_delivery(&ev.author, &ev.payload));
                    delivered.insert(h);
                }
            }
            log(format!("[mesh] app {} registered for delivery", id));
            api::encode_ack(true, &[0u8; 32], "")
        }
        None => api::encode_ack(false, &[0u8; 32], "unrecognized command"),
    };

    deliver_committed(&dag, &conns, &app_id, &mut delivered, &mut finality);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            self_head_hex: self_head.map(|h| hex(&h)).unwrap_or_default(),
            app_id,
            ..state
        },
        (Some(ack),),
    ))
}

/// Encode a command result as an ack payload.
fn ack_bytes(result: Result<Hash, String>) -> Vec<u8> {
    match result {
        Ok(h) => api::encode_ack(true, &h, ""),
        Err(e) => api::encode_ack(false, &[0u8; 32], &e),
    }
}

// ============================================================================
// RPC action surface (theater:simple/rpc)
//
// The reference executor↔node surface for TAKING ACTION / ASKING QUESTIONS —
// synchronous, typed-by-actor-id. Live events are NOT here; those push over the
// message-server stream (see `subscribe`). Call convention (theater
// `call_function`): the export receives `input = tuple<state, params>` and returns
// `result<tuple<state, ret>, string>` — theater persists the returned state. We use
// dynamic `value` in/out (the proven pattern) and convert `ActorState` via its
// derived `From`/`TryFrom` (GraphValue).
// ============================================================================

/// Split an RPC export input `tuple<state, params>` into the typed state + raw params.
fn rpc_split(input: Value) -> Result<(ActorState, Value), String> {
    match input {
        // theater flattens the call to `tuple<state, ...params>`; a no-arg read verb
        // arrives as just `tuple<state>`, so tolerate a missing params slot.
        Value::Tuple(mut items) if !items.is_empty() => {
            let state = ActorState::try_from(items.remove(0))
                .map_err(|e| format!("rpc: undecodable actor state: {:?}", e))?;
            let params = if items.is_empty() { Value::Tuple(Vec::new()) } else { items.remove(0) };
            Ok((state, params))
        }
        _ => Err("rpc: expected input tuple<state, ...>".to_string()),
    }
}

/// `result::ok((state, ret))` — theater persists `state`, the caller receives `ret`.
fn rpc_ok(state: ActorState, ret: Value) -> Value {
    Value::Variant {
        type_name: "result".to_string(),
        case_name: "ok".to_string(),
        tag: 0,
        payload: alloc::vec![Value::Tuple(alloc::vec![Value::from(state), ret])],
    }
}

/// `result::err(msg)`.
fn rpc_err(msg: &str) -> Value {
    Value::Variant {
        type_name: "result".to_string(),
        case_name: "err".to_string(),
        tag: 1,
        payload: alloc::vec![Value::String(msg.to_string())],
    }
}

/// Load the DAG (admitted + persisted orphans re-ingested) from the actor state.
fn load_dag(state: &ActorState) -> Result<Dag, String> {
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    Ok(dag)
}

/// RPC `author(payload) -> hash`: pre-validated author on this node's chain, then
/// gossip to peers + emit the finalized stream. The one write verb.
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
    let signing_key = match from_hex32(&state.signing_key_hex) {
        Ok(b) => SigningKey::from_bytes(&b),
        Err(e) => return rpc_err(&e),
    };
    let mut dag = match load_dag(&state) {
        Ok(d) => d,
        Err(e) => return rpc_err(&e),
    };
    let conns = connections_from_json(&state.connections_json);
    let self_head = if state.self_head_hex.is_empty() {
        None
    } else {
        match from_hex32(&state.self_head_hex) {
            Ok(h) => Some(h),
            Err(e) => return rpc_err(&e),
        }
    };
    let mut delivered: BTreeSet<Hash> = hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);

    let (new_self_head, result) =
        run_command(&mut dag, &conns, &signing_key, self_head, payload, &mut finality);
    // A validation REJECTION is a normal business outcome, not an actor fault — so it
    // is carried IN-BAND as `tuple<ok: bool, data: list<u8>>` (data = hash on success,
    // the SM reason on rejection), never as an export-level `result::err` (which
    // theater treats as a wasm fault and would kill the node). `rpc_err` stays for
    // genuine faults only (undecodable input / corrupt state).
    match result {
        Ok(hash) => {
            deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality);
            let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
            let new_state = ActorState {
                dag_json: dag_to_json(&dag),
                pending_json: events_to_json(&dag.pending_events()),
                delivered_json: hashes_to_json(&delivered_vec),
                final_json: finality_to_json(&finality),
                self_head_hex: new_self_head.map(|h| hex(&h)).unwrap_or_default(),
                ..state
            };
            rpc_ok(new_state, Value::Tuple(alloc::vec![Value::Bool(true), Value::from(hash.to_vec())]))
        }
        Err(reason) => {
            rpc_ok(state, Value::Tuple(alloc::vec![Value::Bool(false), Value::from(reason.into_bytes())]))
        }
    }
}

/// RPC `current-state() -> bytes`: the folded SM state at the current frontier.
#[export(name = "my:mesh.current-state")]
fn current_state_rpc(input: Value) -> Value {
    let (state, _params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let dag = match load_dag(&state) {
        Ok(d) => d,
        Err(e) => return rpc_err(&e),
    };
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let bytes = state_bytes(current_state(&dag, &mut finality));
    rpc_ok(state, Value::from(bytes))
}

/// RPC `event-status(hash) -> u8`: unknown / pending / finalized / stranded.
#[export(name = "my:mesh.event-status")]
fn event_status_rpc(input: Value) -> Value {
    let (state, params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let h: Hash = match Vec::<u8>::try_from(params) {
        Ok(v) if v.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(&v);
            a
        }
        _ => return rpc_err("rpc event-status: expected a 32-byte hash"),
    };
    let dag = match load_dag(&state) {
        Ok(d) => d,
        Err(e) => return rpc_err(&e),
    };
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let status = event_status(&dag, &h, &mut finality);
    rpc_ok(state, Value::U8(status))
}

/// RPC `subscribe(actor-id)`: register an executor for the finalized stream (pushed
/// via message-server `send`). v0 holds one subscriber in `app_id` and replays the
/// finalized history so a late subscriber catches up (dedup-safe — the executor
/// folds by SM state, so replay-all is idempotent).
#[export(name = "my:mesh.subscribe")]
fn subscribe_rpc(input: Value) -> Value {
    let (mut state, params) = match rpc_split(input) {
        Ok(v) => v,
        Err(e) => return rpc_err(&e),
    };
    let actor_id = match String::try_from(params) {
        Ok(s) => s,
        Err(e) => return rpc_err(&format!("rpc subscribe: actor-id not a string: {:?}", e)),
    };
    state.app_id = actor_id;
    let dag = match load_dag(&state) {
        Ok(d) => d,
        Err(e) => return rpc_err(&e),
    };
    let conns = connections_from_json(&state.connections_json);
    // Replay the whole finalized history to the fresh subscriber.
    let mut delivered: BTreeSet<Hash> = BTreeSet::new();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality);
    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    let new_state = ActorState {
        delivered_json: hashes_to_json(&delivered_vec),
        final_json: finality_to_json(&finality),
        ..state
    };
    rpc_ok(new_state, Value::Bool(true))
}

// ---- handshake steps ----

enum Step {
    Advance(Phase),
    Close,
}

/// HELLO → CHALLENGE. Identity proof only: any well-formed pubkey is challenged;
/// the transport does not gate membership (the SM does). The CHALLENGE/AUTH
/// signature still proves the peer owns the key it presented.
fn step_hello(conn_id: &str, frame: &ParsedFrame) -> Step {
    if frame.kind != FRAME_HELLO || frame.payload.len() != 32 {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected HELLO(pubkey)"));
        let _ = tcp_close(conn_id.to_string());
        return Step::Close;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&frame.payload);
    let nonce = challenge_nonce(conn_id);
    let _ = tcp_send(conn_id.to_string(), encode_challenge(&nonce));
    Step::Advance(Phase::AwaitingAuth { pubkey_hex: hex(&pk), nonce_hex: hex(&nonce) })
}

fn step_auth(
    conn_id: &str,
    frame: &ParsedFrame,
    pubkey_hex: &str,
    nonce_hex: &str,
    dag: &Dag,
) -> Result<Step, String> {
    if frame.kind != FRAME_AUTH || frame.payload.len() != 64 {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected AUTH(sig)"));
        let _ = tcp_close(conn_id.to_string());
        return Ok(Step::Close);
    }
    let pk = from_hex32(pubkey_hex)?;
    let nonce = from_hex32(nonce_hex)?;
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&frame.payload);
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let vk = VerifyingKey::from_bytes(&pk).map_err(|e| format!("bad pubkey: {}", e))?;
    if vk.verify(&nonce, &Signature::from_bytes(&sig)).is_err() {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("auth failed"));
        let _ = tcp_close(conn_id.to_string());
        return Ok(Step::Close);
    }
    let _ = tcp_send(conn_id.to_string(), encode_hashes(FRAME_ACCEPTED, &all_heads(dag)));
    log(format!("[mesh] conn {} authed as {}", conn_id, pubkey_hex));
    Ok(Step::Advance(Phase::Authed { pubkey_hex: pubkey_hex.to_string() }))
}

// ---- post-handshake frame handling ----

/// Handle one authenticated frame. Returns the (possibly advanced) self_head.
fn handle_authed_frame(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    frame: &ParsedFrame,
    finality: &mut BTreeMap<Hash, bool>,
) -> Option<Hash> {
    match frame.kind {
        FRAME_DELIVER => match Event::decode(&frame.payload) {
            Ok(ev) => ingest_and_propagate(dag, conns, conn_id, self_head, ev),
            Err(e) => {
                log(format!("[mesh] DELIVER decode failed: {}", e));
                self_head
            }
        },
        // App client asks us to author a payload event on our own chain.
        FRAME_SUBMIT => {
            author_and_broadcast(dag, conns, conn_id, signing_key, self_head, frame.payload.clone(), finality)
        }
        // Interface 2 read verbs (current-state / event-status / witnesses / ancestry).
        FRAME_QUERY => {
            answer_query(dag, conn_id, &frame.payload, finality);
            self_head
        }
        FRAME_WANT => {
            // Answer each WANT with the event if we hold it. The mesh retains full
            // history, so anything we've admitted is still here to serve; a hash we
            // don't hold yet we simply skip and the requester re-asks.
            for h in decode_hashes(&frame.payload) {
                if let Some(ev) = dag.events.get(&h) {
                    let _ = tcp_send(conn_id.to_string(), encode_deliver(&ev.encode()));
                }
            }
            self_head
        }
        FRAME_FRONTIER => {
            let missing: Vec<Hash> = decode_hashes(&frame.payload)
                .into_iter()
                .filter(|h| !dag.has(h))
                .collect();
            if !missing.is_empty() {
                let _ = tcp_send(conn_id.to_string(), encode_hashes(FRAME_WANT, &missing));
            }
            self_head
        }
        // ACK / NOTIFY are responses meant for app clients; a node ignores them.
        _ => self_head,
    }
}

/// Classify an event for Interface 2 `event-status`: `unknown` (not held),
/// `pending` (held but still buffering on a missing dep), `finalized` (admitted +
/// valid against its ancestry — v0 is admission-final), or `stranded` (admitted but
/// invalid against its ancestry — inert).
fn event_status(dag: &Dag, h: &Hash, finality: &mut BTreeMap<Hash, bool>) -> u8 {
    if dag.events.contains_key(h) {
        ensure_finality(dag, finality);
        if finality.get(h).copied().unwrap_or(false) {
            STATUS_FINALIZED
        } else {
            STATUS_STRANDED
        }
    } else if dag.pending_events().iter().any(|e| &e.event_hash() == h) {
        STATUS_PENDING
    } else {
        STATUS_UNKNOWN
    }
}

/// Read the 32-byte hash argument of a query, if present.
fn query_arg_hash(arg: &[u8]) -> Option<Hash> {
    if arg.len() < 32 {
        return None;
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&arg[..32]);
    Some(h)
}

/// Answer an Interface 2 read verb and send the reply on `conn_id`.
fn answer_query(dag: &Dag, conn_id: &str, body: &[u8], finality: &mut BTreeMap<Hash, bool>) {
    let Some((&qkind, arg)) = body.split_first() else {
        return;
    };
    let reply = match qkind {
        Q_STATE => encode_query_reply(Q_STATE, &state_bytes(current_state(dag, finality))),
        Q_STATUS => {
            let status =
                query_arg_hash(arg).map(|h| event_status(dag, &h, finality)).unwrap_or(STATUS_UNKNOWN);
            encode_query_reply(Q_STATUS, &[status])
        }
        Q_WITNESSES => {
            let list: Vec<Hash> = query_arg_hash(arg)
                .map(|h| dag.witnesses(&h).into_iter().collect())
                .unwrap_or_default();
            encode_query_reply(Q_WITNESSES, &encode_hash_list_body(&list))
        }
        Q_ANCESTRY => {
            let list: Vec<Hash> = query_arg_hash(arg)
                .map(|h| dag.ancestors_of(&[h]).into_iter().collect())
                .unwrap_or_default();
            encode_query_reply(Q_ANCESTRY, &encode_hash_list_body(&list))
        }
        _ => return,
    };
    let _ = tcp_send(conn_id.to_string(), reply);
}

/// Ingest a gossiped event: dedup, backfill on missing deps, else admit + forward.
/// Admission-final v0 authors no witnessing graft on receipt — an admitted event
/// is already final, so dissemination (forwarding) is all that's needed.
fn ingest_and_propagate(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    from_conn: &str,
    self_head: Option<Hash>,
    event: Event,
) -> Option<Hash> {
    if dag.has(&event.event_hash()) {
        return self_head; // dedup — already have it
    }
    let missing = missing_deps(dag, &event); // compute before `ingest` consumes it

    match dag.ingest_admitted(event) {
        Ok(admitted) if !admitted.is_empty() => {
            // Forward EVERY newly-admitted event (the incoming one AND any buffered
            // waiters it unblocked) to other peers — the source is excluded (it's not
            // in `conns` during on-data). Forwarding only the incoming event would
            // strand transitively-backfilled chains at a relay (e.g. a bridge that
            // received a chain out of order), breaking multi-hop propagation.
            for h in &admitted {
                if let Some(ev) = dag.events.get(h) {
                    broadcast(conns, from_conn, &encode_deliver(&ev.encode()));
                }
            }
            self_head
        }
        Ok(_) => {
            // Buffered — missing a dependency; ask the source for it.
            if !missing.is_empty() {
                let _ = tcp_send(from_conn.to_string(), encode_hashes(FRAME_WANT, &missing));
            }
            self_head
        }
        Err(e) => {
            log(format!("[mesh] ingest rejected: {}", e));
            self_head
        }
    }
}

// ---- authoring + helpers ----

fn author_genesis(dag: &mut Dag, signing_key: &SigningKey) -> Event {
    let ev = Event::sign(signing_key, now_ms(), None, Vec::new(), Vec::new());
    let _ = dag.ingest(ev.clone());
    ev
}

/// The FINALIZED fold over our whole current frontier — Interface 2's
/// `current-state`. v0 is admission-final, so every admitted event is finalized and
/// this is the fold of the entire held DAG. (Same routine `deliver_committed` uses
/// per-event, here over all heads.)
fn current_state(dag: &Dag, finality: &mut BTreeMap<Hash, bool>) -> Value {
    fold_state_at(dag, &all_heads(dag), finality)
}

/// Serialize the folded SM state (a dynamic `Value`) to the bytes handed to consumers
/// over the read verbs. A byte-state SM (`s := list<u8>`) yields its raw bytes
/// unchanged (wire-preserving — existing JSON-parsing consumers keep working); a
/// typed-state SM yields the Graph-ABI structural encoding, which the consumer decodes
/// via its `GraphValue`.
fn state_bytes(v: Value) -> Vec<u8> {
    match v {
        Value::List { ref items, .. } if items.iter().all(|i| matches!(i, Value::U8(_))) => {
            Vec::<u8>::try_from(v).unwrap_or_default()
        }
        other => packr_guest::encode(&other).unwrap_or_default(),
    }
}

/// Author an event on this node's chain: self_parent = current head (`None` for
/// the node's first event), refs = foreign heads we've seen. **Pre-validated**
/// (Interface 2 `author`): the built event is checked against `current-state`
/// before it is ingested/gossiped, and an inadmissible one is rejected with the
/// SM's own reason rather than authored into a never-finalizing limbo. Ingests and
/// returns it on success.
fn author_event(
    dag: &mut Dag,
    signing_key: &SigningKey,
    _self_head: Option<Hash>,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
) -> Result<Event, String> {
    let author = signing_key.verifying_key().to_bytes();
    // Self-parent from our CURRENT own head *in the DAG* — never a separately
    // threaded/persisted `self_head`, which can lag the DAG across handler
    // invocations. Grafting any *other* own heads also heals a chain that already
    // forked.
    let mut own: Vec<Hash> = dag.heads_of(&author).into_iter().collect();
    let self_parent = own.pop(); // BTreeSet order is deterministic; None until our genesis exists
    let mut refs = foreign_heads(dag, &author);
    refs.extend(own); // merge any residual self-fork so it converges
    let ev = Event::sign(signing_key, now_ms(), self_parent, refs, payload);
    // Pre-validate against the state at this event's ancestry (= our current
    // frontier, since its deps ARE the current heads). An empty payload is the
    // node's own inert graft (genesis/witness) — never SM-gated.
    if !ev.payload.is_empty() {
        let state = current_state(dag, finality);
        sm_validate(ev.event_hash().to_vec(), author.to_vec(), ev.timestamp, ev.payload.clone(), state)?;
    }
    match dag.ingest(ev.clone())? {
        true => Ok(ev),
        false => Err("authored event buffered (missing dep)".to_string()),
    }
}

/// Author a payload event, ACK the requester, and broadcast it. Returns the new
/// self_head on success, unchanged on failure.
fn author_and_broadcast(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
) -> Option<Hash> {
    match author_event(dag, signing_key, self_head, payload, finality) {
        Ok(ev) => {
            let h = ev.event_hash();
            let _ = tcp_send(conn_id.to_string(), encode_ack(&h, true, ""));
            broadcast(conns, conn_id, &encode_deliver(&ev.encode()));
            Some(h)
        }
        Err(e) => {
            let _ = tcp_send(conn_id.to_string(), encode_ack(&[0u8; 32], false, &e));
            self_head
        }
    }
}

/// Author an event on our chain and gossip it to peers, returning the new
/// self_head and a result carrying the event hash (for an app ack). Sends no TCP
/// ACK — the caller (the message-server path) returns the ack itself.
fn run_command(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
) -> (Option<Hash>, Result<Hash, String>) {
    match author_event(dag, signing_key, self_head, payload, finality) {
        Ok(ev) => {
            let h = ev.event_hash();
            broadcast(conns, "", &encode_deliver(&ev.encode()));
            (Some(h), Ok(h))
        }
        Err(e) => (self_head, Err(e)),
    }
}

/// Heads of every author other than `me` that we currently hold.
fn foreign_heads(dag: &Dag, me: &PubKey) -> Vec<Hash> {
    let mut out = Vec::new();
    for a in dag.authors() {
        if a == *me {
            continue;
        }
        out.extend(dag.heads_of(&a));
    }
    out
}

/// All current heads across all authors — our advertised frontier.
fn all_heads(dag: &Dag) -> Vec<Hash> {
    let mut out = Vec::new();
    for a in dag.authors() {
        out.extend(dag.heads_of(&a));
    }
    out
}

fn missing_deps(dag: &Dag, ev: &Event) -> Vec<Hash> {
    let mut out = Vec::new();
    if let Some(sp) = ev.self_parent {
        if !dag.has(&sp) {
            out.push(sp);
        }
    }
    for r in &ev.refs {
        if !dag.has(r) {
            out.push(*r);
        }
    }
    out
}

/// Broadcast a frame to every authed connection except `exclude`.
fn broadcast(conns: &BTreeMap<String, ConnState>, exclude: &str, frame: &[u8]) {
    for (cid, cs) in conns {
        if cid == exclude {
            continue;
        }
        if matches!(cs.phase, Phase::Authed { .. }) {
            let _ = tcp_send(cid.clone(), frame.to_vec());
        }
    }
}

/// Emit the one-shot Ready signal to the subscribed app the first time an app is
/// registered — a node is ready to author as soon as it has an app to serve.
/// Returns the updated `ready_sent`.
fn maybe_emit_ready(app_id: &str, ready_sent: bool) -> bool {
    if ready_sent || app_id.is_empty() {
        return ready_sent;
    }
    let _ = message_server_send(app_id.to_string(), api::encode_ready());
    log(format!("[mesh] signalled READY to app {}", app_id));
    true
}

/// The back-edges (self_parent ∪ refs) of an event — its causal parents.
fn deps_of(ev: &Event) -> Vec<Hash> {
    let mut d = Vec::new();
    if let Some(sp) = ev.self_parent {
        d.push(sp);
    }
    d.extend_from_slice(&ev.refs);
    d
}

/// Fold the SM over the causal past of `frontier` (its ancestry, frontier included)
/// and return the resulting state — the state an event at that frontier is validated
/// against (DESIGN-rsm.md principle 3, ancestry-relative), and the substrate's
/// `current-state` when `frontier = all_heads`.
///
/// **Confluent (principle 4).** Finality and application are SEPARATE: an event is
/// FINAL iff it validates against *its own* ancestry state (judged once, independent
/// of any linearization), and the fold then APPLIES every final event unconditionally
/// in deterministic topo order. Application must NOT re-validate against the running
/// merged state — that was the bug the confluence test caught: with a `member-remove`
/// ordered before a concurrent `post`, re-validation dropped the (already-final) post,
/// so the converged state depended on tiebreak order. Judging only *finality*
/// ancestry-relative keeps a concurrently-invalidated-but-past-valid event in the
/// state (chat's add-wins / message-stands), and makes every node compute the same
/// bytes from the same event set.
///
/// **Memoized (principle 1: finality is immutable).** Finality is decided ONCE per
/// event and persisted in `finality` across handler calls (`ensure_finality`), so a
/// fold is an incremental O(new events × ancestry) rather than the O(events²) rescan
/// the naive two-pass did (and `deliver_committed`, which called this per event, was
/// O(events³) — that was the wall the N-node scale test measured). Application still
/// re-folds the frontier's ancestry each call (Pass 2), which is O(events); only the
/// expensive *finality* judgement is cached.
fn fold_state_at(dag: &Dag, frontier: &[Hash], finality: &mut BTreeMap<Hash, bool>) -> Value {
    ensure_finality(dag, finality);
    apply_final(dag, frontier, finality)
}

/// Decide finality for every admitted event not already in `finality`, walking the
/// whole DAG in topo order so each event's ancestry is settled when we reach it. An
/// event is FINAL iff it validates against *its own* ancestry state (judged once,
/// independent of any linearization — principle 4) or is an inert graft (empty
/// payload). Immutable once decided, so cached entries are never revisited.
fn ensure_finality(dag: &Dag, finality: &mut BTreeMap<Hash, bool>) {
    for h in dag.ordered() {
        if finality.contains_key(&h) {
            continue;
        }
        let Some(ev) = dag.events.get(&h) else { continue };
        let ancestry_state = apply_final(dag, &deps_of(ev), finality);
        let final_here = ev.payload.is_empty() // inert graft: always "final", contributes nothing
            || sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), ancestry_state).is_ok();
        finality.insert(h, final_here);
    }
}

/// Fold only the events in `ancestors_of(frontier)` marked final in `is_final`,
/// applying each (no re-validation) in deterministic topo order.
fn apply_final(dag: &Dag, frontier: &[Hash], is_final: &BTreeMap<Hash, bool>) -> Value {
    let mut state = sm_initial_state();
    for h in dag.topo_sort(&dag.ancestors_of(frontier)) {
        if is_final.get(&h).copied().unwrap_or(false) {
            if let Some(ev) = dag.events.get(&h) {
                state = sm_apply(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), state);
            }
        }
    }
    state
}

/// Fold the admitted DAG through the composed SM and emit the Interface 3 stream:
/// a `finalized` `dag-node` for each newly-final payload event, or a fail-loud
/// `conflict` for an admitted event that is invalid against **its own ancestry
/// state** (per the memoized `finality` map, so concurrent events can't strand each
/// other). v0 is admission-final and our own `author` pre-validates, so in an honest
/// v0 mesh the conflict branch never fires; it is the safety net that makes
/// "conflict-free" a CHECKED runtime invariant against a buggy/dishonest peer.
///
/// Reads finality from the shared cache (one `ensure_finality` pass, not a per-event
/// fold) — the O(events³)→amortized-O(events) fix the scale test motivated.
fn deliver_committed(
    dag: &Dag,
    conns: &BTreeMap<String, ConnState>,
    app_id: &str,
    delivered: &mut BTreeSet<Hash>,
    finality: &mut BTreeMap<Hash, bool>,
) {
    ensure_finality(dag, finality);
    // `ordered()` (topo) only sets a stable *delivery* order; validity for each
    // event was judged against its own ancestry (the cached finality), not position.
    for h in dag.ordered() {
        let Some(ev) = dag.events.get(&h) else {
            continue;
        };
        let deps = deps_of(ev);
        if !finality.get(&h).copied().unwrap_or(false) {
            // Genuine conflict (loser inert). Surface it ONCE to every app client;
            // recompute the SM's reason lazily on this rare path.
            if delivered.insert(h) {
                let state_at = apply_final(dag, &deps, finality);
                let reason =
                    sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), state_at)
                        .err()
                        .unwrap_or_else(|| "invalid against ancestry".to_string());
                log(format!("[mesh] CONFLICT {}: {}", hex(&h), reason));
                let frame = encode_conflict(&h, &reason);
                for (cid, cs) in conns {
                    if matches!(cs.phase, Phase::Authed { .. }) {
                        let _ = tcp_send(cid.clone(), frame.clone());
                    }
                }
            }
            continue;
        }
        if ev.payload.is_empty() || !delivered.insert(h) {
            continue;
        }
        // Interface 3 `finalized`: hand app clients the whole dag-node (sm-event +
        // deps), so the executor can linearize the DAG locally.
        let frame = encode_finalized(&h, &ev.author, ev.timestamp, &deps, &ev.payload);
        for (cid, cs) in conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                let _ = tcp_send(cid.clone(), frame.clone());
            }
        }
        // A subscribed executor gets the SAME dag-node frame over the message-server
        // stream (the reference "watch events" surface). Reuses the wire encoding, so
        // TCP peers and co-located executors see one dag-node format.
        if !app_id.is_empty() {
            let _ = message_server_send(app_id.to_string(), frame.clone());
        }
    }
}

fn challenge_nonce(conn_id: &str) -> [u8; 32] {
    // Derived, not cryptographically random (predictable from timing); adequate
    // as a possession check, replace with a CSPRNG before relying on replay
    // resistance.
    let mut h = Sha256::new();
    h.update(now_ms().to_be_bytes());
    h.update(conn_id.as_bytes());
    h.finalize().into()
}
