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
    events_to_json, from_hex32, hashes_from_json, hashes_to_json, hex,
};
use conn::{ConnState, Phase};
use dag::Dag;
use event::{Event, Hash, PubKey};
use wire::{
    decode_hashes, encode_ack, encode_auth, encode_challenge, encode_deliver, encode_hashes,
    encode_hello, encode_notify, encode_rejected, try_parse_frame, ParsedFrame, FRAME_ACCEPTED,
    FRAME_AUTH, FRAME_CHALLENGE, FRAME_DELIVER, FRAME_FRONTIER, FRAME_HELLO, FRAME_SUBMIT,
    FRAME_WANT,
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
    pub connections_json: String,
    /// The co-located app actor's id (theater actor-id) to `send` delivered
    /// payloads to, set by a Register command. Empty = no app subscribed.
    pub app_id: String,
    /// True once the one-shot Ready signal has been sent to the app. Prevents
    /// re-emitting it.
    pub ready_sent: bool,
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
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: actor-state, connection-id: string) -> result<actor-state, string>,
        theater:simple/tcp-client.on-data: func(state: actor-state, connection-id: string, data: list<u8>) -> result<actor-state, string>,
        theater:simple/tcp-client.on-close: func(state: actor-state, connection-id: string, reason: string) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
        theater:simple/message-server-client.handle-request: func(state: actor-state, params: tuple<string, list<u8>>) -> result<tuple<actor-state, tuple<option<list<u8>>>>, string>,
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
#[import_from("state-machine", name = "initial-state")]
fn sm_initial_state() -> Vec<u8>;
#[import_from("state-machine", name = "validate")]
fn sm_validate(
    id: Vec<u8>,
    author: Vec<u8>,
    timestamp: u64,
    payload: Vec<u8>,
    state: Vec<u8>,
) -> Result<bool, String>;
#[import_from("state-machine", name = "apply")]
fn sm_apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8>;
#[import_from("state-machine", name = "members")]
fn sm_members(state: Vec<u8>) -> Vec<Vec<u8>>;

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
                );
            }
        }
    }

    if !should_close {
        conns.insert(conn_id.clone(), conn_state);
    }
    // Deliver any newly-admitted messages to TCP clients and the co-located app.
    deliver_committed(&dag, &conns, &state.app_id, &mut delivered);
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
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

/// Periodic tick: a delivery safety-net + one-shot Ready. No heartbeat, no
/// eviction — admission-final finality means there is nothing to pump or reap.
#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: ActorState, _timer_name: String) -> Result<(ActorState, ()), String> {
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    let conns = connections_from_json(&state.connections_json);
    let mut delivered: BTreeSet<Hash> =
        hashes_from_json(&state.delivered_json).into_iter().collect();

    deliver_committed(&dag, &conns, &state.app_id, &mut delivered);
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
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
    let mut app_id = state.app_id.clone();

    let ack = match api::decode_command(&body) {
        Some(api::Command::Submit(payload)) => {
            let (h, result) = run_command(&mut dag, &conns, &signing_key, self_head, payload);
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

    deliver_committed(&dag, &conns, &app_id, &mut delivered);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
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
            author_and_broadcast(dag, conns, conn_id, signing_key, self_head, frame.payload.clone())
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
    let encoded = event.encode();
    let missing = missing_deps(dag, &event); // compute before `ingest` consumes it

    match dag.ingest_admitted(event) {
        Ok(admitted) if !admitted.is_empty() => {
            // Forward to every other peer (the source is excluded — it's not in
            // `conns` right now, having been removed for the duration of on-data).
            broadcast(conns, from_conn, &encode_deliver(&encoded));
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
fn current_state(dag: &Dag) -> Vec<u8> {
    fold_state_at(dag, &all_heads(dag))
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
        let state = current_state(dag);
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
) -> Option<Hash> {
    match author_event(dag, signing_key, self_head, payload) {
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
) -> (Option<Hash>, Result<Hash, String>) {
    match author_event(dag, signing_key, self_head, payload) {
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

/// Fold the SM over the causal past of `frontier` (its ancestry, frontier
/// included) and return the resulting state — the **ancestry-relative** state an
/// event at that frontier is validated against (DESIGN-rsm.md principle 3).
///
/// This is what makes the conflict-free story actually hold. The chat-SM's
/// `validate` is "author is a member *of the ancestry state*"; folding only the
/// event's own causal past means a *concurrent* `member-remove` (not in the past)
/// isn't seen, so a message whose author was concurrently removed still validates
/// and stands — chat is conflict-free *because* validation is ancestry-relative.
/// Folding the whole global set instead (as the step-2 draft did) would strand it.
///
/// NOTE (perf): O(ancestry) SM calls per invocation, and `deliver_committed` calls
/// it once per event → O(events²) cross-component calls per pass. Correct but
/// unoptimized — memoize a per-event state cache before any real load (the design
/// signs off on "re-fold now, optimize later").
fn fold_state_at(dag: &Dag, frontier: &[Hash]) -> Vec<u8> {
    let ancestry = dag.ancestors_of(frontier);
    let mut state = sm_initial_state();
    for h in dag.topo_sort(&ancestry) {
        let Some(ev) = dag.events.get(&h) else { continue };
        if sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), state.clone())
            .is_ok()
        {
            state = sm_apply(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), state);
        }
    }
    state
}

/// Fold the admitted DAG through the composed SM and deliver each newly-final
/// payload event to the app. v0 is admission-final: an event is final (deliverable)
/// the moment its `validate` passes against **its own ancestry state** (via
/// `fold_state_at`), never the global fold — so concurrent events can't strand it.
/// A non-validating event is skipped here; explicit conflict/stranded *surfacing*
/// to the app is step 4.
fn deliver_committed(
    dag: &Dag,
    conns: &BTreeMap<String, ConnState>,
    app_id: &str,
    delivered: &mut BTreeSet<Hash>,
) {
    // `ordered()` (topo) only sets a stable *delivery* order; validity for each
    // event is judged against its own ancestry, not this running position.
    for h in dag.ordered() {
        let Some(ev) = dag.events.get(&h) else {
            continue;
        };
        let state_at = fold_state_at(dag, &deps_of(ev));
        if sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, ev.payload.clone(), state_at)
            .is_err()
        {
            continue; // stranded — step 4 will surface the conflict
        }
        if ev.payload.is_empty() || !delivered.insert(h) {
            continue;
        }
        // TCP app clients (test harness) get a NOTIFY frame...
        let frame = encode_notify(&ev.author, &ev.payload);
        for (cid, cs) in conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                let _ = tcp_send(cid.clone(), frame.clone());
            }
        }
        // ...the co-located app actor gets a message-server delivery.
        if !app_id.is_empty() {
            let _ = message_server_send(app_id.to_string(), api::encode_delivery(&ev.author, &ev.payload));
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
