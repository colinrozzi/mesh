//! mesh v2 — DAG-based identity + message-passing actor.
//!
//! See DESIGN.md for the protocol spec. Every event is a signed node in a
//! content-addressed DAG (parent: EventHash). State is purely derived by
//! walking the DAG and applying state-mutating ops in canonical order.
//!
//! Connection lifecycle: HELLO(pubkey) → CHALLENGE(nonce) → AUTH(sig) → ACCEPTED.
//! After accept, the client may SUBMIT signed events. Ingested events are
//! broadcast as DELIVERED to all authenticated connections (peer meshes
//! re-ingest and re-broadcast).
//!
//! Witnesses are emitted on a timer (default every 2s) referencing all events
//! received since the last witness. They cite freshly-seen events, letting the
//! network derive who-has-seen-what just by watching the DAG grow. State-
//! mutating events additionally get an inline witness so single-node consensus
//! is reached before `on_data` returns.
//!
//! Module map:
//!   event — canonical, signed binary event encoding (hand-rolled)
//!   wire  — length-prefixed frame protocol (hand-rolled)
//!   dag   — DAG storage, state derivation, finality + consensus
//!   conn  — per-connection handshake state
//!   codec — persistence of ActorState <-> JSON strings, plus hex helpers

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use sha2::{Digest, Sha256};

#[cfg(not(test))]
packr_guest::setup_guest!();

mod codec;
mod conn;
mod dag;
mod event;
mod wire;

use codec::{
    connections_from_json, connections_to_json, dag_from_json, dag_to_json, from_hex32, hex,
    peer_nodes_to_json, pending_from_json, pending_to_json,
};
use conn::{ConnState, Phase};
use dag::Dag;
use event::{Event, Hash, PubKey, GENESIS_PARENT};
use wire::{
    encode_accepted, encode_ack, encode_challenge, encode_delivered, encode_rejected,
    try_parse_frame, ParsedFrame, FRAME_AUTH, FRAME_DELIVERED, FRAME_HELLO, FRAME_SUBMIT,
};

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ActorState {
    pub listener_id: String,
    pub listen_addr: String,
    /// Mesh's own signing key, derived from init_state.node_seed. Pubkey =
    /// network root for v2-single-node. Kept hex-encoded so it round-trips
    /// through theater's value serialization without losing bytes.
    pub signing_key_hex: String,
    pub root_pubkey_hex: String,
    /// Pre-admitted peer Nodes at genesis, JSON: [[pubkey_hex, name], ...]
    pub peer_nodes_json: String,
    /// Hash of the most recent event mesh itself authored. Used as the
    /// `parent` for the next event mesh creates (Witnesses chain into mesh's
    /// own per-author timeline). Starts at GENESIS_PARENT.
    pub mesh_head_hex: String,
    /// Witness cadence in milliseconds (also the timer interval).
    pub witness_interval_ms: u64,
    /// Encoded DAG (events) — we re-hydrate the in-memory structure each call.
    pub dag_json: String,
    pub connections_json: String,
    /// Hashes of events received since the last Witness — gathered into the
    /// next emitted Witness's also_cite field.
    pub witness_pending_json: String,
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
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: actor-state, connection-id: string) -> result<actor-state, string>,
        theater:simple/tcp-client.on-data: func(state: actor-state, connection-id: string, data: list<u8>) -> result<actor-state, string>,
        theater:simple/tcp-client.on-close: func(state: actor-state, connection-id: string, reason: string) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
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

const LISTEN_ADDR: &str = "127.0.0.1:9447";
const WITNESS_TIMER_NAME: &str = "witness-tick";
const DEFAULT_WITNESS_INTERVAL_MS: u64 = 2000;

// ---- init ----

#[derive(serde::Deserialize)]
struct InitConfig {
    /// Seed material for mesh's own signing key.
    node_seed: String,
    /// The network's root pubkey. Same on all nodes participating in this
    /// network. If absent, defaults to this node's own pubkey (this node is
    /// the founding root).
    #[serde(default)]
    root_pubkey: Option<String>,
    /// Listen address (defaults to 127.0.0.1:9447 if absent).
    #[serde(default)]
    listen_addr: Option<String>,
    /// Additional Nodes pre-admitted at genesis (besides root). On any node
    /// participating in this network, this list must be identical so all nodes
    /// derive the same genesis state.
    #[serde(default)]
    peer_node_pubkeys: Vec<PeerNodeEntry>,
    /// Peer mesh endpoints to open outbound connections to on init.
    #[serde(default)]
    peer_meshes: Vec<PeerMeshEntry>,
    #[serde(default)]
    witness_interval_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct PeerNodeEntry {
    pubkey: String,
    name: String,
}

#[derive(serde::Deserialize)]
struct PeerMeshEntry {
    pubkey: String,
    address: String,
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(ActorState, ()), String> {
    log(String::from("[mesh] init (v2)"));
    let cfg: InitConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse init_state: {}", e))?
        }
        _ => return Err("missing init_state (need {\"node_seed\":\"...\"})".to_string()),
    };
    let witness_interval_ms = cfg.witness_interval_ms.unwrap_or(DEFAULT_WITNESS_INTERVAL_MS);
    let listen_addr = cfg.listen_addr.unwrap_or_else(|| LISTEN_ADDR.to_string());

    // Derive mesh's signing key + pubkey from node_seed.
    use ed25519_dalek::SigningKey;
    let mut h = Sha256::new();
    h.update(cfg.node_seed.as_bytes());
    let key_bytes: [u8; 32] = h.finalize().into();
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let self_pubkey = signing_key.verifying_key().to_bytes();
    let self_pubkey_hex = hex(&self_pubkey);
    let signing_key_hex = hex(&key_bytes);

    // Root pubkey = explicit if provided, else this node's own pubkey.
    let root_pubkey = match &cfg.root_pubkey {
        Some(s) => from_hex32(s)?,
        None => self_pubkey,
    };
    let root_pubkey_hex = hex(&root_pubkey);
    let is_root = root_pubkey == self_pubkey;

    // Parse peer Nodes and pre-admit them at genesis.
    let mut peer_nodes: Vec<(PubKey, String)> = Vec::new();
    for entry in &cfg.peer_node_pubkeys {
        peer_nodes.push((from_hex32(&entry.pubkey)?, entry.name.clone()));
    }
    let dag = Dag::new(root_pubkey, peer_nodes.clone());

    let listener_id =
        tcp_listen(listen_addr.clone()).map_err(|e| format!("listen failed: {}", e))?;
    log(format!(
        "[mesh] listening on {} (id={}); self_pubkey={}; root_pubkey={}{}; peer_nodes={}; witness_interval_ms={}",
        &listen_addr,
        listener_id,
        &self_pubkey_hex,
        &root_pubkey_hex,
        if is_root { " (self)" } else { "" },
        peer_nodes.len(),
        witness_interval_ms,
    ));

    if let Err(e) = timer_set_interval(WITNESS_TIMER_NAME.to_string(), witness_interval_ms) {
        log(format!("[mesh] set-interval failed: {}", e));
    }

    // Open outbound connections to peer meshes, run the client-side handshake,
    // and register them as authed peer connections.
    let mut conns: BTreeMap<String, ConnState> = BTreeMap::new();
    for peer in &cfg.peer_meshes {
        match open_peer_connection(&peer.address, &peer.pubkey, &signing_key) {
            Ok(conn_id) => {
                log(format!(
                    "[mesh] outbound peer connected: {} via conn {}",
                    &peer.address, conn_id
                ));
                conns.insert(conn_id, ConnState::authed(peer.pubkey.clone()));
            }
            Err(e) => {
                log(format!("[mesh] outbound peer {} failed: {}", &peer.address, e));
            }
        }
    }

    Ok((
        ActorState {
            listener_id,
            listen_addr,
            signing_key_hex,
            root_pubkey_hex,
            peer_nodes_json: peer_nodes_to_json(&peer_nodes),
            mesh_head_hex: hex(&GENESIS_PARENT),
            witness_interval_ms,
            dag_json: dag_to_json(&dag),
            connections_json: connections_to_json(&conns),
            witness_pending_json: "[]".to_string(),
        },
        (),
    ))
}

/// Open an outbound TCP connection to a peer mesh, run the client-side
/// handshake (HELLO → CHALLENGE → AUTH → ACCEPTED), then switch to active mode
/// so future on-data callbacks handle the event stream.
fn open_peer_connection(
    address: &str,
    expected_pubkey_hex: &str,
    signing_key: &ed25519_dalek::SigningKey,
) -> Result<String, String> {
    let conn_id =
        tcp_connect(address.to_string()).map_err(|e| format!("connect {}: {}", address, e))?;
    let pk = signing_key.verifying_key().to_bytes();

    // HELLO(pubkey)
    let hello = wire::encode_frame(FRAME_HELLO, &pk);
    tcp_send(conn_id.clone(), hello).map_err(|e| format!("send HELLO: {}", e))?;

    // Read CHALLENGE
    let (kind, nonce) = recv_one_frame(&conn_id)?;
    if kind != wire::FRAME_CHALLENGE || nonce.len() != 32 {
        let _ = tcp_close(conn_id);
        return Err(format!("expected CHALLENGE(nonce), got kind={:#x}", kind));
    }

    // AUTH(sig over nonce)
    use ed25519_dalek::Signer;
    let sig = signing_key.sign(&nonce).to_bytes();
    let auth = wire::encode_frame(FRAME_AUTH, &sig);
    tcp_send(conn_id.clone(), auth).map_err(|e| format!("send AUTH: {}", e))?;

    // Read ACCEPTED
    let (kind, payload) = recv_one_frame(&conn_id)?;
    if kind != wire::FRAME_ACCEPTED {
        let _ = tcp_close(conn_id);
        return Err(format!(
            "peer rejected: kind={:#x} payload={:?}",
            kind,
            core::str::from_utf8(&payload).unwrap_or("<binary>")
        ));
    }
    // (peer's reported head_hash is in payload — we don't need to verify it)
    let _ = expected_pubkey_hex; // (future: verify peer's pubkey matches)

    // Switch to active mode so on-data fires for live event flow.
    tcp_set_active(conn_id.clone(), "active".to_string())
        .map_err(|e| format!("set-active: {}", e))?;

    Ok(conn_id)
}

/// Read one complete length-prefixed frame from a connection. Used during the
/// outbound handshake — the caller is doing blocking I/O so passive mode is fine.
fn recv_one_frame(conn_id: &str) -> Result<(u8, Vec<u8>), String> {
    let mut buf = Vec::with_capacity(4);
    while buf.len() < 4 {
        let chunk = tcp_receive(conn_id.to_string(), 4 - buf.len() as u32)
            .map_err(|e| format!("receive len: {}", e))?;
        if chunk.is_empty() {
            return Err("connection closed during frame length read".to_string());
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
            return Err("connection closed during frame body read".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    let kind = body[0];
    let payload = body[1..].to_vec();
    Ok((kind, payload))
}

// ---- connection lifecycle ----

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(state: ActorState, conn_id: String) -> Result<(ActorState, ()), String> {
    if let Err(e) = tcp_activate(conn_id.clone()) {
        log(format!("[mesh] activate {} failed: {}", conn_id, e));
        let _ = tcp_close(conn_id);
        return Ok((state, ()));
    }
    if let Err(e) = tcp_set_active(conn_id.clone(), "active".to_string()) {
        log(format!("[mesh] set-active {} failed: {}", conn_id, e));
        let _ = tcp_close(conn_id);
        return Ok((state, ()));
    }
    let mut conns = connections_from_json(&state.connections_json);
    conns.insert(conn_id.clone(), ConnState::awaiting_hello());
    log(format!("[mesh] conn {} opened", conn_id));
    Ok((
        ActorState {
            connections_json: connections_to_json(&conns),
            ..state
        },
        (),
    ))
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(state: ActorState, conn_id: String, data: Vec<u8>) -> Result<(ActorState, ()), String> {
    let mut conns = connections_from_json(&state.connections_json);
    let mut dag = dag_from_json(&state.dag_json, &state.root_pubkey_hex, &state.peer_nodes_json)?;
    let mut witness_pending: Vec<Hash> = pending_from_json(&state.witness_pending_json);

    // Pull this connection out of the map so we can mutate it freely; it's
    // re-inserted at the end unless the connection is being closed.
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
    // Mesh's own per-author head advances as it inline-witnesses state changes;
    // we thread the running value here and sweep it into ActorState after the loop.
    let mut mesh_head = state.mesh_head_hex.clone();

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
        let kept: Vec<u8> = conn_state.recv_buf()[frame.total_len..].to_vec();
        *conn_state.recv_buf_mut() = kept;

        match conn_state.phase.clone() {
            Phase::AwaitingHello => match step_hello(&conn_id, &frame, &dag) {
                Step::Advance(phase) => conn_state.phase = phase,
                Step::Close => {
                    should_close = true;
                    break;
                }
            },
            Phase::AwaitingAuth { pubkey_hex, nonce_hex } => {
                match step_auth(&conn_id, &frame, &pubkey_hex, &nonce_hex, &dag)? {
                    Step::Advance(phase) => conn_state.phase = phase,
                    Step::Close => {
                        should_close = true;
                        break;
                    }
                }
            }
            Phase::Authed { pubkey_hex } => {
                if let Some(new_head) = handle_authed_frame(
                    &mut dag,
                    &conns,
                    &conn_id,
                    &pubkey_hex,
                    &frame,
                    &mut witness_pending,
                    &state.signing_key_hex,
                    &mesh_head,
                )? {
                    mesh_head = new_head;
                }
            }
        }
    }

    if !should_close {
        conns.insert(conn_id.clone(), conn_state);
    }
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            connections_json: connections_to_json(&conns),
            witness_pending_json: pending_to_json(&witness_pending),
            mesh_head_hex: mesh_head,
            ..state
        },
        (),
    ))
}

/// Result of handling one frame during the handshake.
enum Step {
    /// Move the connection to a new phase.
    Advance(Phase),
    /// Reject and close the connection.
    Close,
}

/// Handle a HELLO frame: verify the claimed pubkey is a member, then issue a
/// challenge and advance to AwaitingAuth.
fn step_hello(conn_id: &str, frame: &ParsedFrame, dag: &Dag) -> Step {
    if frame.kind != FRAME_HELLO || frame.payload.len() != 32 {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected HELLO(pubkey)"));
        let _ = tcp_close(conn_id.to_string());
        return Step::Close;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&frame.payload);
    if !dag.consensus_state().is_member(&pk) {
        let _ = tcp_send(
            conn_id.to_string(),
            encode_rejected(&format!("not a member: {}", hex(&pk))),
        );
        let _ = tcp_close(conn_id.to_string());
        return Step::Close;
    }
    let nonce = challenge_nonce(conn_id);
    let _ = tcp_send(conn_id.to_string(), encode_challenge(&nonce));
    Step::Advance(Phase::AwaitingAuth {
        pubkey_hex: hex(&pk),
        nonce_hex: hex(&nonce),
    })
}

/// Handle an AUTH frame: verify the signature over the issued nonce, then
/// accept (reporting the consensus head) and advance to Authed.
fn step_auth(
    conn_id: &str,
    frame: &ParsedFrame,
    pubkey_hex: &str,
    nonce_hex: &str,
    dag: &Dag,
) -> Result<Step, String> {
    if frame.kind != FRAME_AUTH || frame.payload.len() != 64 {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected AUTH(sig[64])"));
        let _ = tcp_close(conn_id.to_string());
        return Ok(Step::Close);
    }
    let pk = from_hex32(pubkey_hex)?;
    let nonce = from_hex32(nonce_hex)?;
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&frame.payload);
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let vk = VerifyingKey::from_bytes(&pk).map_err(|e| format!("bad pubkey at auth: {}", e))?;
    if let Err(e) = vk.verify(&nonce, &Signature::from_bytes(&sig)) {
        let _ = tcp_send(
            conn_id.to_string(),
            encode_rejected(&format!("auth failed: {}", e)),
        );
        let _ = tcp_close(conn_id.to_string());
        return Ok(Step::Close);
    }
    let _ = tcp_send(conn_id.to_string(), encode_accepted(&dag.consensus_state_head()));
    log(format!("[mesh] conn {} authenticated as {}", conn_id, pubkey_hex));
    Ok(Step::Advance(Phase::Authed {
        pubkey_hex: pubkey_hex.to_string(),
    }))
}

/// Handle a post-handshake frame. Two kinds are accepted:
///   SUBMIT     — the authed client submits a signed event for ingestion
///   DELIVERED  — a peer mesh forwards an event for us to ingest
/// Both get ingested + re-broadcast, but only SUBMIT gets an ACK back, and
/// SUBMIT additionally requires the event's author to match the connection's
/// authenticated identity (DELIVERED carries events from many authors, so we
/// trust the signature rather than the connection).
///
/// Returns `Some(new_mesh_head)` if an inline Witness was authored.
#[allow(clippy::too_many_arguments)]
fn handle_authed_frame(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    pubkey_hex: &str,
    frame: &ParsedFrame,
    witness_pending: &mut Vec<Hash>,
    signing_key_hex: &str,
    mesh_head_hex: &str,
) -> Result<Option<String>, String> {
    let is_submit = frame.kind == FRAME_SUBMIT;
    let is_delivered = frame.kind == FRAME_DELIVERED;
    if !is_submit && !is_delivered {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected SUBMIT or DELIVERED"));
        return Ok(None);
    }

    let event_obj = match Event::decode(&frame.payload) {
        Ok(e) => e,
        Err(e) => {
            if is_submit {
                let _ = tcp_send(
                    conn_id.to_string(),
                    encode_ack(&[0u8; 32], false, &format!("decode: {}", e)),
                );
            } else {
                log(format!("[mesh] DELIVERED decode failed: {}", e));
            }
            return Ok(None);
        }
    };

    if is_submit && hex(&event_obj.author) != pubkey_hex {
        let _ = tcp_send(
            conn_id.to_string(),
            encode_ack(
                &event_obj.event_hash(),
                false,
                "author doesn't match authenticated identity",
            ),
        );
        return Ok(None);
    }

    let event_hash = event_obj.event_hash();
    let is_state_mutating = Dag::is_state_mutating(&event_obj.op);
    match dag.ingest(event_obj.clone()) {
        Ok(true) => {
            if is_submit {
                let _ = tcp_send(conn_id.to_string(), encode_ack(&event_hash, true, ""));
            }
            witness_pending.push(event_hash);
            // Broadcast to all other authed connections; the submitter (pulled
            // out of `conns` during on-data) gets it via the explicit re-send.
            broadcast_delivered(conns, conn_id, &event_obj.encode());

            // State changes get an inline witness so single-node consensus is
            // reached before on_data returns — the submitter can re-handshake
            // and see their change applied without waiting for the timer tick.
            if is_state_mutating {
                if let Some(witness) =
                    mesh_author_witness(dag, signing_key_hex, mesh_head_hex, witness_pending)?
                {
                    broadcast_delivered(conns, "", &witness.encode());
                    witness_pending.clear();
                    return Ok(Some(hex(&witness.event_hash())));
                }
            }
            Ok(None)
        }
        Ok(false) => {
            if is_submit {
                let _ = tcp_send(
                    conn_id.to_string(),
                    encode_ack(&event_hash, true, "buffered pending parent"),
                );
            }
            Ok(None)
        }
        Err(e) => {
            if is_submit {
                let _ = tcp_send(conn_id.to_string(), encode_ack(&event_hash, false, &e));
            } else {
                log(format!("[mesh] DELIVERED ingest failed: {}", e));
            }
            Ok(None)
        }
    }
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(state: ActorState, conn_id: String, reason: String) -> Result<(ActorState, ()), String> {
    log(format!("[mesh] conn {} closed: {}", conn_id, reason));
    let mut conns = connections_from_json(&state.connections_json);
    conns.remove(&conn_id);
    Ok((
        ActorState {
            connections_json: connections_to_json(&conns),
            ..state
        },
        (),
    ))
}

// ---- timer-driven witness emission ----

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: ActorState, timer_name: String) -> Result<(ActorState, ()), String> {
    if timer_name != WITNESS_TIMER_NAME {
        return Ok((state, ()));
    }
    let pending: Vec<Hash> = pending_from_json(&state.witness_pending_json);
    if pending.is_empty() {
        return Ok((state, ()));
    }
    let mut dag = dag_from_json(&state.dag_json, &state.root_pubkey_hex, &state.peer_nodes_json)?;
    let conns = connections_from_json(&state.connections_json);
    let Some(witness) =
        mesh_author_witness(&mut dag, &state.signing_key_hex, &state.mesh_head_hex, &pending)?
    else {
        return Ok((state, ()));
    };
    let w_hash = witness.event_hash();
    log(format!(
        "[mesh] witness-tick: emitted Witness citing {} events (hash={}...)",
        pending.len(),
        &hex(&w_hash)[..16]
    ));
    broadcast_delivered(&conns, "", &witness.encode());
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            mesh_head_hex: hex(&w_hash),
            witness_pending_json: "[]".to_string(),
            ..state
        },
        (),
    ))
}

/// Build, sign, and ingest a Witness event covering `cite_hashes`, parented on
/// `mesh_head_hex`. Returns Ok(Some(event)) on success, Ok(None) if there's
/// nothing to cite.
fn mesh_author_witness(
    dag: &mut Dag,
    signing_key_hex: &str,
    mesh_head_hex: &str,
    cite_hashes: &[Hash],
) -> Result<Option<Event>, String> {
    if cite_hashes.is_empty() {
        return Ok(None);
    }
    use ed25519_dalek::{Signer, SigningKey};
    let key_bytes = from_hex32(signing_key_hex)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let author = signing_key.verifying_key().to_bytes();
    let parent = from_hex32(mesh_head_hex)?;
    let op = event::Op::Witness { also_cite: cite_hashes.to_vec() };
    let signing_hash = Event::signing_hash(&parent, &author, &op);
    let signature = signing_key.sign(&signing_hash).to_bytes();
    let witness = Event { parent, author, op, signature };
    match dag.ingest(witness.clone()) {
        Ok(true) => Ok(Some(witness)),
        Ok(false) => {
            log(String::from(
                "[mesh] mesh_author_witness: ingest buffered — should not happen for self-signed",
            ));
            Ok(None)
        }
        Err(e) => Err(format!("mesh_author_witness ingest: {}", e)),
    }
}

/// Send a DELIVERED frame to every authed connection. `submitter` is excluded
/// from the iteration (it was pulled out of `conns` during on-data) and gets an
/// explicit re-send unless empty — empty means there's no submitter, e.g. when
/// mesh itself emits a Witness via the timer tick.
fn broadcast_delivered(conns: &BTreeMap<String, ConnState>, submitter: &str, event_bytes: &[u8]) {
    let frame = encode_delivered(event_bytes);
    for (cid, cs) in conns {
        if matches!(cs.phase, Phase::Authed { .. }) {
            let _ = tcp_send(cid.clone(), frame.clone());
        }
    }
    if !submitter.is_empty() {
        let _ = tcp_send(submitter.to_string(), frame);
    }
}

/// Per-connection challenge value: sha256(now_ms ‖ conn_id). NOTE: this is
/// derived, not cryptographically random — it's predictable from timing. It's
/// adequate as a possession/liveness check (the client must sign it with the
/// claimed key) but should be replaced with a real CSPRNG nonce before relying
/// on it for replay resistance.
fn challenge_nonce(conn_id: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(now_ms().to_be_bytes());
    h.update(conn_id.as_bytes());
    h.finalize().into()
}
