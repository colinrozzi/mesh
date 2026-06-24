//! mesh v2 — DAG-based identity + message-passing actor.
//!
//! See DESIGN.md for the protocol spec. Every event is a signed node in
//! a content-addressed DAG (parent: EventHash). State is purely derived
//! by walking the DAG and applying state-mutating ops in canonical order.
//!
//! Connection lifecycle: HELLO(pubkey) → CHALLENGE(nonce) → AUTH(sig) → ACCEPTED.
//! After accept, the client may SUBMIT signed events. State-mutating events
//! get broadcast as DELIVERED to all authenticated connections. Send events
//! are routed to the recipient's connection (if online).
//!
//! Witnesses are emitted on a timer (default every 2s) referencing all
//! events received since the last witness. They cite freshly-seen events,
//! letting the network derive who-has-seen-what just by watching the DAG grow.

#![no_std]
extern crate alloc;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use sha2::{Digest, Sha256};

packr_guest::setup_guest!();

mod dag;
mod event;
mod state;
mod wire;

use dag::{hex, Dag};
use event::{Event, Op, PubKey, GENESIS_PARENT};
use wire::{
    encode_accepted, encode_ack, encode_challenge, encode_delivered, encode_rejected,
    try_parse_frame, FRAME_AUTH, FRAME_HELLO, FRAME_SUBMIT,
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
    /// Encoded DAG (events_json) + connections_json. We re-hydrate the
    /// in-memory structures on each call.
    pub dag_json: String,
    pub connections_json: String,
    /// Hashes of events received since the last Witness — gathered into
    /// the next emitted Witness's also_cite field.
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
    /// network. If absent, defaults to this node's own pubkey (this node
    /// is the founding root).
    #[serde(default)]
    root_pubkey: Option<String>,
    /// Listen address (defaults to 127.0.0.1:9447 if absent).
    #[serde(default)]
    listen_addr: Option<String>,
    /// Additional Nodes pre-admitted at genesis (besides root). On any node
    /// participating in this network, this list must be identical so all
    /// nodes derive the same genesis state.
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
        Some(s) => parse_hex_pubkey(s)?,
        None => self_pubkey,
    };
    let root_pubkey_hex = hex(&root_pubkey);
    let is_root = root_pubkey == self_pubkey;

    // Parse peer Nodes and pre-admit them at genesis.
    let mut peer_nodes: Vec<(PubKey, String)> = Vec::new();
    for entry in &cfg.peer_node_pubkeys {
        let pk = parse_hex_pubkey(&entry.pubkey)?;
        peer_nodes.push((pk, entry.name.clone()));
    }
    let dag = Dag::new_with_peers(root_pubkey, peer_nodes.clone());
    let dag_json = serialize_dag(&dag);
    let peer_nodes_json = serde_json::to_string(
        &peer_nodes
            .iter()
            .map(|(pk, name)| (hex(pk), name.clone()))
            .collect::<Vec<_>>(),
    ).unwrap_or_else(|_| "[]".to_string());

    let listener_id = tcp_listen(listen_addr.clone())
        .map_err(|e| format!("listen failed: {}", e))?;
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

    // Open outbound connections to peer meshes, run handshake from client side,
    // and register them in `connections_json` as authed peer connections.
    let mut conns: BTreeMap<String, ConnState> = BTreeMap::new();
    for peer in &cfg.peer_meshes {
        match open_peer_connection(
            &peer.address,
            &peer.pubkey,
            &signing_key,
        ) {
            Ok(conn_id) => {
                log(format!(
                    "[mesh] outbound peer connected: {} via conn {}",
                    &peer.address, conn_id
                ));
                conns.insert(
                    conn_id,
                    ConnState {
                        phase: Phase::Authed { pubkey_hex: peer.pubkey.clone() },
                        buf: Vec::new(),
                    },
                );
            }
            Err(e) => {
                log(format!(
                    "[mesh] outbound peer {} failed: {}",
                    &peer.address, e
                ));
            }
        }
    }

    Ok((
        ActorState {
            listener_id,
            listen_addr,
            signing_key_hex,
            root_pubkey_hex,
            peer_nodes_json,
            mesh_head_hex: hex(&GENESIS_PARENT),
            witness_interval_ms,
            dag_json,
            connections_json: ser_connections(&conns),
            witness_pending_json: "[]".to_string(),
        },
        (),
    ))
}

/// Open an outbound TCP connection to a peer mesh, run the client-side
/// handshake (HELLO → CHALLENGE → AUTH → ACCEPTED), then switch to active
/// mode so future on-data callbacks handle the event stream.
fn open_peer_connection(
    address: &str,
    expected_pubkey_hex: &str,
    signing_key: &ed25519_dalek::SigningKey,
) -> Result<String, String> {
    let conn_id = tcp_connect(address.to_string())
        .map_err(|e| format!("connect {}: {}", address, e))?;
    let pk = signing_key.verifying_key().to_bytes();

    // HELLO(pubkey)
    let hello = wire::encode_frame(wire::FRAME_HELLO, &pk);
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
    let auth = wire::encode_frame(wire::FRAME_AUTH, &sig);
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

/// Read one complete length-prefixed frame from a connection. Used during
/// outbound handshake — caller is doing blocking I/O so passive mode is fine.
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
fn handle_connection(
    state: ActorState,
    conn_id: String,
) -> Result<(ActorState, ()), String> {
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
    let mut conns = deser_connections(&state.connections_json);
    conns.insert(conn_id.clone(), ConnState::awaiting_hello());
    let new_state = ActorState {
        connections_json: ser_connections(&conns),
        ..state
    };
    log(format!("[mesh] conn {} opened", conn_id));
    Ok((new_state, ()))
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(
    state: ActorState,
    conn_id: String,
    data: Vec<u8>,
) -> Result<(ActorState, ()), String> {
    let mut conns = deser_connections(&state.connections_json);
    let mut dag = deser_dag(&state.dag_json, &state.root_pubkey_hex, &state.peer_nodes_json)?;
    let mut witness_pending: Vec<event::Hash> = deser_pending(&state.witness_pending_json);

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
    let mut state_mesh_head_override: Option<String> = None;

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
        let consumed = frame.total_len;
        let kept: Vec<u8> = conn_state.recv_buf()[consumed..].to_vec();
        *conn_state.recv_buf_mut() = kept;

        let phase_snapshot = conn_state.phase.clone();
        match phase_snapshot {
            Phase::AwaitingHello => {
                if frame.kind != FRAME_HELLO || frame.payload.len() != 32 {
                    let _ = tcp_send(conn_id.clone(), encode_rejected("expected HELLO(pubkey)"));
                    let _ = tcp_close(conn_id.clone());
                    should_close = true;
                    break;
                }
                let mut pk = [0u8; 32];
                pk.copy_from_slice(&frame.payload);
                let current_state = dag.consensus_state();
                if !current_state.is_member(&pk) {
                    let _ = tcp_send(
                        conn_id.clone(),
                        encode_rejected(&format!("not a member: {}", hex(&pk))),
                    );
                    let _ = tcp_close(conn_id.clone());
                    should_close = true;
                    break;
                }
                let nonce = random_nonce(&conn_id);
                let _ = tcp_send(conn_id.clone(), encode_challenge(&nonce));
                conn_state.phase = Phase::AwaitingAuth {
                    pubkey_hex: hex(&pk),
                    nonce_hex: hex(&nonce),
                };
            }
            Phase::AwaitingAuth { pubkey_hex, nonce_hex } => {
                if frame.kind != FRAME_AUTH || frame.payload.len() != 64 {
                    let _ = tcp_send(conn_id.clone(), encode_rejected("expected AUTH(sig[64])"));
                    let _ = tcp_close(conn_id.clone());
                    should_close = true;
                    break;
                }
                let pk = parse_hex_pubkey(&pubkey_hex)?;
                let nonce = parse_hex32(&nonce_hex)?;
                let mut sig = [0u8; 64];
                sig.copy_from_slice(&frame.payload);
                use ed25519_dalek::{Signature, Verifier, VerifyingKey};
                let vk = VerifyingKey::from_bytes(&pk)
                    .map_err(|e| format!("bad pubkey at auth: {}", e))?;
                let sigobj = Signature::from_bytes(&sig);
                if let Err(e) = vk.verify(&nonce, &sigobj) {
                    let _ = tcp_send(
                        conn_id.clone(),
                        encode_rejected(&format!("auth failed: {}", e)),
                    );
                    let _ = tcp_close(conn_id.clone());
                    should_close = true;
                    break;
                }
                let head = dag.consensus_state_head();
                let _ = tcp_send(conn_id.clone(), encode_accepted(&head));
                log(format!(
                    "[mesh] conn {} authenticated as {}",
                    conn_id, pubkey_hex
                ));
                conn_state.phase = Phase::Authed { pubkey_hex };
            }
            Phase::Authed { pubkey_hex } => {
                // Two frame kinds are accepted post-handshake:
                //   SUBMIT     — client submits a signed event for ingestion
                //   DELIVERED  — peer mesh forwards an event for us to ingest
                // Both end up ingested + re-broadcast to other peers, but only
                // SUBMIT gets an ACK back to the sender.
                let is_submit = frame.kind == FRAME_SUBMIT;
                let is_delivered = frame.kind == wire::FRAME_DELIVERED;
                if !is_submit && !is_delivered {
                    let _ = tcp_send(
                        conn_id.clone(),
                        encode_rejected("expected SUBMIT or DELIVERED"),
                    );
                    continue;
                }
                let pk_hex = pubkey_hex.clone();
                let event_obj = match Event::decode(&frame.payload) {
                    Ok(e) => e,
                    Err(e) => {
                        if is_submit {
                            let zero = [0u8; 32];
                            let _ = tcp_send(
                                conn_id.clone(),
                                encode_ack(&zero, false, &format!("decode: {}", e)),
                            );
                        } else {
                            log(format!("[mesh] DELIVERED decode failed: {}", e));
                        }
                        continue;
                    }
                };
                // For SUBMIT, author must match authenticated identity. For
                // DELIVERED, the peer is forwarding events from various authors
                // — we trust the signature, not the connection identity.
                if is_submit && hex(&event_obj.author) != pk_hex {
                    let _ = tcp_send(
                        conn_id.clone(),
                        encode_ack(
                            &event_obj.event_hash(),
                            false,
                            "author doesn't match authenticated identity",
                        ),
                    );
                    continue;
                }
                let event_hash = event_obj.event_hash();
                let is_state_mutating = Dag::is_state_mutating(&event_obj.op);
                match dag.ingest(event_obj.clone()) {
                    Ok(true) => {
                        if is_submit {
                            let _ = tcp_send(conn_id.clone(), encode_ack(&event_hash, true, ""));
                        }
                        witness_pending.push(event_hash);

                        // Broadcast the ingested event to all authed connections.
                        let encoded = event_obj.encode();
                        broadcast_delivered(&conns, &conn_id, &encoded);

                        // For state-mutating events, mesh inline-emits a Witness so
                        // consensus is reached immediately (single-Node case) — the
                        // submitter can then re-handshake and have their state
                        // change reflected without waiting for the periodic tick.
                        if is_state_mutating {
                            if let Some(witness_event) = mesh_author_witness(
                                &mut dag,
                                &state.signing_key_hex,
                                &state.mesh_head_hex,
                                &witness_pending,
                            )? {
                                let w_hash = witness_event.event_hash();
                                let w_encoded = witness_event.encode();
                                broadcast_delivered(&conns, "", &w_encoded);
                                // The mesh_head advances; remember in a local var, sweep into
                                // the final ActorState after the loop.
                                state_mesh_head_override = Some(hex(&w_hash));
                                witness_pending.clear();
                            }
                        }
                    }
                    Ok(false) => {
                        if is_submit {
                            let _ = tcp_send(
                                conn_id.clone(),
                                encode_ack(&event_hash, true, "buffered pending parent"),
                            );
                        }
                    }
                    Err(e) => {
                        if is_submit {
                            let _ = tcp_send(
                                conn_id.clone(),
                                encode_ack(&event_hash, false, &e),
                            );
                        } else {
                            log(format!("[mesh] DELIVERED ingest failed: {}", e));
                        }
                    }
                }
            }
        }
    }

    if !should_close {
        conns.insert(conn_id.clone(), conn_state);
    }
    let mesh_head_hex = state_mesh_head_override.unwrap_or(state.mesh_head_hex.clone());
    let new = ActorState {
        dag_json: serialize_dag(&dag),
        connections_json: ser_connections(&conns),
        witness_pending_json: ser_pending(&witness_pending),
        mesh_head_hex,
        ..state
    };
    Ok((new, ()))
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(
    state: ActorState,
    conn_id: String,
    reason: String,
) -> Result<(ActorState, ()), String> {
    log(format!("[mesh] conn {} closed: {}", conn_id, reason));
    let mut conns = deser_connections(&state.connections_json);
    conns.remove(&conn_id);
    Ok((
        ActorState {
            connections_json: ser_connections(&conns),
            ..state
        },
        (),
    ))
}

// ---- timer-driven witness emission ----
//
// On every tick, if there are pending events to attest, the actor would
// emit a Witness event citing them. v2 phase 1 keeps this as a stub: the
// mesh itself doesn't have a keypair (it's a broker, not a member), so
// emitting Witnesses requires future design — either give the mesh its
// own identity, or have members emit witnesses themselves.
//
// For now: clear the pending list each tick. The next implementation
// phase will resolve who-emits-witnesses.

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(
    state: ActorState,
    timer_name: String,
) -> Result<(ActorState, ()), String> {
    if timer_name != WITNESS_TIMER_NAME {
        return Ok((state, ()));
    }
    let pending: Vec<event::Hash> = deser_pending(&state.witness_pending_json);
    if pending.is_empty() {
        return Ok((state, ()));
    }
    let mut dag = deser_dag(&state.dag_json, &state.root_pubkey_hex, &state.peer_nodes_json)?;
    let conns = deser_connections(&state.connections_json);
    let Some(witness_event) = mesh_author_witness(
        &mut dag,
        &state.signing_key_hex,
        &state.mesh_head_hex,
        &pending,
    )?
    else {
        return Ok((state, ()));
    };
    let w_hash = witness_event.event_hash();
    log(format!(
        "[mesh] witness-tick: emitted Witness citing {} events (hash={}...)",
        pending.len(),
        &hex(&w_hash)[..16]
    ));
    broadcast_delivered(&conns, "", &witness_event.encode());
    let new = ActorState {
        dag_json: serialize_dag(&dag),
        mesh_head_hex: hex(&w_hash),
        witness_pending_json: "[]".to_string(),
        ..state
    };
    Ok((new, ()))
}

/// Build, sign, ingest a Witness event covering `cite_hashes`, parented
/// on `mesh_head_hex`. Returns Ok(Some(event)) on success, Ok(None) if
/// there's nothing to cite (no pending events).
fn mesh_author_witness(
    dag: &mut Dag,
    signing_key_hex: &str,
    mesh_head_hex: &str,
    cite_hashes: &[event::Hash],
) -> Result<Option<event::Event>, String> {
    if cite_hashes.is_empty() {
        return Ok(None);
    }
    use ed25519_dalek::{Signer, SigningKey};
    let key_bytes = parse_hex32(signing_key_hex)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let author_pk = signing_key.verifying_key().to_bytes();
    let parent = parse_hex32(mesh_head_hex)?;
    let op = event::Op::Witness { also_cite: cite_hashes.to_vec() };
    let signing_hash = event::Event::signing_hash(&parent, &author_pk, &op);
    let sig = signing_key.sign(&signing_hash).to_bytes();
    let witness_event = event::Event {
        parent,
        author: author_pk,
        op,
        signature: sig,
    };
    match dag.ingest(witness_event.clone()) {
        Ok(true) => Ok(Some(witness_event)),
        Ok(false) => {
            log(String::from(
                "[mesh] mesh_author_witness: ingest buffered — should not happen for self-signed",
            ));
            Ok(None)
        }
        Err(e) => Err(format!("mesh_author_witness ingest: {}", e)),
    }
}

// ---- broadcasting helpers ----

fn broadcast_delivered(
    conns: &BTreeMap<String, ConnState>,
    submitter: &str,
    event_bytes: &[u8],
) {
    let frame = encode_delivered(event_bytes);
    let targets: Vec<String> = conns
        .iter()
        .filter_map(|(cid, cs)| match &cs.phase {
            Phase::Authed { .. } => Some(cid.clone()),
            _ => None,
        })
        .collect();
    for cid in targets {
        let _ = tcp_send(cid, frame.clone());
    }
    // Also send to the submitter (temporarily removed from `conns` during
    // on-data processing). Skip when there's no submitter — e.g. when mesh
    // itself emits a Witness via the timer tick.
    if !submitter.is_empty() {
        let _ = tcp_send(submitter.to_string(), frame);
    }
}

/// The "latest anchor" — the most recent leaf-ish event hash we'd advise a
/// client to branch off. For v2 phase 1 we just pick a leaf with the
/// smallest hash if there are several, or GENESIS_PARENT if the DAG is empty.
fn latest_anchor(dag: &Dag) -> event::Hash {
    let leaves = dag.leaves();
    leaves.into_iter().next().unwrap_or(GENESIS_PARENT)
}

// ---- connection state ----

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ConnState {
    phase: Phase,
    buf: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "phase")]
enum Phase {
    AwaitingHello,
    AwaitingAuth { pubkey_hex: String, nonce_hex: String },
    Authed { pubkey_hex: String },
}

impl ConnState {
    fn awaiting_hello() -> Self {
        Self { phase: Phase::AwaitingHello, buf: Vec::new() }
    }
    fn recv_buf(&self) -> &Vec<u8> { &self.buf }
    fn recv_buf_mut(&mut self) -> &mut Vec<u8> { &mut self.buf }
}

fn deser_connections(s: &str) -> BTreeMap<String, ConnState> {
    serde_json::from_str(s).unwrap_or_default()
}
fn ser_connections(c: &BTreeMap<String, ConnState>) -> String {
    serde_json::to_string(c).unwrap_or_else(|_| "{}".to_string())
}

// ---- DAG (de)serialization ----

#[derive(serde::Serialize, serde::Deserialize)]
struct DagBlob {
    events_hex: BTreeMap<String, String>, // event_hash_hex -> event_bytes_hex
}

fn serialize_dag(dag: &Dag) -> String {
    let mut events_hex = BTreeMap::new();
    for (h, ev) in &dag.events {
        events_hex.insert(hex(h), hex(&ev.encode()));
    }
    serde_json::to_string(&DagBlob { events_hex }).unwrap_or_else(|_| "{}".to_string())
}

fn deser_dag(
    s: &str,
    root_pubkey_hex: &str,
    peer_nodes_json: &str,
) -> Result<Dag, String> {
    let blob: DagBlob = serde_json::from_str(s).unwrap_or(DagBlob {
        events_hex: BTreeMap::new(),
    });
    let root = parse_hex_pubkey(root_pubkey_hex)?;
    let peer_nodes_raw: Vec<(String, String)> =
        serde_json::from_str(peer_nodes_json).unwrap_or_default();
    let mut peer_nodes: Vec<(PubKey, String)> = Vec::new();
    for (pk_hex, name) in peer_nodes_raw {
        if let Ok(pk) = parse_hex_pubkey(&pk_hex) {
            peer_nodes.push((pk, name));
        }
    }
    let mut dag = Dag::new_with_peers(root, peer_nodes);
    for (_h, ev_hex) in blob.events_hex {
        let bytes = decode_hex(&ev_hex)?;
        if let Ok(ev) = Event::decode(&bytes) {
            let _ = dag.ingest(ev);
        }
    }
    Ok(dag)
}

fn ser_pending(v: &[event::Hash]) -> String {
    let hexed: Vec<String> = v.iter().map(|h| hex(h)).collect();
    serde_json::to_string(&hexed).unwrap_or_else(|_| "[]".to_string())
}
fn deser_pending(s: &str) -> Vec<event::Hash> {
    let hexed: Vec<String> = serde_json::from_str(s).unwrap_or_default();
    hexed.into_iter().filter_map(|h| parse_hex32(&h).ok()).collect()
}

// ---- helpers ----

fn parse_hex_pubkey(s: &str) -> Result<PubKey, String> {
    parse_hex32(s)
}

fn parse_hex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("bad hex at byte {}", i))?;
    }
    Ok(out)
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex".to_string());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| format!("bad hex at {}", i))?,
        );
    }
    Ok(out)
}

fn random_nonce(conn_id: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(&now_ms().to_be_bytes());
    h.update(conn_id.as_bytes());
    let out = h.finalize();
    let mut nonce = [0u8; 32];
    nonce.copy_from_slice(&out);
    nonce
}
