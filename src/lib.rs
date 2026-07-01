//! mesh v3 — DAG-based replicated state-machine substrate.
//!
//! See DESIGN.md. Each node is a self-rooted log; events carry
//! `self_parent ∪ refs` back-edges; membership is static config; finality is
//! "every member has witnessed." Message-passing rides on top as a reducer.
//!
//! Actor flow:
//!   - init: derive key, build the static member set, author this node's
//!     genesis, listen, dial configured peers (client-side handshake).
//!   - connections authenticate by membership (HELLO → CHALLENGE → AUTH →
//!     ACCEPTED), then exchange FRONTIER/WANT to catch up.
//!   - DELIVER gossips events; on a newly-seen *payload* event a node forwards
//!     it, NOTIFYs its app clients (optimistic delivery), and authors a graft
//!     (its witness). WANT backfills missing ancestry.
//!   - SUBMIT lets an app client ask this node to author a payload event.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use ed25519_dalek::{Signer, SigningKey};
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
    connections_from_json, connections_to_json, dag_from_json, dag_to_json, events_from_json,
    events_to_json, from_hex32, hashes_from_json, hashes_to_json, hex, members_from_json,
    members_to_json,
};
use conn::{ConnState, Phase};
use dag::Dag;
use event::{Event, Hash, PubKey, SystemOp};
use wire::{
    decode_checkpoint, decode_hashes, encode_ack, encode_auth, encode_challenge, encode_checkpoint,
    encode_deliver, encode_hashes, encode_hello, encode_notify, encode_rejected, try_parse_frame,
    ParsedFrame, FRAME_ACCEPTED, FRAME_AUTH, FRAME_CHALLENGE, FRAME_CHECKPOINT, FRAME_DELIVER,
    FRAME_DEPART, FRAME_FRONTIER, FRAME_HELLO, FRAME_INTRODUCE, FRAME_SUBMIT, FRAME_WANT,
};

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ActorState {
    pub listener_id: String,
    pub listen_addr: String,
    /// This node's signing key (hex), derived from `node_seed`.
    pub signing_key_hex: String,
    /// The static, configured member set, JSON `[pubkey_hex, ...]`.
    pub members_json: String,
    /// This node's own chain head (hex event hash).
    pub self_head_hex: String,
    /// Persisted DAG (admitted events).
    pub dag_json: String,
    /// Persisted orphan buffer (events awaiting missing dependencies).
    pub pending_json: String,
    /// Event hashes of messages already delivered to clients (NOTIFY dedup).
    pub delivered_json: String,
    pub connections_json: String,
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
const HEARTBEAT_TIMER: &str = "heartbeat";
const DEFAULT_INTERVAL_MS: u64 = 2000;

// ---- init ----

#[derive(serde::Deserialize)]
struct InitConfig {
    node_seed: String,
    /// All other member pubkeys (hex). These, plus this node's own, form the
    /// static member set. Membership is independent of who we dial.
    #[serde(default)]
    members: Vec<String>,
    /// Peers to outbound-connect to on init: pubkey + address. Must be a subset
    /// of `members`.
    #[serde(default)]
    dial: Vec<PeerEntry>,
    #[serde(default)]
    listen_addr: Option<String>,
    #[serde(default)]
    heartbeat_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct PeerEntry {
    pubkey: String,
    address: String,
}

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(ActorState, ()), String> {
    log(String::from("[mesh] init (v3)"));
    let cfg: InitConfig = match state {
        Value::String(s) if !s.is_empty() => {
            serde_json::from_str(&s).map_err(|e| format!("parse init_state: {}", e))?
        }
        _ => return Err("missing init_state (need {\"node_seed\":\"...\"})".to_string()),
    };
    let heartbeat_ms = cfg.heartbeat_ms.unwrap_or(DEFAULT_INTERVAL_MS);
    let listen_addr = cfg.listen_addr.clone().unwrap_or_else(|| LISTEN_ADDR.to_string());

    let mut h = Sha256::new();
    h.update(cfg.node_seed.as_bytes());
    let key_bytes: [u8; 32] = h.finalize().into();
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let self_pubkey = signing_key.verifying_key().to_bytes();

    // Genesis member set = the configured bootstrap set (identical on every
    // node), or {self} if none is given (single-node). We do NOT auto-add self:
    // a node whose key isn't in the genesis set is a *joining* node, admitted
    // later via an Introduce event.
    let mut members = members_from_json("[]");
    for m in &cfg.members {
        members.insert(from_hex32(m)?);
    }
    if members.is_empty() {
        members.insert(self_pubkey);
    }
    let is_bootstrap = members.contains(&self_pubkey);

    let mut dag = Dag::new(members.clone());
    // Bootstrap members author a genesis at init; a joining node has no chain
    // yet — it authors its first event (grafting its admission) after it syncs.
    let self_head: Option<Hash> = if is_bootstrap {
        Some(author_genesis(&mut dag, &signing_key).event_hash())
    } else {
        None
    };

    let listener_id =
        tcp_listen(listen_addr.clone()).map_err(|e| format!("listen failed: {}", e))?;
    log(format!(
        "[mesh] listening on {} (id={}); self={}; members={}",
        &listen_addr,
        listener_id,
        &hex(&self_pubkey),
        members.len(),
    ));
    if let Err(e) = timer_set_interval(HEARTBEAT_TIMER.to_string(), heartbeat_ms) {
        log(format!("[mesh] set-interval failed: {}", e));
    }

    // Dial peers, handshake from the client side, register them as authed.
    let mut conns: BTreeMap<String, ConnState> = BTreeMap::new();
    for p in &cfg.dial {
        match open_peer_connection(&p.address, &signing_key, &dag) {
            Ok(conn_id) => {
                log(format!("[mesh] dialed peer {} (conn {})", &p.address, conn_id));
                conns.insert(conn_id, ConnState::authed(p.pubkey.clone()));
            }
            Err(e) => log(format!("[mesh] dial {} failed: {}", &p.address, e)),
        }
    }

    Ok((
        ActorState {
            listener_id,
            listen_addr,
            signing_key_hex: hex(&key_bytes),
            members_json: members_to_json(&members),
            self_head_hex: self_head.map(|h| hex(&h)).unwrap_or_default(),
            dag_json: dag_to_json(&dag),
            pending_json: "[]".to_string(),
            delivered_json: "[]".to_string(),
            connections_json: connections_to_json(&conns),
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
    let mut delivered: BTreeSet<Hash> = hashes_from_json(&state.delivered_json).into_iter().collect();

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
            Phase::AwaitingHello => match step_hello(&conn_id, &frame, &dag) {
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
                        // Send our checkpoint (so a behind peer can bootstrap past
                        // our pruned watermark) then our frontier.
                        let _ = tcp_send(conn_id.clone(), checkpoint_frame(&dag));
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
    // Deliver any newly-finalized messages (committed delivery via the reducer).
    deliver_committed(&dag, &conns, &mut delivered);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            connections_json: connections_to_json(&conns),
            self_head_hex: self_head.map(|h| hex(&h)).unwrap_or_default(),
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

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: ActorState, _timer_name: String) -> Result<(ActorState, ()), String> {
    // Periodic compaction: drop sealed history below the finalized frontier.
    // Safe now that catch-up carries a checkpoint (base members + sealed anchors)
    // over the sync path, so a behind node can bootstrap past the watermark.
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    let mut delivered: BTreeSet<Hash> = hashes_from_json(&state.delivered_json).into_iter().collect();

    let pruned = dag.compact(&delivered);
    if pruned.is_empty() {
        return Ok((state, ()));
    }
    for h in &pruned {
        delivered.remove(h);
    }
    log(format!(
        "[mesh] compacted {} events; {} retained, {} sealed",
        pruned.len(),
        dag.events.len(),
        dag.sealed.len()
    ));

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        ActorState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            ..state
        },
        (),
    ))
}

// ---- handshake steps ----

enum Step {
    Advance(Phase),
    Close,
}

fn step_hello(conn_id: &str, frame: &ParsedFrame, dag: &Dag) -> Step {
    if frame.kind != FRAME_HELLO || frame.payload.len() != 32 {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("expected HELLO(pubkey)"));
        let _ = tcp_close(conn_id.to_string());
        return Step::Close;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&frame.payload);
    if !dag.consensus_members().contains(&pk) {
        let _ = tcp_send(conn_id.to_string(), encode_rejected("not a member"));
        let _ = tcp_close(conn_id.to_string());
        return Step::Close;
    }
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
            Ok(ev) => ingest_and_propagate(dag, conns, conn_id, signing_key, self_head, ev),
            Err(e) => {
                log(format!("[mesh] DELIVER decode failed: {}", e));
                self_head
            }
        },
        // App client asks us to author an event on our own chain: a message
        // payload (SUBMIT), or a membership change (INTRODUCE / DEPART).
        FRAME_SUBMIT => {
            author_and_broadcast(dag, conns, conn_id, signing_key, self_head, frame.payload.clone(), None)
        }
        FRAME_INTRODUCE => {
            if frame.payload.len() != 32 {
                let _ = tcp_send(conn_id.to_string(), encode_ack(&[0u8; 32], false, "INTRODUCE needs pubkey[32]"));
                return self_head;
            }
            let mut node = [0u8; 32];
            node.copy_from_slice(&frame.payload);
            let op = Some(SystemOp::Introduce { node });
            author_and_broadcast(dag, conns, conn_id, signing_key, self_head, Vec::new(), op)
        }
        FRAME_DEPART => {
            let me = signing_key.verifying_key().to_bytes();
            let op = Some(SystemOp::Depart { node: me });
            author_and_broadcast(dag, conns, conn_id, signing_key, self_head, Vec::new(), op)
        }
        FRAME_WANT => {
            for h in decode_hashes(&frame.payload) {
                if let Some(ev) = dag.events.get(&h) {
                    let _ = tcp_send(conn_id.to_string(), encode_deliver(&ev.encode()));
                }
            }
            self_head
        }
        FRAME_FRONTIER => {
            let missing: Vec<Hash> =
                decode_hashes(&frame.payload).into_iter().filter(|h| !dag.has(h)).collect();
            if !missing.is_empty() {
                let _ = tcp_send(conn_id.to_string(), encode_hashes(FRAME_WANT, &missing));
            }
            self_head
        }
        FRAME_CHECKPOINT => {
            // Adopt if we're behind this peer's pruned watermark (a no-op
            // otherwise), then any events buffered on now-sealed deps re-drive.
            let (members, sealed) = decode_checkpoint(&frame.payload);
            dag.install_checkpoint(members.into_iter().collect(), sealed.into_iter().collect());
            self_head
        }
        // ACK / NOTIFY are responses meant for app clients; a node ignores them.
        _ => self_head,
    }
}

/// Ingest a gossiped event: dedup, backfill on missing deps, else admit +
/// forward + (for payload events) NOTIFY clients and author a witnessing graft.
fn ingest_and_propagate(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    from_conn: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    event: Event,
) -> Option<Hash> {
    if dag.has(&event.event_hash()) {
        return self_head; // dedup — already have it
    }
    // Anything that must reach finality — a payload event or a membership change
    // — we witness by grafting. Pure heartbeats (empty payload, no system op)
    // are only forwarded, so grafts don't beget grafts forever.
    let needs_witness = !event.payload.is_empty() || event.system.is_some();
    let encoded = event.encode();
    let missing = missing_deps(dag, &event); // compute before `ingest` consumes it

    match dag.ingest(event) {
        Ok(true) => {
            // Forward to every other peer (the source is excluded — it's not in
            // `conns` right now, having been removed for the duration of on-data).
            broadcast(conns, from_conn, &encode_deliver(&encoded));
            if needs_witness {
                // Witness it: author a graft whose refs cover the new head.
                // (Delivery happens on finality, via deliver_committed.)
                match author_event(dag, signing_key, self_head, Vec::new(), None) {
                    Ok(graft) => {
                        // Send our witness to every peer INCLUDING the source. The
                        // source was pulled out of `conns` for the duration of
                        // on-data, but it's precisely the peer that needs to see
                        // we witnessed — so *it* can finalize. Without this, a
                        // witness never flows back to an event's origin.
                        let frame = encode_deliver(&graft.encode());
                        broadcast(conns, "", &frame);
                        let _ = tcp_send(from_conn.to_string(), frame);
                        return Some(graft.event_hash());
                    }
                    Err(e) => log(format!("[mesh] graft failed: {}", e)),
                }
            }
            self_head
        }
        Ok(false) => {
            // Missing a dependency — ask the source for it.
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
    let ev = Event::sign(signing_key, None, Vec::new(), Vec::new(), None);
    let _ = dag.ingest(ev.clone());
    ev
}

/// Author an event on this node's chain: self_parent = current head (`None` for
/// the node's first event), refs = foreign heads we've seen. Ingests and returns
/// it. A joining node's first event has `self_head == None` but still grafts the
/// network via `refs` (which reach its Introduce), so it validates as a member.
fn author_event(
    dag: &mut Dag,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    system: Option<SystemOp>,
) -> Result<Event, String> {
    let author = signing_key.verifying_key().to_bytes();
    let refs = foreign_heads(dag, &author);
    let ev = Event::sign(signing_key, self_head, refs, payload, system);
    match dag.ingest(ev.clone())? {
        true => Ok(ev),
        false => Err("authored event buffered (missing dep)".to_string()),
    }
}

/// Author an event (payload or system op), ACK the requester, and broadcast it.
/// Returns the new self_head on success, unchanged on failure.
fn author_and_broadcast(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    system: Option<SystemOp>,
) -> Option<Hash> {
    match author_event(dag, signing_key, self_head, payload, system) {
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

/// Heads of every member other than `me` that we currently hold.
fn foreign_heads(dag: &Dag, me: &PubKey) -> Vec<Hash> {
    let mut out = Vec::new();
    for m in dag.consensus_members() {
        if m == *me {
            continue;
        }
        out.extend(dag.heads_of(&m));
    }
    out
}

/// Encode this node's checkpoint (membership base + sealed anchors) so a peer
/// catching up past our pruned watermark can bootstrap.
fn checkpoint_frame(dag: &Dag) -> Vec<u8> {
    let members: Vec<PubKey> = dag.base_members.iter().copied().collect();
    let sealed: Vec<Hash> = dag.sealed.iter().copied().collect();
    encode_checkpoint(&members, &sealed)
}

/// All current heads across all members — our advertised frontier.
fn all_heads(dag: &Dag) -> Vec<Hash> {
    let mut out = Vec::new();
    for m in dag.consensus_members() {
        out.extend(dag.heads_of(&m));
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

/// Broadcast newly-finalized payloads to connected clients via NOTIFY. Walks the
/// finalized, canonically-ordered stream and NOTIFYs `(author, payload)` for
/// every payload-bearing event not yet delivered (tracked in `delivered`). The
/// substrate is payload-agnostic: addressing / message-type / routing all live
/// in the payload bytes and are the application's concern.
fn deliver_committed(dag: &Dag, conns: &BTreeMap<String, ConnState>, delivered: &mut BTreeSet<Hash>) {
    for h in dag.ordered_finalized() {
        let Some(ev) = dag.events.get(&h) else {
            continue;
        };
        if ev.payload.is_empty() || !delivered.insert(h) {
            continue;
        }
        let frame = encode_notify(&ev.author, &ev.payload);
        for (cid, cs) in conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                let _ = tcp_send(cid.clone(), frame.clone());
            }
        }
    }
}

fn challenge_nonce(conn_id: &str) -> [u8; 32] {
    // Derived, not cryptographically random (predictable from timing); adequate
    // as a possession check, replace with a CSPRNG before relying on replay
    // resistance. Same caveat as v2.
    let mut h = Sha256::new();
    h.update(now_ms().to_be_bytes());
    h.update(conn_id.as_bytes());
    h.finalize().into()
}
