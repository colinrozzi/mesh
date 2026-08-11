//! mesh node — the pure, I/O-free RSM core (DESIGN-dx.md "Interface 0" / `node.pact`).
//!
//! The core is identity + gossip + a partial-order (DAG) event log + the fold over a
//! composed consumer state machine. It performs **no host I/O**: every entry point takes
//! bytes/events in and returns `(new node-state, a list of EFFECTS)` as DATA. The system
//! entry actor owns the sockets / timer / message-server and performs the effects (see
//! `node.pact`). It imports NO host functions at all — a non-entry composed component
//! can't call them (theater rejects the encoding), so diagnostics are dropped here and the
//! system does the logging. A pure core can't read a clock either, so the system injects
//! `now` where events are signed (init / on-bytes / author).
//!
//! Finality is admission in v0 (conflict-free consumers), judged ancestry-relative and
//! memoized. Membership is the SM's business; the transport authenticates identity only.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use ed25519_dalek::{Signer, SigningKey};
use packr_guest::{export, import_from, pack_types, Value};
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

/// The node's own persisted state. Opaque bytes across the `node` interface — the system
/// holds it and never inspects it; only the node (de)serializes it (as JSON). No
/// `listener_id`/`listen_addr` here: sockets are the system's, not the core's.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct NodeState {
    /// This node's signing key (hex), derived from `node_seed`.
    pub signing_key_hex: String,
    /// This node's own chain head (hex event hash).
    pub self_head_hex: String,
    /// Persisted DAG (admitted events).
    pub dag_json: String,
    /// Persisted orphan buffer (events awaiting missing dependencies).
    pub pending_json: String,
    /// Event hashes of payloads already delivered (delivery dedup).
    pub delivered_json: String,
    /// Memoized per-event finality (hex hash → final?), so each pass only decides
    /// newly-admitted events instead of re-folding the whole DAG.
    pub final_json: String,
    pub connections_json: String,
    /// The subscribed app actor's id to stream finalized dag-nodes to (App effects).
    /// Empty = no subscriber.
    pub app_id: String,
    /// True once the one-shot Ready signal has been sent to the app.
    pub ready_sent: bool,
}

fn ns_load(bytes: &[u8]) -> Result<NodeState, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("node-state decode: {}", e))
}
fn ns_save(state: &NodeState) -> Vec<u8> {
    serde_json::to_vec(state).unwrap_or_default()
}

/// A deferred output action the system performs. The pure engine pushes these instead of
/// calling host I/O; they cross the `node` interface as self-framed byte blobs (see
/// `encode_effect`). This is the seam that makes the core I/O-free (DESIGN-dx.md).
enum Effect {
    /// tcp `send` of `bytes` on connection `conn`.
    Send(String, Vec<u8>),
    /// message-server `send` of `bytes` to actor `id` (the subscribed app).
    App(String, Vec<u8>),
    /// tcp `close` of connection `conn`.
    Close(String),
}

type Outbox = Vec<Effect>;

/// What the system should do at init, encoded for the `node.init` return. The node parses
/// config (it derives identity) but the system owns the sockets, so it performs the plan:
/// `listen(listen_addr)`, `set-interval(tick_ms)`, and `connect` each dial (then calls
/// `on-connect(dialed=true)`).
struct InitPlan {
    listen_addr: String,
    tick_ms: u64,
    dials: Vec<(String, String)>, // (pubkey hex, address)
}

pack_types! {
    // The node is GENERIC over the SM's state `s` AND its payload `p` (packr 0.13 M4):
    // compose unifies both to the SM's concrete types. Both are erased to the dynamic
    // `Value` here — the node never inspects them, it just decodes the payload bytes to a
    // typed value and shuttles state between validate/apply.
    type s: serializable
    type p: serializable
    imports {
        // The node is I/O-FREE — it imports NO host functions, only the composed SM. A
        // non-entry composed component can't call host imports (theater rejects the
        // encoding), and it doesn't need to: everything is returned as an Effect.
        // The consumer state machine (DESIGN-rsm.md Interface 1), composed IN. Called
        // synchronously on the fold hot path.
        state-machine {
            initial-state: func() -> s,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: p, state: s) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: p, state: s) -> s,
            members: func(state: s) -> list<list<u8>>,
        }
    }
    exports {
        // The `node` interface (DESIGN-dx.md Interface 0 / node.pact) — the system↔core
        // boundary. Pure: state in/out is opaque bytes, effects come back as data.
        node {
            init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>,
            on-connect: func(state: list<u8>, conn: string, dialed: bool, peer: string) -> tuple<list<u8>, list<list<u8>>>,
            on-bytes: func(state: list<u8>, conn: string, data: list<u8>, now: u64) -> tuple<list<u8>, list<list<u8>>>,
            on-close: func(state: list<u8>, conn: string) -> list<u8>,
            tick: func(state: list<u8>) -> tuple<list<u8>, list<list<u8>>>,
            author: func(state: list<u8>, payload: list<u8>, now: u64) -> tuple<list<u8>, bool, list<u8>, list<list<u8>>>,
            subscribe: func(state: list<u8>, app-id: string) -> tuple<list<u8>, list<list<u8>>>,
            current-state: func(state: list<u8>) -> list<u8>,
            event-status: func(state: list<u8>, id: list<u8>) -> u8,
        }
    }
}

/// The node is I/O-free, so diagnostics are dropped here (a non-entry composed component
/// cannot call host imports). The system does the logging; kept as a no-op so the engine
/// reads the same and can later route diagnostics as a Log effect if wanted.
#[inline]
fn log(_msg: String) {}

// ---- composed state machine (DESIGN-rsm.md Interface 1) ----
#[import_from("state-machine", name = "initial-state")]
fn sm_initial_state() -> Value;
#[import_from("state-machine", name = "validate")]
fn sm_validate(
    id: Vec<u8>,
    author: Vec<u8>,
    timestamp: u64,
    payload: Value,
    state: Value,
) -> Result<bool, String>;
#[import_from("state-machine", name = "apply")]
fn sm_apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Value, state: Value) -> Value;

/// Decode a payload's bytes into the typed value the SM expects (the erased `p`). The wire
/// is the Graph-ABI encoding of the SM's payload type; `None` if it isn't well-formed.
fn decode_payload(bytes: &[u8]) -> Option<Value> {
    packr_guest::decode(bytes).ok()
}
#[import_from("state-machine", name = "members")]
fn sm_members(state: Value) -> Vec<Vec<u8>>;

const LISTEN_ADDR: &str = "127.0.0.1:9447";
const DEFAULT_INTERVAL_MS: u64 = 2000;

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

// ============================================================================
// `node` interface — the pure exports (DESIGN-dx.md Interface 0)
//
// Each export marshals opaque `node-state` bytes <-> NodeState and encodes the Outbox as
// a list of self-framed effect blobs. The logic lives in the internal fns below.
// ============================================================================

#[export(name = "init")]
fn export_init(config: String, now: u64) -> Result<(Vec<u8>, Vec<u8>), String> {
    let (state, plan) = node_init(&config, now)?;
    Ok((ns_save(&state), encode_init_plan(&plan)))
}

#[export(name = "on-connect")]
fn export_on_connect(state: Vec<u8>, conn: String, dialed: bool, peer: String) -> (Vec<u8>, Vec<Vec<u8>>) {
    let s = match ns_load(&state) {
        Ok(s) => s,
        Err(e) => {
            log(format!("[mesh] on-connect: {}", e));
            return (state, Vec::new());
        }
    };
    let (s2, out) = node_on_connect(s, conn, dialed, peer);
    (ns_save(&s2), encode_effects(out))
}

#[export(name = "on-bytes")]
fn export_on_bytes(state: Vec<u8>, conn: String, data: Vec<u8>, now: u64) -> (Vec<u8>, Vec<Vec<u8>>) {
    let s = match ns_load(&state) {
        Ok(s) => s,
        Err(e) => {
            log(format!("[mesh] on-bytes: {}", e));
            return (state, Vec::new());
        }
    };
    match node_on_bytes(s, conn, data, now) {
        Ok((s2, out)) => (ns_save(&s2), encode_effects(out)),
        Err(e) => {
            log(format!("[mesh] on-bytes: {}", e));
            (state, Vec::new())
        }
    }
}

#[export(name = "on-close")]
fn export_on_close(state: Vec<u8>, conn: String) -> Vec<u8> {
    match ns_load(&state) {
        Ok(s) => ns_save(&node_on_close(s, conn)),
        Err(_) => state,
    }
}

#[export(name = "tick")]
fn export_tick(state: Vec<u8>) -> (Vec<u8>, Vec<Vec<u8>>) {
    let s = match ns_load(&state) {
        Ok(s) => s,
        Err(e) => {
            log(format!("[mesh] tick: {}", e));
            return (state, Vec::new());
        }
    };
    match node_tick(s) {
        Ok((s2, out)) => (ns_save(&s2), encode_effects(out)),
        Err(e) => {
            log(format!("[mesh] tick: {}", e));
            (state, Vec::new())
        }
    }
}

#[export(name = "author")]
#[allow(clippy::type_complexity)] // mirrors the node.pact `author` return tuple
fn export_author(state: Vec<u8>, payload: Vec<u8>, now: u64) -> (Vec<u8>, bool, Vec<u8>, Vec<Vec<u8>>) {
    let s = match ns_load(&state) {
        Ok(s) => s,
        Err(e) => return (state, false, e.into_bytes(), Vec::new()),
    };
    match node_author(s, payload, now) {
        Ok((s2, Ok(h), out)) => (ns_save(&s2), true, h.to_vec(), encode_effects(out)),
        Ok((s2, Err(reason), out)) => (ns_save(&s2), false, reason.into_bytes(), encode_effects(out)),
        Err(e) => (state, false, e.into_bytes(), Vec::new()),
    }
}

#[export(name = "subscribe")]
fn export_subscribe(state: Vec<u8>, app_id: String) -> (Vec<u8>, Vec<Vec<u8>>) {
    let s = match ns_load(&state) {
        Ok(s) => s,
        Err(e) => {
            log(format!("[mesh] subscribe: {}", e));
            return (state, Vec::new());
        }
    };
    match node_subscribe(s, app_id) {
        Ok((s2, out)) => (ns_save(&s2), encode_effects(out)),
        Err(e) => {
            log(format!("[mesh] subscribe: {}", e));
            (state, Vec::new())
        }
    }
}

#[export(name = "current-state")]
fn export_current_state(state: Vec<u8>) -> Vec<u8> {
    match ns_load(&state) {
        Ok(s) => node_current_state(&s).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

#[export(name = "event-status")]
fn export_event_status(state: Vec<u8>, id: Vec<u8>) -> u8 {
    match ns_load(&state) {
        Ok(s) => node_event_status(&s, &id).unwrap_or(STATUS_UNKNOWN),
        Err(_) => STATUS_UNKNOWN,
    }
}

// ---- effect + init-plan codecs (the `node` interface wire) ----

/// One effect as `[kind:u8][id-len:u16 BE][id utf8][payload...]` (kind 0=Send, 1=App,
/// 2=Close). The system decodes and performs.
fn encode_effect(e: &Effect) -> Vec<u8> {
    let (kind, id, payload): (u8, &str, &[u8]) = match e {
        Effect::Send(c, b) => (0, c.as_str(), b),
        Effect::App(i, b) => (1, i.as_str(), b),
        Effect::Close(c) => (2, c.as_str(), &[]),
    };
    let mut v = Vec::with_capacity(3 + id.len() + payload.len());
    v.push(kind);
    v.extend_from_slice(&(id.len() as u16).to_be_bytes());
    v.extend_from_slice(id.as_bytes());
    v.extend_from_slice(payload);
    v
}

fn encode_effects(out: Outbox) -> Vec<Vec<u8>> {
    out.iter().map(encode_effect).collect()
}

/// `[listen-len:u16][listen][tick_ms:u64][ndials:u16]` then per dial
/// `[pk-len:u16][pk][addr-len:u16][addr]`.
fn encode_init_plan(p: &InitPlan) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(p.listen_addr.len() as u16).to_be_bytes());
    v.extend_from_slice(p.listen_addr.as_bytes());
    v.extend_from_slice(&p.tick_ms.to_be_bytes());
    v.extend_from_slice(&(p.dials.len() as u16).to_be_bytes());
    for (pk, addr) in &p.dials {
        v.extend_from_slice(&(pk.len() as u16).to_be_bytes());
        v.extend_from_slice(pk.as_bytes());
        v.extend_from_slice(&(addr.len() as u16).to_be_bytes());
        v.extend_from_slice(addr.as_bytes());
    }
    v
}

// ============================================================================
// `node` interface — the internal logic (host-testable; pure over NodeState + Outbox)
// ============================================================================

fn node_init(config: &str, now: u64) -> Result<(NodeState, InitPlan), String> {
    let cfg: InitConfig =
        serde_json::from_str(config).map_err(|e| format!("parse init_state: {}", e))?;
    let tick_ms = cfg.tick_ms.unwrap_or(DEFAULT_INTERVAL_MS);
    let listen_addr = cfg.listen_addr.clone().unwrap_or_else(|| LISTEN_ADDR.to_string());

    let mut h = Sha256::new();
    h.update(cfg.node_seed.as_bytes());
    let key_bytes: [u8; 32] = h.finalize().into();
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let self_pubkey = signing_key.verifying_key().to_bytes();

    // Every node is a self-rooted log: author an (empty) genesis so it has a chain head
    // and a frontier to gossip (an empty-payload graft is inert to the SM).
    let mut dag = Dag::new();
    let self_head = author_genesis(&mut dag, &signing_key, now).event_hash();
    log(format!("[mesh] init self={}", hex(&self_pubkey)));

    let dials = cfg.dial.iter().map(|p| (p.pubkey.clone(), p.address.clone())).collect();
    let state = NodeState {
        signing_key_hex: hex(&key_bytes),
        self_head_hex: hex(&self_head),
        dag_json: dag_to_json(&dag),
        pending_json: "[]".to_string(),
        delivered_json: "[]".to_string(),
        final_json: "{}".to_string(),
        connections_json: connections_to_json(&BTreeMap::new()),
        app_id: String::new(),
        ready_sent: false,
    };
    Ok((state, InitPlan { listen_addr, tick_ms, dials }))
}

/// A raw connection now exists. A dialed peer gets our HELLO (kicks off the client-side
/// handshake); an inbound accept just registers, awaiting the peer's HELLO.
fn node_on_connect(mut state: NodeState, conn_id: String, dialed: bool, peer: String) -> (NodeState, Outbox) {
    let mut conns = connections_from_json(&state.connections_json);
    let mut out: Outbox = Vec::new();
    if dialed {
        conns.insert(conn_id.clone(), ConnState::dialing(peer));
        if let Ok(kb) = from_hex32(&state.signing_key_hex) {
            let pk = SigningKey::from_bytes(&kb).verifying_key().to_bytes();
            out.push(Effect::Send(conn_id.clone(), encode_hello(&pk)));
        }
        log(format!("[mesh] dialed peer (conn {})", conn_id));
    } else {
        conns.insert(conn_id.clone(), ConnState::awaiting_hello());
        log(format!("[mesh] conn {} opened", conn_id));
    }
    state.connections_json = connections_to_json(&conns);
    (state, out)
}

fn node_on_bytes(state: NodeState, conn_id: String, data: Vec<u8>, now: u64) -> Result<(NodeState, Outbox), String> {
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
    let mut out: Outbox = Vec::new();

    let mut conn_state = match conns.remove(&conn_id) {
        Some(c) => c,
        None => {
            log(format!("[mesh] on-data for unknown conn {}", conn_id));
            out.push(Effect::Close(conn_id));
            return Ok((state, out));
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
                out.push(Effect::Send(conn_id.clone(), encode_rejected(&e)));
                out.push(Effect::Close(conn_id.clone()));
                should_close = true;
                break;
            }
        };
        let kept = conn_state.recv_buf()[frame.total_len..].to_vec();
        *conn_state.recv_buf_mut() = kept;

        match conn_state.phase.clone() {
            Phase::AwaitingHello => match step_hello(&conn_id, &frame, &mut out, now) {
                Step::Advance(p) => conn_state.phase = p,
                Step::Close => {
                    should_close = true;
                    break;
                }
            },
            Phase::AwaitingAuth { pubkey_hex, nonce_hex } => {
                match step_auth(&conn_id, &frame, &pubkey_hex, &nonce_hex, &dag, &mut out)? {
                    Step::Advance(p) => {
                        conn_state.phase = p;
                        // Announce our frontier; a behind peer WANTs what it lacks and we
                        // answer with the event (full history is retained).
                        out.push(Effect::Send(
                            conn_id.clone(),
                            encode_hashes(FRAME_FRONTIER, &all_heads(&dag)),
                        ));
                    }
                    Step::Close => {
                        should_close = true;
                        break;
                    }
                }
            }
            Phase::AwaitingChallenge { peer_pubkey_hex } => {
                match step_challenge(&conn_id, &frame, &peer_pubkey_hex, &signing_key, &mut out) {
                    Step::Advance(p) => conn_state.phase = p,
                    Step::Close => {
                        should_close = true;
                        break;
                    }
                }
            }
            Phase::AwaitingAccepted { peer_pubkey_hex } => {
                match step_accepted(&conn_id, &frame, &peer_pubkey_hex, &dag, &mut out) {
                    Step::Advance(p) => conn_state.phase = p,
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
                    &mut out,
                    now,
                );
            }
        }
    }

    if !should_close {
        conns.insert(conn_id.clone(), conn_state);
    }
    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality, &mut out);
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent, &mut out);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        NodeState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            connections_json: connections_to_json(&conns),
            self_head_hex: self_head.map(|h| hex(&h)).unwrap_or_default(),
            ready_sent,
            ..state
        },
        out,
    ))
}

fn node_on_close(mut state: NodeState, conn_id: String) -> NodeState {
    log(format!("[mesh] conn {} closed", conn_id));
    let mut conns = connections_from_json(&state.connections_json);
    conns.remove(&conn_id);
    state.connections_json = connections_to_json(&conns);
    state
}

/// Periodic tick: delivery safety-net + anti-entropy (re-advertise frontier to authed
/// peers so a one-shot gossip miss or partition heal reconciles within a few ticks).
fn node_tick(state: NodeState) -> Result<(NodeState, Outbox), String> {
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    let conns = connections_from_json(&state.connections_json);
    let mut delivered: BTreeSet<Hash> =
        hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let mut out: Outbox = Vec::new();

    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality, &mut out);

    let frontier = all_heads(&dag);
    if !frontier.is_empty() {
        for (cid, cs) in &conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                out.push(Effect::Send(cid.clone(), encode_hashes(FRAME_FRONTIER, &frontier)));
            }
        }
    }
    let ready_sent = maybe_emit_ready(&state.app_id, state.ready_sent, &mut out);

    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        NodeState {
            dag_json: dag_to_json(&dag),
            pending_json: events_to_json(&dag.pending_events()),
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            ready_sent,
            ..state
        },
        out,
    ))
}

/// Author a payload event on this node's chain (pre-validated), gossip it, and emit the
/// finalized stream. Returns the new state, an ok(hash)/err(reason) result, and effects.
fn node_author(state: NodeState, payload: Vec<u8>, now: u64) -> Result<(NodeState, Result<Hash, String>, Outbox), String> {
    let signing_key = SigningKey::from_bytes(&from_hex32(&state.signing_key_hex)?);
    let mut dag = load_dag(&state)?;
    let conns = connections_from_json(&state.connections_json);
    let self_head = if state.self_head_hex.is_empty() {
        None
    } else {
        Some(from_hex32(&state.self_head_hex)?)
    };
    let mut delivered: BTreeSet<Hash> = hashes_from_json(&state.delivered_json).into_iter().collect();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let mut out: Outbox = Vec::new();

    let (new_self_head, result) =
        run_command(&mut dag, &conns, &signing_key, self_head, payload, &mut finality, &mut out, now);
    if result.is_ok() {
        deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality, &mut out);
    }
    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    let new_state = NodeState {
        dag_json: dag_to_json(&dag),
        pending_json: events_to_json(&dag.pending_events()),
        delivered_json: hashes_to_json(&delivered_vec),
        final_json: finality_to_json(&finality),
        self_head_hex: new_self_head.map(|h| hex(&h)).unwrap_or_default(),
        ..state
    };
    Ok((new_state, result, out))
}

/// Register/replace the subscribed app and replay the finalized history to it (as App
/// effects) so a late subscriber catches up (idempotent — the app folds by SM state).
fn node_subscribe(mut state: NodeState, app_id: String) -> Result<(NodeState, Outbox), String> {
    state.app_id = app_id;
    let dag = load_dag(&state)?;
    let conns = connections_from_json(&state.connections_json);
    let mut delivered: BTreeSet<Hash> = BTreeSet::new();
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    let mut out: Outbox = Vec::new();
    deliver_committed(&dag, &conns, &state.app_id, &mut delivered, &mut finality, &mut out);
    let delivered_vec: Vec<Hash> = delivered.iter().copied().collect();
    Ok((
        NodeState {
            delivered_json: hashes_to_json(&delivered_vec),
            final_json: finality_to_json(&finality),
            ..state
        },
        out,
    ))
}

fn node_current_state(state: &NodeState) -> Result<Vec<u8>, String> {
    let dag = load_dag(state)?;
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    Ok(state_bytes(current_state(&dag, &mut finality)))
}

fn node_event_status(state: &NodeState, id: &[u8]) -> Result<u8, String> {
    if id.len() != 32 {
        return Err("event-status: expected a 32-byte hash".to_string());
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(id);
    let dag = load_dag(state)?;
    let mut finality: BTreeMap<Hash, bool> = finality_from_json(&state.final_json);
    Ok(event_status(&dag, &h, &mut finality))
}

/// Load the DAG (admitted + persisted orphans re-ingested) from the node state.
fn load_dag(state: &NodeState) -> Result<Dag, String> {
    let mut dag = dag_from_json(&state.dag_json)?;
    for ev in events_from_json(&state.pending_json) {
        let _ = dag.ingest(ev);
    }
    Ok(dag)
}

// ---- handshake steps ----

enum Step {
    Advance(Phase),
    Close,
}

/// HELLO → CHALLENGE. Identity proof only; the SM gates membership, not the transport.
fn step_hello(conn_id: &str, frame: &ParsedFrame, out: &mut Outbox, now: u64) -> Step {
    if frame.kind != FRAME_HELLO || frame.payload.len() != 32 {
        out.push(Effect::Send(conn_id.to_string(), encode_rejected("expected HELLO(pubkey)")));
        out.push(Effect::Close(conn_id.to_string()));
        return Step::Close;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&frame.payload);
    let nonce = challenge_nonce(conn_id, now);
    out.push(Effect::Send(conn_id.to_string(), encode_challenge(&nonce)));
    Step::Advance(Phase::AwaitingAuth { pubkey_hex: hex(&pk), nonce_hex: hex(&nonce) })
}

fn step_auth(
    conn_id: &str,
    frame: &ParsedFrame,
    pubkey_hex: &str,
    nonce_hex: &str,
    dag: &Dag,
    out: &mut Outbox,
) -> Result<Step, String> {
    if frame.kind != FRAME_AUTH || frame.payload.len() != 64 {
        out.push(Effect::Send(conn_id.to_string(), encode_rejected("expected AUTH(sig)")));
        out.push(Effect::Close(conn_id.to_string()));
        return Ok(Step::Close);
    }
    let pk = from_hex32(pubkey_hex)?;
    let nonce = from_hex32(nonce_hex)?;
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&frame.payload);
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let vk = VerifyingKey::from_bytes(&pk).map_err(|e| format!("bad pubkey: {}", e))?;
    if vk.verify(&nonce, &Signature::from_bytes(&sig)).is_err() {
        out.push(Effect::Send(conn_id.to_string(), encode_rejected("auth failed")));
        out.push(Effect::Close(conn_id.to_string()));
        return Ok(Step::Close);
    }
    out.push(Effect::Send(conn_id.to_string(), encode_hashes(FRAME_ACCEPTED, &all_heads(dag))));
    log(format!("[mesh] conn {} authed as {}", conn_id, pubkey_hex));
    Ok(Step::Advance(Phase::Authed { pubkey_hex: pubkey_hex.to_string() }))
}

/// Client side: CHALLENGE → AUTH. Sign the peer's nonce to prove we own our pubkey.
fn step_challenge(
    conn_id: &str,
    frame: &ParsedFrame,
    peer_pubkey_hex: &str,
    signing_key: &SigningKey,
    out: &mut Outbox,
) -> Step {
    if frame.kind != FRAME_CHALLENGE || frame.payload.len() != 32 {
        out.push(Effect::Send(conn_id.to_string(), encode_rejected("expected CHALLENGE(nonce)")));
        out.push(Effect::Close(conn_id.to_string()));
        return Step::Close;
    }
    let sig = signing_key.sign(&frame.payload).to_bytes();
    out.push(Effect::Send(conn_id.to_string(), encode_auth(&sig)));
    Step::Advance(Phase::AwaitingAccepted { peer_pubkey_hex: peer_pubkey_hex.to_string() })
}

/// Client side: ACCEPTED(frontier) → Authed. Catch up on what the peer's frontier shows
/// we lack, and announce our own.
fn step_accepted(
    conn_id: &str,
    frame: &ParsedFrame,
    peer_pubkey_hex: &str,
    dag: &Dag,
    out: &mut Outbox,
) -> Step {
    if frame.kind != FRAME_ACCEPTED {
        out.push(Effect::Send(conn_id.to_string(), encode_rejected("expected ACCEPTED(frontier)")));
        out.push(Effect::Close(conn_id.to_string()));
        return Step::Close;
    }
    for want in decode_hashes(&frame.payload).into_iter().filter(|h| !dag.has(h)) {
        out.push(Effect::Send(conn_id.to_string(), encode_hashes(FRAME_WANT, &[want])));
    }
    out.push(Effect::Send(conn_id.to_string(), encode_hashes(FRAME_FRONTIER, &all_heads(dag))));
    log(format!("[mesh] conn {} authed to {}", conn_id, peer_pubkey_hex));
    Step::Advance(Phase::Authed { pubkey_hex: peer_pubkey_hex.to_string() })
}

// ---- post-handshake frame handling ----

/// Handle one authenticated frame. Returns the (possibly advanced) self_head.
#[allow(clippy::too_many_arguments)]
fn handle_authed_frame(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    frame: &ParsedFrame,
    finality: &mut BTreeMap<Hash, bool>,
    out: &mut Outbox,
    now: u64,
) -> Option<Hash> {
    match frame.kind {
        FRAME_DELIVER => match Event::decode(&frame.payload) {
            Ok(ev) => ingest_and_propagate(dag, conns, conn_id, self_head, ev, out),
            Err(e) => {
                log(format!("[mesh] DELIVER decode failed: {}", e));
                self_head
            }
        },
        // Peer/app asks us to author a payload event on our own chain.
        FRAME_SUBMIT => author_and_broadcast(
            dag,
            conns,
            conn_id,
            signing_key,
            self_head,
            frame.payload.clone(),
            finality,
            out,
            now,
        ),
        // Interface 2 read verbs (current-state / event-status / witnesses / ancestry).
        FRAME_QUERY => {
            answer_query(dag, conn_id, &frame.payload, finality, out);
            self_head
        }
        FRAME_WANT => {
            for h in decode_hashes(&frame.payload) {
                if let Some(ev) = dag.events.get(&h) {
                    out.push(Effect::Send(conn_id.to_string(), encode_deliver(&ev.encode())));
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
                out.push(Effect::Send(conn_id.to_string(), encode_hashes(FRAME_WANT, &missing)));
            }
            self_head
        }
        _ => self_head,
    }
}

/// Classify an event for `event-status`: unknown / pending / finalized / stranded.
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

fn query_arg_hash(arg: &[u8]) -> Option<Hash> {
    if arg.len() < 32 {
        return None;
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&arg[..32]);
    Some(h)
}

/// Answer an Interface 2 read verb, pushing the reply as a Send effect on `conn_id`.
fn answer_query(dag: &Dag, conn_id: &str, body: &[u8], finality: &mut BTreeMap<Hash, bool>, out: &mut Outbox) {
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
    out.push(Effect::Send(conn_id.to_string(), reply));
}

/// Ingest a gossiped event: dedup, backfill on missing deps, else admit + forward.
fn ingest_and_propagate(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    from_conn: &str,
    self_head: Option<Hash>,
    event: Event,
    out: &mut Outbox,
) -> Option<Hash> {
    if dag.has(&event.event_hash()) {
        return self_head; // dedup — already have it
    }
    let missing = missing_deps(dag, &event); // compute before `ingest` consumes it

    match dag.ingest_admitted(event) {
        Ok(admitted) if !admitted.is_empty() => {
            // Forward EVERY newly-admitted event (the incoming one AND any buffered
            // waiters it unblocked); the source is excluded (not in `conns` during on-data).
            for h in &admitted {
                if let Some(ev) = dag.events.get(h) {
                    broadcast(conns, from_conn, &encode_deliver(&ev.encode()), out);
                }
            }
            self_head
        }
        Ok(_) => {
            if !missing.is_empty() {
                out.push(Effect::Send(from_conn.to_string(), encode_hashes(FRAME_WANT, &missing)));
            }
            self_head
        }
        Err(e) => {
            log(format!("[mesh] ingest rejected: {}", e));
            self_head
        }
    }
}

// ---- authoring + fold helpers ----

fn author_genesis(dag: &mut Dag, signing_key: &SigningKey, now: u64) -> Event {
    let ev = Event::sign(signing_key, now, None, Vec::new(), Vec::new());
    let _ = dag.ingest(ev.clone());
    ev
}

/// The FINALIZED fold over the whole current frontier — `current-state`.
fn current_state(dag: &Dag, finality: &mut BTreeMap<Hash, bool>) -> Value {
    fold_state_at(dag, &all_heads(dag), finality)
}

/// Serialize the folded SM state (a dynamic `Value`) to bytes for the read verbs. A
/// byte-state SM yields its raw bytes; a typed-state SM yields the Graph-ABI encoding.
fn state_bytes(v: Value) -> Vec<u8> {
    match v {
        Value::List { ref items, .. } if items.iter().all(|i| matches!(i, Value::U8(_))) => {
            Vec::<u8>::try_from(v).unwrap_or_default()
        }
        other => packr_guest::encode(&other).unwrap_or_default(),
    }
}

/// Author an event on this node's chain, PRE-VALIDATED against the current frontier.
fn author_event(
    dag: &mut Dag,
    signing_key: &SigningKey,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
    now: u64,
) -> Result<Event, String> {
    let author = signing_key.verifying_key().to_bytes();
    // Self-parent from our CURRENT own head *in the DAG* (never a lagging threaded head);
    // grafting other own heads heals a chain that already forked.
    let mut own: Vec<Hash> = dag.heads_of(&author).into_iter().collect();
    let self_parent = own.pop();
    let mut refs = foreign_heads(dag, &author);
    refs.extend(own);
    let ev = Event::sign(signing_key, now, self_parent, refs, payload);
    if !ev.payload.is_empty() {
        let state = current_state(dag, finality);
        let p = decode_payload(&ev.payload).ok_or_else(|| "undecodable payload".to_string())?;
        sm_validate(ev.event_hash().to_vec(), author.to_vec(), ev.timestamp, p, state)?;
    }
    match dag.ingest(ev.clone())? {
        true => Ok(ev),
        false => Err("authored event buffered (missing dep)".to_string()),
    }
}

/// Author a payload event, ACK the requester, and broadcast it.
#[allow(clippy::too_many_arguments)]
fn author_and_broadcast(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    conn_id: &str,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
    out: &mut Outbox,
    now: u64,
) -> Option<Hash> {
    match author_event(dag, signing_key, payload, finality, now) {
        Ok(ev) => {
            let h = ev.event_hash();
            out.push(Effect::Send(conn_id.to_string(), encode_ack(&h, true, "")));
            broadcast(conns, conn_id, &encode_deliver(&ev.encode()), out);
            Some(h)
        }
        Err(e) => {
            out.push(Effect::Send(conn_id.to_string(), encode_ack(&[0u8; 32], false, &e)));
            self_head
        }
    }
}

/// Author an event on our chain and gossip it, returning the new self_head + a result
/// carrying the event hash. Sends no ACK — the caller returns the ack itself.
#[allow(clippy::too_many_arguments)]
fn run_command(
    dag: &mut Dag,
    conns: &BTreeMap<String, ConnState>,
    signing_key: &SigningKey,
    self_head: Option<Hash>,
    payload: Vec<u8>,
    finality: &mut BTreeMap<Hash, bool>,
    out: &mut Outbox,
    now: u64,
) -> (Option<Hash>, Result<Hash, String>) {
    match author_event(dag, signing_key, payload, finality, now) {
        Ok(ev) => {
            let h = ev.event_hash();
            broadcast(conns, "", &encode_deliver(&ev.encode()), out);
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
fn broadcast(conns: &BTreeMap<String, ConnState>, exclude: &str, frame: &[u8], out: &mut Outbox) {
    for (cid, cs) in conns {
        if cid == exclude {
            continue;
        }
        if matches!(cs.phase, Phase::Authed { .. }) {
            out.push(Effect::Send(cid.clone(), frame.to_vec()));
        }
    }
}

/// Emit the one-shot Ready signal to the subscribed app the first time one registers.
fn maybe_emit_ready(app_id: &str, ready_sent: bool, out: &mut Outbox) -> bool {
    if ready_sent || app_id.is_empty() {
        return ready_sent;
    }
    out.push(Effect::App(app_id.to_string(), api::encode_ready()));
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

/// Fold the SM over the causal past of `frontier` and return the state. Finality and
/// application are SEPARATE (confluent): an event is FINAL iff it validates against its
/// OWN ancestry (judged once, memoized), and the fold then APPLIES every final event
/// unconditionally in topo order — never re-validating against the running merged state.
fn fold_state_at(dag: &Dag, frontier: &[Hash], finality: &mut BTreeMap<Hash, bool>) -> Value {
    ensure_finality(dag, finality);
    apply_final(dag, frontier, finality)
}

/// Decide finality for every admitted event not already cached, in topo order so each
/// event's ancestry is settled when reached. Immutable once decided.
fn ensure_finality(dag: &Dag, finality: &mut BTreeMap<Hash, bool>) {
    for h in dag.ordered() {
        if finality.contains_key(&h) {
            continue;
        }
        let Some(ev) = dag.events.get(&h) else { continue };
        let ancestry_state = apply_final(dag, &deps_of(ev), finality);
        let final_here = ev.payload.is_empty()
            || decode_payload(&ev.payload)
                .map(|p| sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, p, ancestry_state).is_ok())
                .unwrap_or(false); // undecodable payload → not final (inert)
        finality.insert(h, final_here);
    }
}

/// Fold only the events in `ancestors_of(frontier)` marked final, in topo order.
fn apply_final(dag: &Dag, frontier: &[Hash], is_final: &BTreeMap<Hash, bool>) -> Value {
    let mut state = sm_initial_state();
    for h in dag.topo_sort(&dag.ancestors_of(frontier)) {
        if is_final.get(&h).copied().unwrap_or(false) {
            if let Some(ev) = dag.events.get(&h) {
                if !ev.payload.is_empty() {
                    if let Some(p) = decode_payload(&ev.payload) {
                        state = sm_apply(h.to_vec(), ev.author.to_vec(), ev.timestamp, p, state);
                    }
                }
            }
        }
    }
    state
}

/// Emit the Interface 3 stream: a `finalized` dag-node for each newly-final payload event
/// (to authed TCP peers AND the subscribed app), or a fail-loud `conflict` for an admitted
/// event invalid against its own ancestry (the checked "conflict-free" safety net).
fn deliver_committed(
    dag: &Dag,
    conns: &BTreeMap<String, ConnState>,
    app_id: &str,
    delivered: &mut BTreeSet<Hash>,
    finality: &mut BTreeMap<Hash, bool>,
    out: &mut Outbox,
) {
    ensure_finality(dag, finality);
    for h in dag.ordered() {
        let Some(ev) = dag.events.get(&h) else {
            continue;
        };
        let deps = deps_of(ev);
        if !finality.get(&h).copied().unwrap_or(false) {
            if delivered.insert(h) {
                let state_at = apply_final(dag, &deps, finality);
                let reason = match decode_payload(&ev.payload) {
                    Some(p) => sm_validate(h.to_vec(), ev.author.to_vec(), ev.timestamp, p, state_at)
                        .err()
                        .unwrap_or_else(|| "invalid against ancestry".to_string()),
                    None => "undecodable payload".to_string(),
                };
                log(format!("[mesh] CONFLICT {}: {}", hex(&h), reason));
                let frame = encode_conflict(&h, &reason);
                for (cid, cs) in conns {
                    if matches!(cs.phase, Phase::Authed { .. }) {
                        out.push(Effect::Send(cid.clone(), frame.clone()));
                    }
                }
            }
            continue;
        }
        if ev.payload.is_empty() || !delivered.insert(h) {
            continue;
        }
        let frame = encode_finalized(&h, &ev.author, ev.timestamp, &deps, &ev.payload);
        for (cid, cs) in conns {
            if matches!(cs.phase, Phase::Authed { .. }) {
                out.push(Effect::Send(cid.clone(), frame.clone()));
            }
        }
        if !app_id.is_empty() {
            out.push(Effect::App(app_id.to_string(), frame.clone()));
        }
    }
}

/// Derived (not crypto-random) challenge nonce — adequate as a possession check.
fn challenge_nonce(conn_id: &str, now: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(now.to_be_bytes());
    h.update(conn_id.as_bytes());
    h.finalize().into()
}
