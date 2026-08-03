//! Wire frame protocol between mesh nodes (and app clients) — v3.
//!
//! Frame = LEN(u32 BE) + KIND(u8) + PAYLOAD(bytes).
//!
//! Handshake (identity proof only — transport is membership-permissive; the SM
//! gates membership, see DESIGN-rsm.md):
//!   0x01 HELLO       pubkey[32]
//!   0x02 AUTH        signature[64]                 (signs the challenge nonce)
//!   0x80 CHALLENGE   nonce[32]
//!   0x81 ACCEPTED    frontier (hash-list: my heads)
//!   0x82 REJECTED    utf8 reason
//!
//! Post-handshake — gossip (node ↔ node):
//!   0x10 DELIVER     one encoded Event (broadcast / backfill response)
//!   0x20 FRONTIER    hash-list — "here are my heads"
//!   0x21 WANT        hash-list — "send me these events"
//!
//! Post-handshake — app client protocol (client ↔ node). This is the TCP binding
//! of DESIGN-rsm.md Interfaces 2 (`mesh`, drive+inspect requests) and 3 (the
//! node-emitted settled-event stream):
//!   0x11 SUBMIT      payload bytes — Interface 2 `author` (pre-validated)
//!   0x30 QUERY       [qkind u8][arg] — Interface 2 read verbs (see Q_* below)
//!   0x91 ACK         event_hash[32] + ok_byte + utf8 err if !ok
//!   0xa0 QUERY-REPLY [qkind u8][result bytes] — the QUERY response
//!   0x93 FINALIZED   dag-node — Interface 3 `finalized` (one newly-final node)
//!   0x94 STRANDED    own-event hash[32] — Interface 3 `stranded`
//!   0x95 CONFLICT    dag-node id[32] + utf8 reason — Interface 3 `conflict`
//!
//! A `dag-node` on the wire = id[32] ++ author[32] ++ timestamp(u64 BE) ++
//! (u16 dep-count ++ deps[32*n]) ++ payload(rest) — the sm-event plus its `deps`.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::event::{Hash, PubKey};

// client/dialer → node
pub const FRAME_HELLO: u8 = 0x01;
pub const FRAME_AUTH: u8 = 0x02;
pub const FRAME_DELIVER: u8 = 0x10;
pub const FRAME_SUBMIT: u8 = 0x11;
pub const FRAME_QUERY: u8 = 0x30;
pub const FRAME_FRONTIER: u8 = 0x20;
pub const FRAME_WANT: u8 = 0x21;

// node → client/dialer
pub const FRAME_CHALLENGE: u8 = 0x80;
pub const FRAME_ACCEPTED: u8 = 0x81;
pub const FRAME_REJECTED: u8 = 0x82;
pub const FRAME_ACK: u8 = 0x91;
pub const FRAME_FINALIZED: u8 = 0x93;
pub const FRAME_STRANDED: u8 = 0x94;
pub const FRAME_CONFLICT: u8 = 0x95;
pub const FRAME_QUERY_REPLY: u8 = 0xa0;

// Interface 2 query sub-kinds (the byte after FRAME_QUERY / FRAME_QUERY_REPLY).
pub const Q_STATE: u8 = 0; // current-state : arg none  → state bytes
pub const Q_STATUS: u8 = 1; // event-status : arg hash  → [status u8]
pub const Q_WITNESSES: u8 = 2; // witnesses  : arg hash  → pubkey-list
pub const Q_ANCESTRY: u8 = 3; // ancestry   : arg hash  → hash-list

// event-status wire values (DESIGN-rsm.md `event-status` enum).
pub const STATUS_UNKNOWN: u8 = 0;
pub const STATUS_PENDING: u8 = 1;
pub const STATUS_FINALIZED: u8 = 2;
pub const STATUS_STRANDED: u8 = 3;

pub fn encode_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 1) as u32;
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    out
}

pub fn encode_hello(pubkey: &PubKey) -> Vec<u8> {
    encode_frame(FRAME_HELLO, pubkey)
}

pub fn encode_auth(sig: &[u8; 64]) -> Vec<u8> {
    encode_frame(FRAME_AUTH, sig)
}

pub fn encode_challenge(nonce: &[u8; 32]) -> Vec<u8> {
    encode_frame(FRAME_CHALLENGE, nonce)
}

pub fn encode_rejected(msg: &str) -> Vec<u8> {
    encode_frame(FRAME_REJECTED, msg.as_bytes())
}

pub fn encode_deliver(event_bytes: &[u8]) -> Vec<u8> {
    encode_frame(FRAME_DELIVER, event_bytes)
}

pub fn encode_ack(event_hash: &Hash, ok: bool, err: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(33 + err.len());
    payload.extend_from_slice(event_hash);
    payload.push(if ok { 1 } else { 0 });
    if !ok {
        payload.extend_from_slice(err.as_bytes());
    }
    encode_frame(FRAME_ACK, &payload)
}

/// Encode a `dag-node` body: id ++ author ++ ts ++ (u16 deps ++ deps) ++ payload.
/// Shared shape for FINALIZED (and the node half of CONFLICT).
pub fn encode_dag_node(
    id: &Hash,
    author: &PubKey,
    timestamp: u64,
    deps: &[Hash],
    payload: &[u8],
) -> Vec<u8> {
    let mut b = Vec::with_capacity(32 + 32 + 8 + 2 + deps.len() * 32 + payload.len());
    b.extend_from_slice(id);
    b.extend_from_slice(author);
    b.extend_from_slice(&timestamp.to_be_bytes());
    b.extend_from_slice(&(deps.len() as u16).to_be_bytes());
    for d in deps {
        b.extend_from_slice(d);
    }
    b.extend_from_slice(payload);
    b
}

/// A newly-finalized `dag-node` (Interface 3 `finalized`, one node per frame).
pub fn encode_finalized(id: &Hash, author: &PubKey, timestamp: u64, deps: &[Hash], payload: &[u8]) -> Vec<u8> {
    encode_frame(FRAME_FINALIZED, &encode_dag_node(id, author, timestamp, deps, payload))
}

/// Interface 3 `stranded` — your own event lost (never fires for honest v0 peers).
#[allow(dead_code)]
pub fn encode_stranded(own_event: &Hash) -> Vec<u8> {
    encode_frame(FRAME_STRANDED, own_event)
}

/// Interface 3 `conflict` — a genuine conflict surfaced (fail-loud net in v0):
/// the offending node's id ++ a utf8 reason.
pub fn encode_conflict(id: &Hash, reason: &str) -> Vec<u8> {
    let mut b = Vec::with_capacity(32 + reason.len());
    b.extend_from_slice(id);
    b.extend_from_slice(reason.as_bytes());
    encode_frame(FRAME_CONFLICT, &b)
}

/// A QUERY-REPLY frame: the sub-kind byte then the result bytes.
pub fn encode_query_reply(qkind: u8, result: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + result.len());
    b.push(qkind);
    b.extend_from_slice(result);
    encode_frame(FRAME_QUERY_REPLY, &b)
}

/// A bare hash-list body (u16 count + N*32), no frame wrapper — used inside
/// QUERY-REPLY results (witnesses / ancestry). `decode_hashes` is its inverse.
pub fn encode_hash_list_body(hashes: &[Hash]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + hashes.len() * 32);
    payload.extend_from_slice(&(hashes.len() as u16).to_be_bytes());
    for h in hashes {
        payload.extend_from_slice(h);
    }
    payload
}

/// Encode a hash list (used by ACCEPTED, FRONTIER, WANT): u16 count + N*32.
pub fn encode_hashes(kind: u8, hashes: &[Hash]) -> Vec<u8> {
    encode_frame(kind, &encode_hash_list_body(hashes))
}


pub fn decode_hashes(payload: &[u8]) -> Vec<Hash> {
    if payload.len() < 2 {
        return Vec::new();
    }
    let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut out = Vec::with_capacity(n);
    let mut pos = 2;
    for _ in 0..n {
        if pos + 32 > payload.len() {
            break;
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&payload[pos..pos + 32]);
        pos += 32;
        out.push(h);
    }
    out
}

pub struct ParsedFrame {
    pub kind: u8,
    pub payload: Vec<u8>,
    pub total_len: usize, // bytes consumed from input
}

/// Try to parse one frame from the front of `buf`. `Ok(None)` if not enough
/// bytes yet; `Err` for malformed frames.
pub fn try_parse_frame(buf: &[u8]) -> Result<Option<ParsedFrame>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err("frame length zero".into());
    }
    if len > 16 * 1024 * 1024 {
        return Err(format!("frame too large: {}", len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let kind = buf[4];
    let payload = buf[5..4 + len].to_vec();
    Ok(Some(ParsedFrame { kind, payload, total_len: 4 + len }))
}
