//! Wire frame protocol between mesh nodes (and app clients) — v3.
//!
//! Frame = LEN(u32 BE) + KIND(u8) + PAYLOAD(bytes).
//!
//! Handshake (membership checked against static config):
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
//! Post-handshake — app client protocol (client ↔ node):
//!   0x11 SUBMIT      payload bytes — "author this as an event for me"
//!   0x91 ACK         event_hash[32] + ok_byte + utf8 err if !ok
//!   0x92 NOTIFY      from[32] + body — optimistic message delivery

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::event::{Hash, PubKey};

// client/dialer → node
pub const FRAME_HELLO: u8 = 0x01;
pub const FRAME_AUTH: u8 = 0x02;
pub const FRAME_DELIVER: u8 = 0x10;
pub const FRAME_SUBMIT: u8 = 0x11;
pub const FRAME_INTRODUCE: u8 = 0x12; // author Introduce{pubkey} — admit a member
pub const FRAME_DEPART: u8 = 0x13; // author Depart{self} — leave the network
pub const FRAME_FRONTIER: u8 = 0x20;
pub const FRAME_WANT: u8 = 0x21;

// node → client/dialer
pub const FRAME_CHALLENGE: u8 = 0x80;
pub const FRAME_ACCEPTED: u8 = 0x81;
pub const FRAME_REJECTED: u8 = 0x82;
pub const FRAME_ACK: u8 = 0x91;
pub const FRAME_NOTIFY: u8 = 0x92;

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

pub fn encode_notify(from: &PubKey, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(32 + body.len());
    payload.extend_from_slice(from);
    payload.extend_from_slice(body);
    encode_frame(FRAME_NOTIFY, &payload)
}

/// Decode a NOTIFY payload into `(from, body)`. None if too short.
/// Client-facing helper (the mesh sends NOTIFY; clients parse it).
#[allow(dead_code)]
pub fn decode_notify(payload: &[u8]) -> Option<(PubKey, &[u8])> {
    if payload.len() < 32 {
        return None;
    }
    let mut from = [0u8; 32];
    from.copy_from_slice(&payload[..32]);
    Some((from, &payload[32..]))
}

/// Encode a hash list (used by ACCEPTED, FRONTIER, WANT): u16 count + N*32.
pub fn encode_hashes(kind: u8, hashes: &[Hash]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + hashes.len() * 32);
    payload.extend_from_slice(&(hashes.len() as u16).to_be_bytes());
    for h in hashes {
        payload.extend_from_slice(h);
    }
    encode_frame(kind, &payload)
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
