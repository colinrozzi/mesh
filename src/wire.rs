//! Wire frame protocol between mesh and connected actors.
//!
//! Frame = LEN(u32 BE) + KIND(u8) + PAYLOAD(bytes).
//!
//! Client → mesh:
//!   0x01 HELLO       pubkey[32]
//!   0x02 AUTH        signature[64]                   (signing the challenge)
//!   0x10 SUBMIT      event_bytes (Event::encode())
//!
//! Mesh → client:
//!   0x80 CHALLENGE   nonce[32]
//!   0x81 ACCEPTED    state_hash[32]
//!   0x82 REJECTED    utf8 error message
//!   0x90 DELIVERED   event_bytes (broadcast of any ingested event)
//!   0x91 ACK         event_hash[32] + ok_byte + utf8 error if !ok

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

// Client → mesh
pub const FRAME_HELLO: u8 = 0x01;
pub const FRAME_AUTH: u8 = 0x02;
pub const FRAME_SUBMIT: u8 = 0x10;

// Mesh → client
pub const FRAME_CHALLENGE: u8 = 0x80;
pub const FRAME_ACCEPTED: u8 = 0x81;
pub const FRAME_REJECTED: u8 = 0x82;
pub const FRAME_DELIVERED: u8 = 0x90;
pub const FRAME_ACK: u8 = 0x91;

pub fn encode_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 1) as u32;
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    out
}

pub fn encode_rejected(msg: &str) -> Vec<u8> {
    encode_frame(FRAME_REJECTED, msg.as_bytes())
}

pub fn encode_accepted(state_hash: &[u8; 32]) -> Vec<u8> {
    encode_frame(FRAME_ACCEPTED, state_hash)
}

pub fn encode_challenge(nonce: &[u8; 32]) -> Vec<u8> {
    encode_frame(FRAME_CHALLENGE, nonce)
}

pub fn encode_delivered(event_bytes: &[u8]) -> Vec<u8> {
    encode_frame(FRAME_DELIVERED, event_bytes)
}

pub fn encode_ack(event_hash: &[u8; 32], ok: bool, err: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(33 + err.len());
    payload.extend_from_slice(event_hash);
    payload.push(if ok { 1 } else { 0 });
    if !ok {
        payload.extend_from_slice(err.as_bytes());
    }
    encode_frame(FRAME_ACK, &payload)
}

pub struct ParsedFrame {
    pub kind: u8,
    pub payload: Vec<u8>,
    pub total_len: usize, // bytes consumed from input
}

/// Try to parse one frame from the front of `buf`. Returns Ok(None) if not
/// enough bytes yet. Returns Err for malformed frames.
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
    Ok(Some(ParsedFrame {
        kind,
        payload,
        total_len: 4 + len,
    }))
}
