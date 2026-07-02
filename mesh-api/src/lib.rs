//! App ↔ node control API carried over theater's `message-server` (co-located
//! link). This is NOT the signed wire/event format (event.rs) nor the node↔node
//! gossip framing (wire.rs) — it's the local, trusted envelope an app actor uses
//! to drive its own mesh node.
//!
//! The app is the node's supervisor, so there's no handshake and no signing: the
//! node authors every event under its own key. Three moving parts:
//!
//!   - **Command** (app → node, via `request`): Submit / Introduce / Depart /
//!     Register. A single leading kind byte + body.
//!   - **Ack** (node → app, the `request` response): ok + event hash, or an error
//!     string. Register acks with an all-zero hash.
//!   - **Delivery** (node → app, via `send`): `from[32] || body` — the committed
//!     payload and its author. The NOTIFY analog for the co-located link.
//!
//! Message-server delivers whole messages, so — unlike wire.rs — there's no
//! length prefix here.
//!
//! Shared by the node (which decodes commands / encodes acks + deliveries) and
//! by app actors (which encode commands / decode acks + deliveries).

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

/// A 32-byte content hash (event id), matching the node's `event::Hash`.
pub type Hash = [u8; 32];
/// A 32-byte ed25519 public key (node identity), matching `event::PubKey`.
pub type PubKey = [u8; 32];

// command kinds (app → node)
pub const CMD_SUBMIT: u8 = 0x01; // body = payload bytes
pub const CMD_INTRODUCE: u8 = 0x02; // body = pubkey[32]
pub const CMD_DEPART: u8 = 0x03; // body = (none)
pub const CMD_REGISTER: u8 = 0x04; // body = app actor-id (utf8) for delivery

/// A decoded command from an app actor.
pub enum Command {
    Submit(Vec<u8>),
    Introduce(PubKey),
    Depart,
    Register(String),
}

pub fn encode_submit(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + payload.len());
    v.push(CMD_SUBMIT);
    v.extend_from_slice(payload);
    v
}

pub fn encode_introduce(node: &PubKey) -> Vec<u8> {
    let mut v = Vec::with_capacity(33);
    v.push(CMD_INTRODUCE);
    v.extend_from_slice(node);
    v
}

pub fn encode_depart() -> Vec<u8> {
    alloc::vec![CMD_DEPART]
}

pub fn encode_register(app_id: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + app_id.len());
    v.push(CMD_REGISTER);
    v.extend_from_slice(app_id.as_bytes());
    v
}

/// Decode a command envelope. `None` if empty or the shape is wrong for the kind.
pub fn decode_command(bytes: &[u8]) -> Option<Command> {
    let (kind, body) = bytes.split_first()?;
    match *kind {
        CMD_SUBMIT => Some(Command::Submit(body.to_vec())),
        CMD_INTRODUCE => {
            if body.len() != 32 {
                return None;
            }
            let mut pk = [0u8; 32];
            pk.copy_from_slice(body);
            Some(Command::Introduce(pk))
        }
        CMD_DEPART => Some(Command::Depart),
        CMD_REGISTER => Some(Command::Register(String::from_utf8_lossy(body).to_string())),
        _ => None,
    }
}

// ---- ack (node → app, request response) ----

/// `ok(1) || hash[32]` on success; `err(0) || utf8` on failure.
pub fn encode_ack(ok: bool, hash: &Hash, err: &str) -> Vec<u8> {
    if ok {
        let mut v = Vec::with_capacity(33);
        v.push(1);
        v.extend_from_slice(hash);
        v
    } else {
        let mut v = Vec::with_capacity(1 + err.len());
        v.push(0);
        v.extend_from_slice(err.as_bytes());
        v
    }
}

/// Decode an ack into `Ok(event_hash)` or `Err(reason)`. Client-side helper.
pub fn decode_ack(bytes: &[u8]) -> Result<Hash, String> {
    match bytes.split_first() {
        Some((1, h)) if h.len() == 32 => {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(h);
            Ok(hash)
        }
        Some((0, e)) => Err(String::from_utf8_lossy(e).to_string()),
        _ => Err("malformed ack".to_string()),
    }
}

// ---- delivery (node → app, via send) ----

/// `from[32] || body` — a committed payload and its author.
pub fn encode_delivery(from: &PubKey, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + body.len());
    v.extend_from_slice(from);
    v.extend_from_slice(body);
    v
}

/// Decode a delivery into `(from, body)`. Client-side helper for `handle_send`.
pub fn decode_delivery(bytes: &[u8]) -> Option<(PubKey, Vec<u8>)> {
    if bytes.len() < 32 {
        return None;
    }
    let mut from = [0u8; 32];
    from.copy_from_slice(&bytes[..32]);
    Some((from, bytes[32..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_round_trips() {
        match decode_command(&encode_submit(b"hello")) {
            Some(Command::Submit(p)) => assert_eq!(p, b"hello"),
            _ => panic!("expected Submit"),
        }
    }

    #[test]
    fn introduce_round_trips_and_rejects_bad_len() {
        match decode_command(&encode_introduce(&[7u8; 32])) {
            Some(Command::Introduce(pk)) => assert_eq!(pk, [7u8; 32]),
            _ => panic!("expected Introduce"),
        }
        // a truncated pubkey body is rejected, not silently accepted.
        assert!(matches!(decode_command(&[CMD_INTRODUCE, 1, 2, 3]), None));
    }

    #[test]
    fn depart_and_register_round_trip() {
        assert!(matches!(decode_command(&encode_depart()), Some(Command::Depart)));
        match decode_command(&encode_register("actor-123")) {
            Some(Command::Register(id)) => assert_eq!(id, "actor-123"),
            _ => panic!("expected Register"),
        }
    }

    #[test]
    fn empty_command_is_none() {
        assert!(decode_command(&[]).is_none());
    }

    #[test]
    fn ack_round_trips_both_ways() {
        assert_eq!(decode_ack(&encode_ack(true, &[9u8; 32], "")), Ok([9u8; 32]));
        assert_eq!(decode_ack(&encode_ack(false, &[0u8; 32], "nope")), Err("nope".to_string()));
    }

    #[test]
    fn delivery_round_trips() {
        let (from, body) = decode_delivery(&encode_delivery(&[3u8; 32], b"hi")).unwrap();
        assert_eq!(from, [3u8; 32]);
        assert_eq!(body, b"hi");
    }
}
