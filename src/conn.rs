//! Per-connection state: the handshake phase machine plus the receive buffer.
//!
//! A connection starts in `AwaitingHello`, advances through `AwaitingAuth`
//! once it sends a valid pubkey, and reaches `Authed` after signing the
//! challenge. Outbound peer-mesh connections skip straight to `Authed` because
//! we run the client side of the handshake synchronously when we dial them.

use alloc::string::String;
use alloc::vec::Vec;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnState {
    pub phase: Phase,
    pub buf: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "phase")]
pub enum Phase {
    AwaitingHello,
    AwaitingAuth { pubkey_hex: String, nonce_hex: String },
    Authed { pubkey_hex: String },
}

impl ConnState {
    pub fn awaiting_hello() -> Self {
        Self { phase: Phase::AwaitingHello, buf: Vec::new() }
    }

    /// A connection that is already authenticated (used for outbound peer
    /// meshes we dialed and handshook synchronously).
    pub fn authed(pubkey_hex: String) -> Self {
        Self { phase: Phase::Authed { pubkey_hex }, buf: Vec::new() }
    }

    pub fn recv_buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn recv_buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}
