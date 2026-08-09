//! Per-connection state: the handshake phase machine plus the receive buffer.
//!
//! An inbound connection (a peer dialed us) starts in `AwaitingHello`, advances
//! through `AwaitingAuth` once the peer sends a valid pubkey, and reaches `Authed`
//! after it signs the challenge. An outbound connection (we dialed a peer) runs the
//! client side of the handshake **event-driven** — `AwaitingChallenge` →
//! `AwaitingAccepted` → `Authed` — because a pure core can't block on `receive`, so
//! both directions share the same async phase machine in `on_data`.

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
    // ---- server side: a peer dialed us ----
    AwaitingHello,
    AwaitingAuth { pubkey_hex: String, nonce_hex: String },
    // ---- client side: we dialed a peer (handshake runs event-driven) ----
    /// Sent HELLO, awaiting the peer's CHALLENGE. `peer_pubkey_hex` is the peer's
    /// expected key (from dial config), recorded once we reach `Authed`.
    AwaitingChallenge { peer_pubkey_hex: String },
    /// Sent AUTH, awaiting the peer's ACCEPTED(frontier).
    AwaitingAccepted { peer_pubkey_hex: String },
    // ---- both sides ----
    Authed { pubkey_hex: String },
}

impl ConnState {
    pub fn awaiting_hello() -> Self {
        Self { phase: Phase::AwaitingHello, buf: Vec::new() }
    }

    /// A freshly dialed outbound connection: we've sent HELLO and await the peer's
    /// CHALLENGE. `peer_pubkey_hex` is the peer's expected key (from dial config).
    pub fn dialing(peer_pubkey_hex: String) -> Self {
        Self { phase: Phase::AwaitingChallenge { peer_pubkey_hex }, buf: Vec::new() }
    }

    pub fn recv_buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn recv_buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}
