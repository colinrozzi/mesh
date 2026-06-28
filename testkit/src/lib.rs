//! Shared client helpers for mesh integration tests — the wire protocol from a
//! client's perspective. Used by `smoke` and `multi-node-test`.

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const HELLO: u8 = 0x01;
const AUTH: u8 = 0x02;
const SUBMIT: u8 = 0x11;
const CHALLENGE: u8 = 0x80;
const ACCEPTED: u8 = 0x81;
const ACK: u8 = 0x91;
const NOTIFY: u8 = 0x92;

/// Derive a signing key from a seed string (sha256), matching the mesh's own
/// key derivation from `node_seed`.
pub fn seeded_key(seed: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    SigningKey::from_bytes(&h.finalize().into())
}

pub fn pubkey(sk: &SigningKey) -> [u8; 32] {
    sk.verifying_key().to_bytes()
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn encode_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 1) as u32;
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    out
}

fn read_frame(s: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    s.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf)?;
    Ok((buf[0], buf[1..].to_vec()))
}

fn read_until(s: &mut TcpStream, kind: u8) -> io::Result<Vec<u8>> {
    loop {
        let (k, payload) = read_frame(s)?;
        if k == kind {
            return Ok(payload);
        }
    }
}

fn err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

/// A test client speaking the mesh wire protocol.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect to `addr` and authenticate as `signer` (must be a member).
    pub fn connect(addr: &str, signer: &SigningKey) -> io::Result<Client> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(8)))?;
        stream.write_all(&encode_frame(HELLO, &pubkey(signer)))?;
        let nonce = read_until(&mut stream, CHALLENGE)?;
        let sig = signer.sign(&nonce).to_bytes();
        stream.write_all(&encode_frame(AUTH, &sig))?;
        read_until(&mut stream, ACCEPTED)?;
        Ok(Client { stream })
    }

    /// SUBMIT a message (`recipient[32] || body`); returns the event hash.
    pub fn submit(&mut self, recipient: &[u8; 32], body: &[u8]) -> io::Result<[u8; 32]> {
        let mut payload = recipient.to_vec();
        payload.extend_from_slice(body);
        self.stream.write_all(&encode_frame(SUBMIT, &payload))?;
        let ack = read_until(&mut self.stream, ACK)?;
        if ack.len() != 33 || ack[32] != 1 {
            return Err(err(format!(
                "submit rejected: {}",
                String::from_utf8_lossy(ack.get(33..).unwrap_or(&[]))
            )));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&ack[..32]);
        Ok(h)
    }

    /// Wait for the next committed message delivery (NOTIFY) → `(from, body)`.
    pub fn recv_message(&mut self) -> io::Result<([u8; 32], Vec<u8>)> {
        let payload = read_until(&mut self.stream, NOTIFY)?;
        if payload.len() < 32 {
            return Err(err("NOTIFY too short".to_string()));
        }
        let mut from = [0u8; 32];
        from.copy_from_slice(&payload[..32]);
        Ok((from, payload[32..].to_vec()))
    }
}
