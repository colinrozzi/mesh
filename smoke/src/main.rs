//! mesh v3 smoke test — single node, message-passing reducer.
//!
//! Scenario (single member = the mesh itself):
//!   - client authenticates as the member node
//!   - client SUBMITs a message addressed to bob
//!   - the node authors it; with one member it finalizes immediately
//!   - committed delivery NOTIFYs the message back; client observes it

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const ADDR: &str = "127.0.0.1:9447";

const FRAME_HELLO: u8 = 0x01;
const FRAME_AUTH: u8 = 0x02;
const FRAME_SUBMIT: u8 = 0x11;
const FRAME_CHALLENGE: u8 = 0x80;
const FRAME_ACCEPTED: u8 = 0x81;
const FRAME_ACK: u8 = 0x91;
const FRAME_NOTIFY: u8 = 0x92;

/// The mesh derives its key from `node_seed` = sha256(node_seed). The manifest
/// sets node_seed = "mesh-smoke-v1:root", so the member key is sha256 of that.
fn node_key() -> SigningKey {
    seeded_key("root")
}

fn seeded_key(label: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(b"mesh-smoke-v1:");
    h.update(label.as_bytes());
    SigningKey::from_bytes(&h.finalize().into())
}

fn hex(b: &[u8]) -> String {
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

fn read_frame(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    s.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf)?;
    Ok((buf[0], buf[1..].to_vec()))
}

/// Read frames until one of `kind` arrives, ignoring others (FRONTIER, etc.).
fn read_until(s: &mut TcpStream, kind: u8) -> std::io::Result<Vec<u8>> {
    loop {
        let (k, payload) = read_frame(s)?;
        if k == kind {
            return Ok(payload);
        }
    }
}

fn handshake(signer: &SigningKey) -> std::io::Result<TcpStream> {
    let mut s = TcpStream::connect(ADDR)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    let pk = signer.verifying_key().to_bytes();
    s.write_all(&encode_frame(FRAME_HELLO, &pk))?;
    let nonce = read_until(&mut s, FRAME_CHALLENGE)?;
    let sig = signer.sign(&nonce).to_bytes();
    s.write_all(&encode_frame(FRAME_AUTH, &sig))?;
    read_until(&mut s, FRAME_ACCEPTED)?;
    Ok(s)
}

fn main() {
    let node = node_key();
    let bob = seeded_key("bob");
    let node_pk = node.verifying_key().to_bytes();
    let bob_pk = bob.verifying_key().to_bytes();

    if std::env::args().any(|a| a == "--pubkeys") {
        println!("node (member) = {}", hex(&node_pk));
        println!("bob (recipient) = {}", hex(&bob_pk));
        return;
    }

    let mut conn = handshake(&node).expect("handshake");
    println!("✓ authenticated as member node");

    // SUBMIT a message addressed to bob: payload = recipient[32] || body.
    let body = b"hello bob, this is a message via mesh v3";
    let mut payload = bob_pk.to_vec();
    payload.extend_from_slice(body);
    conn.write_all(&encode_frame(FRAME_SUBMIT, &payload)).unwrap();

    let ack = read_until(&mut conn, FRAME_ACK).expect("ack");
    assert_eq!(ack.len(), 33, "ack should be hash[32] + ok");
    assert_eq!(ack[32], 1, "submit not ok");
    println!("✓ message submitted and ACKed (event {}…)", &hex(&ack[..32])[..16]);

    // Committed delivery: NOTIFY = from[32] || body.
    let notify = read_until(&mut conn, FRAME_NOTIFY).expect("notify");
    assert!(notify.len() >= 32, "notify too short");
    let from = &notify[..32];
    let got = &notify[32..];
    assert_eq!(from, &node_pk, "message should be from the authoring node");
    assert_eq!(got, body, "delivered body mismatch");
    println!("✓ received committed delivery: {:?}", String::from_utf8_lossy(got));

    println!("\nALL CHECKS PASSED");
}
