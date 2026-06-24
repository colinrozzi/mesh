//! mesh v2 smoke test — DAG-based + Node/Mailbox roles.
//!
//! Scenario:
//!   - root (a Node) authenticates
//!   - root creates Mailbox(alice)         [event parent = GENESIS]
//!   - root creates Mailbox(bob)           [parent = E_alice]
//!   - alice + bob authenticate
//!   - alice sends to bob                  [parent = E_bob]
//!   - bob receives DELIVERED

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const ADDR: &str = "127.0.0.1:9447";

const FRAME_HELLO: u8 = 0x01;
const FRAME_AUTH: u8 = 0x02;
const FRAME_SUBMIT: u8 = 0x10;
const FRAME_CHALLENGE: u8 = 0x80;
const FRAME_ACCEPTED: u8 = 0x81;
const FRAME_DELIVERED: u8 = 0x90;
const FRAME_ACK: u8 = 0x91;

// Op kinds (must match event.rs)
const OP_NODE_INTRODUCE: u8 = 0;
const OP_MAILBOX_CREATE: u8 = 1;
const OP_SEND: u8 = 3;
// const OP_REVOKE: u8 = 2;
// const OP_WITNESS: u8 = 4;

const GENESIS_PARENT: [u8; 32] = [0u8; 32];

fn seeded_key(label: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(b"mesh-smoke-v1:");
    h.update(label.as_bytes());
    let seed = h.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    SigningKey::from_bytes(&bytes)
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
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
    let kind = buf[0];
    let payload = buf[1..].to_vec();
    Ok((kind, payload))
}

/// Build an event of a Member-Create shape (NodeIntroduce / MailboxCreate).
/// op_payload = subject[32] + u16(name.len) + name
fn make_member_event(
    signer: &SigningKey,
    parent: &[u8; 32],
    op_kind: u8,
    subject: &[u8; 32],
    name: &str,
) -> Vec<u8> {
    let author = signer.verifying_key().to_bytes();
    let mut op_payload = Vec::new();
    op_payload.extend_from_slice(subject);
    op_payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
    op_payload.extend_from_slice(name.as_bytes());

    let mut h = Sha256::new();
    h.update(parent);
    h.update(&author);
    h.update([op_kind]);
    h.update(&op_payload);
    let sig_hash: [u8; 32] = h.finalize().into();
    let sig = signer.sign(&sig_hash).to_bytes();

    let mut event = Vec::new();
    event.extend_from_slice(parent);
    event.extend_from_slice(&author);
    event.push(op_kind);
    event.extend_from_slice(&op_payload);
    event.extend_from_slice(&sig);
    event
}

/// op_payload = recipient[32] + u32(body.len) + body
fn make_send_event(
    signer: &SigningKey,
    parent: &[u8; 32],
    recipient: &[u8; 32],
    body: &[u8],
) -> Vec<u8> {
    let author = signer.verifying_key().to_bytes();
    let mut op_payload = Vec::new();
    op_payload.extend_from_slice(recipient);
    op_payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
    op_payload.extend_from_slice(body);

    let mut h = Sha256::new();
    h.update(parent);
    h.update(&author);
    h.update([OP_SEND]);
    h.update(&op_payload);
    let sig_hash: [u8; 32] = h.finalize().into();
    let sig = signer.sign(&sig_hash).to_bytes();

    let mut event = Vec::new();
    event.extend_from_slice(parent);
    event.extend_from_slice(&author);
    event.push(OP_SEND);
    event.extend_from_slice(&op_payload);
    event.extend_from_slice(&sig);
    event
}

fn event_hash(event_bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(event_bytes);
    h.finalize().into()
}

fn handshake(signer: &SigningKey) -> std::io::Result<TcpStream> {
    let mut s = TcpStream::connect(ADDR)?;
    s.set_read_timeout(Some(Duration::from_secs(3)))?;
    let pk = signer.verifying_key().to_bytes();
    s.write_all(&encode_frame(FRAME_HELLO, &pk))?;
    let (kind, payload) = read_frame(&mut s)?;
    assert_eq!(kind, FRAME_CHALLENGE, "expected CHALLENGE, got {:#x}", kind);
    let nonce = &payload;
    let sig = signer.sign(nonce).to_bytes();
    s.write_all(&encode_frame(FRAME_AUTH, &sig))?;
    let (kind, payload) = read_frame(&mut s)?;
    assert_eq!(kind, FRAME_ACCEPTED, "expected ACCEPTED, got {:#x}: {}",
        kind, String::from_utf8_lossy(&payload));
    Ok(s)
}

fn main() {
    let root = seeded_key("root");
    let alice = seeded_key("alice");
    let bob = seeded_key("bob");

    let root_pk = root.verifying_key().to_bytes();
    let alice_pk = alice.verifying_key().to_bytes();
    let bob_pk = bob.verifying_key().to_bytes();

    if std::env::args().any(|a| a == "--pubkeys") {
        println!("root (Node)    = {}", hex(&root_pk));
        println!("alice (Mailbox) = {}", hex(&alice_pk));
        println!("bob (Mailbox)   = {}", hex(&bob_pk));
        println!();
        println!("Set in manifest.toml:");
        println!("initial_state = '{{\"root_pubkey\":\"{}\"}}'", hex(&root_pk));
        return;
    }

    // root is the only Node; it authenticates and creates two Mailboxes.
    let mut root_conn = handshake(&root).expect("root handshake");
    println!("✓ root (Node) authenticated");

    let mb_alice = make_member_event(
        &root, &GENESIS_PARENT, OP_MAILBOX_CREATE, &alice_pk, "alice",
    );
    let mb_alice_hash = event_hash(&mb_alice);
    root_conn.write_all(&encode_frame(FRAME_SUBMIT, &mb_alice)).unwrap();
    let (kind, payload) = read_frame(&mut root_conn).unwrap();
    assert_eq!(kind, FRAME_ACK);
    assert_eq!(payload[32], 1, "MailboxCreate alice ng: {}",
        String::from_utf8_lossy(&payload[33..]));
    println!("✓ root created Mailbox(alice)  (event_hash = {}…)", &hex(&mb_alice_hash)[..16]);
    let (k, _) = read_frame(&mut root_conn).unwrap();
    assert_eq!(k, FRAME_DELIVERED);

    let mb_bob = make_member_event(
        &root, &mb_alice_hash, OP_MAILBOX_CREATE, &bob_pk, "bob",
    );
    let mb_bob_hash = event_hash(&mb_bob);
    root_conn.write_all(&encode_frame(FRAME_SUBMIT, &mb_bob)).unwrap();
    let (kind, payload) = read_frame(&mut root_conn).unwrap();
    assert_eq!(kind, FRAME_ACK);
    assert_eq!(payload[32], 1, "MailboxCreate bob ng: {}",
        String::from_utf8_lossy(&payload[33..]));
    println!("✓ root created Mailbox(bob)    (event_hash = {}…)", &hex(&mb_bob_hash)[..16]);
    let (k, _) = read_frame(&mut root_conn).unwrap();
    assert_eq!(k, FRAME_DELIVERED);

    // Mailboxes alice + bob authenticate. The handshake accepts any member,
    // Node or Mailbox.
    let mut alice_conn = handshake(&alice).expect("alice handshake");
    println!("✓ alice (Mailbox) authenticated");
    let mut bob_conn = handshake(&bob).expect("bob handshake");
    println!("✓ bob (Mailbox) authenticated");

    let send = make_send_event(
        &alice, &mb_bob_hash, &bob_pk, b"hello bob, this is alice via mesh v2",
    );
    let send_hash = event_hash(&send);
    alice_conn.write_all(&encode_frame(FRAME_SUBMIT, &send)).unwrap();
    let (kind, payload) = read_frame(&mut alice_conn).unwrap();
    assert_eq!(kind, FRAME_ACK);
    assert_eq!(payload[32], 1, "send ng: {}", String::from_utf8_lossy(&payload[33..]));
    println!("✓ alice's Send acked  (event_hash = {}…)", &hex(&send_hash)[..16]);

    // bob receives a DELIVERED frame.
    // Layout: parent[32] + author[32] + kind[1=3] + recipient[32] + len[4] + body + sig[64]
    let (kind, payload) = read_frame(&mut bob_conn).unwrap();
    assert_eq!(kind, FRAME_DELIVERED, "expected DELIVERED on bob, got {:#x}", kind);
    assert_eq!(payload[64], OP_SEND, "delivered event kind should be Send (3)");
    let body_len = u32::from_be_bytes([payload[97], payload[98], payload[99], payload[100]]) as usize;
    let body = &payload[101..101 + body_len];
    assert_eq!(body, b"hello bob, this is alice via mesh v2");
    println!("✓ bob received: {:?}", String::from_utf8_lossy(body));

    println!("\nALL CHECKS PASSED");
}
