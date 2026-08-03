//! Shared client helpers for mesh integration tests — the wire protocol from a
//! client's perspective. Used by `smoke` and `multi-node-test`.

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const HELLO: u8 = 0x01;
const AUTH: u8 = 0x02;
const DELIVER: u8 = 0x10; // node↔node gossip frame (a raw encoded Event)
const SUBMIT: u8 = 0x11;
const INTRODUCE: u8 = 0x12;
const DEPART: u8 = 0x13;
const QUERY: u8 = 0x30;
const CHALLENGE: u8 = 0x80;
const ACCEPTED: u8 = 0x81;
const ACK: u8 = 0x91;
const FINALIZED: u8 = 0x93; // Interface 3 `finalized` dag-node delivery
const CONFLICT: u8 = 0x95; // Interface 3 `conflict` (fail-loud)
const QUERY_REPLY: u8 = 0xa0;

// Interface 2 query sub-kinds + event-status values (mirror src/wire.rs).
const Q_STATE: u8 = 0;
const Q_STATUS: u8 = 1;
const Q_WITNESSES: u8 = 2;
const Q_ANCESTRY: u8 = 3;

/// event-status (DESIGN-rsm.md): 0 unknown · 1 pending · 2 finalized · 3 stranded.
pub const STATUS_UNKNOWN: u8 = 0;
pub const STATUS_PENDING: u8 = 1;
pub const STATUS_FINALIZED: u8 = 2;
pub const STATUS_STRANDED: u8 = 3;

/// A delivered `dag-node` (Interface 3): the sm-event plus its DAG `deps`.
#[derive(Debug, Clone)]
pub struct DagNode {
    pub id: [u8; 32],
    pub author: [u8; 32],
    pub timestamp: u64,
    pub deps: Vec<[u8; 32]>,
    pub payload: Vec<u8>,
}

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

fn sha256(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().into()
}

/// The canonical unsigned encoding of an event (mirrors `src/event.rs`):
/// author ++ ts(u64 BE) ++ self_parent(tag+hash) ++ refs(u16+N*32) ++ payload(u32+bytes).
fn encode_unsigned(
    author: &[u8; 32],
    timestamp: u64,
    self_parent: &Option<[u8; 32]>,
    refs: &[[u8; 32]],
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(author);
    out.extend_from_slice(&timestamp.to_be_bytes());
    match self_parent {
        None => out.push(0),
        Some(h) => {
            out.push(1);
            out.extend_from_slice(h);
        }
    }
    out.extend_from_slice(&(refs.len() as u16).to_be_bytes());
    for r in refs {
        out.extend_from_slice(r);
    }
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Forge a properly-SIGNED raw mesh event (for dishonest-peer / adversarial tests).
/// Returns `(event_hash, wire_bytes)`. The signature is valid — only the *content*
/// (e.g. a non-member authoring) is adversarial, which is what a dishonest peer can
/// do that honest pre-validated `author` never would. Structurally admissible iff
/// its deps are present at the target node.
pub fn forge_event(
    signer: &SigningKey,
    timestamp: u64,
    self_parent: Option<[u8; 32]>,
    refs: &[[u8; 32]],
    payload: &[u8],
) -> ([u8; 32], Vec<u8>) {
    let author = pubkey(signer);
    let unsigned = encode_unsigned(&author, timestamp, &self_parent, refs, payload);
    let signature = signer.sign(&sha256(&unsigned)).to_bytes();
    let mut bytes = unsigned;
    bytes.extend_from_slice(&signature);
    let event_hash = sha256(&bytes);
    (event_hash, bytes)
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

    /// SUBMIT a payload (opaque app bytes); returns the event hash. Any
    /// addressing lives in the payload — the substrate doesn't interpret it.
    pub fn submit(&mut self, payload: &[u8]) -> io::Result<[u8; 32]> {
        self.stream.write_all(&encode_frame(SUBMIT, payload))?;
        self.read_ack()
    }

    /// Ask the node to admit `node` as a member (author Introduce{node}).
    pub fn introduce(&mut self, node: &[u8; 32]) -> io::Result<[u8; 32]> {
        self.stream.write_all(&encode_frame(INTRODUCE, node))?;
        self.read_ack()
    }

    /// Ask the node to leave the network (author Depart{self}).
    pub fn depart(&mut self) -> io::Result<[u8; 32]> {
        self.stream.write_all(&encode_frame(DEPART, &[]))?;
        self.read_ack()
    }

    fn read_ack(&mut self) -> io::Result<[u8; 32]> {
        let ack = read_until(&mut self.stream, ACK)?;
        if ack.len() != 33 || ack[32] != 1 {
            return Err(err(format!(
                "rejected: {}",
                String::from_utf8_lossy(ack.get(33..).unwrap_or(&[]))
            )));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&ack[..32]);
        Ok(h)
    }

    /// Inject a raw encoded event as if gossiped from a peer (dishonest-peer path —
    /// bypasses the node's honest pre-validated `author`). No ack; the node ingests
    /// it structurally and the SM's `validate` decides its fate on the fold.
    pub fn gossip_raw(&mut self, event_bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(&encode_frame(DELIVER, event_bytes))
    }

    /// Wait for the next `conflict` (Interface 3): `(offending_event_id, reason)`.
    pub fn recv_conflict(&mut self) -> io::Result<([u8; 32], String)> {
        let payload = read_until(&mut self.stream, CONFLICT)?;
        if payload.len() < 32 {
            return Err(err("conflict frame too short".to_string()));
        }
        let mut id = [0u8; 32];
        id.copy_from_slice(&payload[..32]);
        Ok((id, String::from_utf8_lossy(&payload[32..]).to_string()))
    }

    /// Wait for the next finalized `dag-node` (Interface 3 `finalized`).
    pub fn recv_finalized(&mut self) -> io::Result<DagNode> {
        let payload = read_until(&mut self.stream, FINALIZED)?;
        decode_dag_node(&payload)
    }

    /// Back-compat: the next delivery as `(author, payload)`.
    pub fn recv_message(&mut self) -> io::Result<([u8; 32], Vec<u8>)> {
        let dn = self.recv_finalized()?;
        Ok((dn.author, dn.payload))
    }

    // ---- Interface 2 read verbs (each a QUERY → QUERY-REPLY round trip) ----

    /// `current-state` — the finalized SM fold (opaque bytes).
    pub fn current_state(&mut self) -> io::Result<Vec<u8>> {
        self.stream.write_all(&encode_frame(QUERY, &[Q_STATE]))?;
        self.read_query_reply(Q_STATE)
    }

    /// `event-status` — where `event` sits in its lifecycle (STATUS_*).
    pub fn event_status(&mut self, event: &[u8; 32]) -> io::Result<u8> {
        let mut body = Vec::with_capacity(33);
        body.push(Q_STATUS);
        body.extend_from_slice(event);
        self.stream.write_all(&encode_frame(QUERY, &body))?;
        let r = self.read_query_reply(Q_STATUS)?;
        Ok(*r.first().unwrap_or(&STATUS_UNKNOWN))
    }

    /// `witnesses` — the pubkeys whose chains causally see `event`.
    pub fn witnesses(&mut self, event: &[u8; 32]) -> io::Result<Vec<[u8; 32]>> {
        let mut body = Vec::with_capacity(33);
        body.push(Q_WITNESSES);
        body.extend_from_slice(event);
        self.stream.write_all(&encode_frame(QUERY, &body))?;
        Ok(decode_hash_list(&self.read_query_reply(Q_WITNESSES)?))
    }

    /// `ancestry` — the causal past (ancestor hashes) of `event`.
    pub fn ancestry(&mut self, event: &[u8; 32]) -> io::Result<Vec<[u8; 32]>> {
        let mut body = Vec::with_capacity(33);
        body.push(Q_ANCESTRY);
        body.extend_from_slice(event);
        self.stream.write_all(&encode_frame(QUERY, &body))?;
        Ok(decode_hash_list(&self.read_query_reply(Q_ANCESTRY)?))
    }

    fn read_query_reply(&mut self, expect: u8) -> io::Result<Vec<u8>> {
        let payload = read_until(&mut self.stream, QUERY_REPLY)?;
        let (qkind, rest) = payload.split_first().ok_or_else(|| err("empty query reply".to_string()))?;
        if *qkind != expect {
            return Err(err(format!("query reply kind {qkind:#x} != expected {expect:#x}")));
        }
        Ok(rest.to_vec())
    }
}

/// Decode a FINALIZED frame body into a `DagNode`.
fn decode_dag_node(p: &[u8]) -> io::Result<DagNode> {
    if p.len() < 32 + 32 + 8 + 2 {
        return Err(err("dag-node too short".to_string()));
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(&p[0..32]);
    let mut author = [0u8; 32];
    author.copy_from_slice(&p[32..64]);
    let timestamp = u64::from_be_bytes(p[64..72].try_into().unwrap());
    let n = u16::from_be_bytes([p[72], p[73]]) as usize;
    let mut pos = 74;
    let mut deps = Vec::with_capacity(n);
    for _ in 0..n {
        if pos + 32 > p.len() {
            break;
        }
        let mut d = [0u8; 32];
        d.copy_from_slice(&p[pos..pos + 32]);
        deps.push(d);
        pos += 32;
    }
    Ok(DagNode { id, author, timestamp, deps, payload: p[pos..].to_vec() })
}

/// Decode a bare hash-list body (u16 count + N*32) — witnesses / ancestry results.
fn decode_hash_list(b: &[u8]) -> Vec<[u8; 32]> {
    if b.len() < 2 {
        return Vec::new();
    }
    let n = u16::from_be_bytes([b[0], b[1]]) as usize;
    let mut out = Vec::with_capacity(n);
    let mut pos = 2;
    for _ in 0..n {
        if pos + 32 > b.len() {
            break;
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&b[pos..pos + 32]);
        out.push(h);
        pos += 32;
    }
    out
}

// ---- process harness: spawn mesh nodes via the theater runtime ----

pub const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
pub const MESH_DIR: &str = "/home/colin/work/actors/mesh";

/// Poll `addr` until it accepts a connection or `deadline` elapses.
pub fn wait_for_port(addr: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Spawn a mesh actor from `manifest_path`, logging to `log_path`.
pub fn spawn_mesh(manifest_path: &str, log_path: &str) -> Child {
    let log = std::fs::File::create(log_path).unwrap();
    let log_err = log.try_clone().unwrap();
    Command::new(THEATER_BIN)
        .args(["spawn", manifest_path])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("failed to spawn theater")
}

/// Write a mesh manifest with the given `initial_state` JSON + store.
pub fn write_manifest(path: &str, initial_state: &str, store_path: &str, store_id: &str) {
    let template = format!(
        r#"name = "{store_id}"
version = "0.1.0"
package = "{MESH_DIR}/target/wasm32-unknown-unknown/release/mesh.wasm"
static_package = true
initial_state = '{initial_state}'

[[handler]]
type = "runtime"

[[handler]]
type = "tcp"

[[handler]]
type = "timer"

[[handler]]
type = "message-server"

[[handler]]
type = "store"
base_path = "{store_path}"
store_id = "{store_id}"
"#
    );
    std::fs::write(path, template).expect("write manifest");
    let _ = std::fs::create_dir_all(store_path);
}
