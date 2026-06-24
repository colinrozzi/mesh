//! mesh multi-node integration test.
//!
//! Spawns two mesh instances (Node A on :9447, Node B on :9448), each
//! pre-admitting the other as a Node in genesis. Node B opens an outbound
//! connection to Node A on startup. Then:
//!
//!   1. Test client connects to mesh A authenticated as Node A
//!   2. Test client connects to mesh B authenticated as Node B
//!   3. Test client (as A) submits Send(NodeB, "hello from A across the mesh")
//!   4. Mesh A broadcasts DELIVERED → mesh B's outbound conn receives it
//!   5. Mesh B ingests, broadcasts DELIVERED to its connections (including
//!      the test client connected as B)
//!   6. Test verifies the message arrived at the B-side client

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const FRAME_HELLO: u8 = 0x01;
const FRAME_AUTH: u8 = 0x02;
const FRAME_SUBMIT: u8 = 0x10;
const FRAME_CHALLENGE: u8 = 0x80;
const FRAME_ACCEPTED: u8 = 0x81;
const FRAME_DELIVERED: u8 = 0x90;
const FRAME_ACK: u8 = 0x91;
const OP_SEND: u8 = 3;
const GENESIS_PARENT: [u8; 32] = [0u8; 32];

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";

const ADDR_A: &str = "127.0.0.1:9447";
const ADDR_B: &str = "127.0.0.1:9448";

fn seeded_signing_key(seed: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    let bytes: [u8; 32] = h.finalize().into();
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
    Ok((buf[0], buf[1..].to_vec()))
}

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

fn handshake(addr: &str, signer: &SigningKey) -> std::io::Result<TcpStream> {
    let mut s = TcpStream::connect(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    let pk = signer.verifying_key().to_bytes();
    s.write_all(&encode_frame(FRAME_HELLO, &pk))?;
    let (kind, payload) = read_frame(&mut s)?;
    assert_eq!(kind, FRAME_CHALLENGE, "expected CHALLENGE, got {:#x}: {}",
        kind, String::from_utf8_lossy(&payload));
    let sig = signer.sign(&payload).to_bytes();
    s.write_all(&encode_frame(FRAME_AUTH, &sig))?;
    let (kind, payload) = read_frame(&mut s)?;
    assert_eq!(kind, FRAME_ACCEPTED, "expected ACCEPTED, got {:#x}: {}",
        kind, String::from_utf8_lossy(&payload));
    Ok(s)
}

fn wait_for_port(addr: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

fn spawn_mesh(manifest_path: &str, log_path: &str) -> Child {
    let log = std::fs::File::create(log_path).unwrap();
    let log_err = log.try_clone().unwrap();
    Command::new(THEATER_BIN)
        .args(["spawn", manifest_path])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err))
        .spawn()
        .expect("failed to spawn theater")
}

fn write_manifest(path: &str, initial_state: &str, store_path: &str) {
    let template = format!(
        r#"name = "mesh-multi-node"
version = "0.1.0"
package = "{mesh_dir}/target/wasm32-unknown-unknown/release/mesh.wasm"
static_package = true
initial_state = '{initial_state}'

[[handler]]
type = "runtime"

[[handler]]
type = "tcp"

[[handler]]
type = "timer"

[[handler]]
type = "store"
base_path = "{store_path}"
store_id = "mesh-multi-node"
"#,
        mesh_dir = MESH_DIR,
        initial_state = initial_state,
        store_path = store_path,
    );
    std::fs::write(path, template).expect("write manifest");
    let _ = std::fs::create_dir_all(store_path);
}

fn main() {
    // Derive both Node keypairs.
    let node_a = seeded_signing_key("mesh-multi-node-a-seed");
    let node_b = seeded_signing_key("mesh-multi-node-b-seed");
    let pk_a = node_a.verifying_key().to_bytes();
    let pk_b = node_b.verifying_key().to_bytes();
    println!("Node A pubkey: {}", hex(&pk_a));
    println!("Node B pubkey: {}", hex(&pk_b));

    // Build both init_state JSON blobs. Node A doesn't outbound-connect (waits);
    // Node B opens an outbound connection to A once spawned.
    let init_a = format!(
        r#"{{"node_seed":"mesh-multi-node-a-seed","listen_addr":"{addr}","peer_node_pubkeys":[{{"pubkey":"{pk_b}","name":"node-b"}}]}}"#,
        addr = ADDR_A,
        pk_b = hex(&pk_b),
    );
    let init_b = format!(
        r#"{{"node_seed":"mesh-multi-node-b-seed","root_pubkey":"{pk_a}","listen_addr":"{addr}","peer_node_pubkeys":[{{"pubkey":"{pk_b}","name":"node-b"}}],"peer_meshes":[{{"pubkey":"{pk_a}","address":"{addr_a}"}}]}}"#,
        addr = ADDR_B,
        pk_a = hex(&pk_a),
        pk_b = hex(&pk_b),
        addr_a = ADDR_A,
    );

    // Clean up + write manifests.
    let _ = std::fs::remove_dir_all("/tmp/mesh-node-a-store");
    let _ = std::fs::remove_dir_all("/tmp/mesh-node-b-store");
    write_manifest(
        "/tmp/mesh-node-a-manifest.toml",
        &init_a,
        "/tmp/mesh-node-a-store",
    );
    write_manifest(
        "/tmp/mesh-node-b-manifest.toml",
        &init_b,
        "/tmp/mesh-node-b-store",
    );

    // Spawn mesh A first; wait for it to listen.
    let mut child_a = spawn_mesh(
        "/tmp/mesh-node-a-manifest.toml",
        "/tmp/mesh-node-a.log",
    );
    if !wait_for_port(ADDR_A, Duration::from_secs(5)) {
        eprintln!("mesh A failed to listen");
        let _ = child_a.kill();
        std::process::exit(1);
    }
    println!("✓ Node A listening on {}", ADDR_A);

    // Spawn mesh B; it'll outbound-connect to A.
    let mut child_b = spawn_mesh(
        "/tmp/mesh-node-b-manifest.toml",
        "/tmp/mesh-node-b.log",
    );
    if !wait_for_port(ADDR_B, Duration::from_secs(5)) {
        eprintln!("mesh B failed to listen");
        let _ = child_a.kill();
        let _ = child_b.kill();
        std::process::exit(1);
    }
    println!("✓ Node B listening on {}", ADDR_B);

    // Give B a moment to complete its outbound handshake.
    std::thread::sleep(Duration::from_millis(500));

    let result = (|| -> Result<(), String> {
        // Connect test clients to both meshes.
        let mut client_a = handshake(ADDR_A, &node_a).map_err(|e| format!("handshake A: {}", e))?;
        println!("✓ test client authenticated to A as Node A");
        let mut client_b = handshake(ADDR_B, &node_b).map_err(|e| format!("handshake B: {}", e))?;
        println!("✓ test client authenticated to B as Node B");

        // Set a generous read timeout on B so we can wait for the cross-mesh
        // delivery to arrive.
        client_b.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        // Submit a Send(Node B, "hello...") via client_a.
        let payload = b"hello from A across the mesh";
        let event = make_send_event(&node_a, &GENESIS_PARENT, &pk_b, payload);
        client_a.write_all(&encode_frame(FRAME_SUBMIT, &event))
            .map_err(|e| format!("submit: {}", e))?;
        let (kind, ack) = read_frame(&mut client_a).map_err(|e| format!("read ack: {}", e))?;
        if kind != FRAME_ACK || ack[32] != 1 {
            return Err(format!(
                "submit not ACKed cleanly: kind={:#x} ok={}: {}",
                kind,
                ack[32],
                String::from_utf8_lossy(&ack[33..])
            ));
        }
        println!("✓ Send submitted to A and ACKed");

        // Drain client_a's DELIVERED echo of its own event (since broadcast
        // hits the submitter too).
        let _ = read_frame(&mut client_a);

        // Now: client_b should receive a DELIVERED frame containing the Send.
        loop {
            let (kind, payload) = read_frame(&mut client_b)
                .map_err(|e| format!("client_b read: {}", e))?;
            if kind != FRAME_DELIVERED {
                // Could be a STATE or other frame; keep reading.
                continue;
            }
            // Layout: parent[32]+author[32]+kind[1]+...
            if payload.len() < 65 || payload[64] != OP_SEND {
                continue; // some other event delivered (e.g. a Witness)
            }
            // Extract Send body.
            let body_len = u32::from_be_bytes([
                payload[97], payload[98], payload[99], payload[100],
            ]) as usize;
            let body = &payload[101..101 + body_len];
            if body != b"hello from A across the mesh" {
                return Err(format!(
                    "B received unexpected body: {:?}",
                    String::from_utf8_lossy(body)
                ));
            }
            println!("✓ B received cross-mesh Send: {:?}", String::from_utf8_lossy(body));
            break;
        }

        Ok(())
    })();

    let _ = child_a.kill();
    let _ = child_b.kill();
    let _ = child_a.wait();
    let _ = child_b.wait();

    match result {
        Ok(_) => {
            println!("\nMULTI-NODE TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nMULTI-NODE TEST FAILED: {}", e);
            std::process::exit(1);
        }
    }
}
