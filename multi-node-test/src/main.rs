//! mesh v3 multi-node integration test.
//!
//! Spawns two member nodes (A on :9447, B on :9448), each knowing the other is
//! a member; B dials A. Then:
//!   1. test client authenticates to A as node A, and to B as node B
//!   2. client (to A) SUBMITs a message
//!   3. A authors it and gossips to B; B grafts it (witness) → it finalizes
//!   4. B's committed delivery NOTIFYs the message to the B-side client
//!   5. test verifies the message arrived cross-mesh

use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const FRAME_HELLO: u8 = 0x01;
const FRAME_AUTH: u8 = 0x02;
const FRAME_SUBMIT: u8 = 0x11;
const FRAME_CHALLENGE: u8 = 0x80;
const FRAME_ACCEPTED: u8 = 0x81;
const FRAME_ACK: u8 = 0x91;
const FRAME_NOTIFY: u8 = 0x92;

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const ADDR_A: &str = "127.0.0.1:9447";
const ADDR_B: &str = "127.0.0.1:9448";

fn seeded_key(seed: &str) -> SigningKey {
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
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

fn read_until(s: &mut TcpStream, kind: u8) -> std::io::Result<Vec<u8>> {
    loop {
        let (k, payload) = read_frame(s)?;
        if k == kind {
            return Ok(payload);
        }
    }
}

fn handshake(addr: &str, signer: &SigningKey) -> std::io::Result<TcpStream> {
    let mut s = TcpStream::connect(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    let pk = signer.verifying_key().to_bytes();
    s.write_all(&encode_frame(FRAME_HELLO, &pk))?;
    let nonce = read_until(&mut s, FRAME_CHALLENGE)?;
    let sig = signer.sign(&nonce).to_bytes();
    s.write_all(&encode_frame(FRAME_AUTH, &sig))?;
    read_until(&mut s, FRAME_ACCEPTED)?;
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
    let node_a = seeded_key("mesh-multi-node-a-seed");
    let node_b = seeded_key("mesh-multi-node-b-seed");
    let pk_a = node_a.verifying_key().to_bytes();
    let pk_b = node_b.verifying_key().to_bytes();
    println!("Node A pubkey: {}", hex(&pk_a));
    println!("Node B pubkey: {}", hex(&pk_b));

    // A knows B is a member but doesn't dial; B knows A and dials it.
    let init_a = format!(
        r#"{{"node_seed":"mesh-multi-node-a-seed","listen_addr":"{addr}","members":["{pk_b}"]}}"#,
        addr = ADDR_A,
        pk_b = hex(&pk_b),
    );
    let init_b = format!(
        r#"{{"node_seed":"mesh-multi-node-b-seed","listen_addr":"{addr}","members":["{pk_a}"],"dial":[{{"pubkey":"{pk_a}","address":"{addr_a}"}}]}}"#,
        addr = ADDR_B,
        pk_a = hex(&pk_a),
        addr_a = ADDR_A,
    );

    let _ = std::fs::remove_dir_all("/tmp/mesh-node-a-store");
    let _ = std::fs::remove_dir_all("/tmp/mesh-node-b-store");
    write_manifest("/tmp/mesh-node-a-manifest.toml", &init_a, "/tmp/mesh-node-a-store");
    write_manifest("/tmp/mesh-node-b-manifest.toml", &init_b, "/tmp/mesh-node-b-store");

    let mut child_a = spawn_mesh("/tmp/mesh-node-a-manifest.toml", "/tmp/mesh-node-a.log");
    if !wait_for_port(ADDR_A, Duration::from_secs(5)) {
        eprintln!("mesh A failed to listen");
        let _ = child_a.kill();
        std::process::exit(1);
    }
    println!("✓ Node A listening on {}", ADDR_A);

    let mut child_b = spawn_mesh("/tmp/mesh-node-b-manifest.toml", "/tmp/mesh-node-b.log");
    if !wait_for_port(ADDR_B, Duration::from_secs(5)) {
        eprintln!("mesh B failed to listen");
        let _ = child_a.kill();
        let _ = child_b.kill();
        std::process::exit(1);
    }
    println!("✓ Node B listening on {}", ADDR_B);

    // Give B time to dial A and exchange genesis.
    std::thread::sleep(Duration::from_millis(800));

    let result = (|| -> Result<(), String> {
        let mut client_a = handshake(ADDR_A, &node_a).map_err(|e| format!("handshake A: {}", e))?;
        println!("✓ test client authenticated to A as Node A");
        let mut client_b = handshake(ADDR_B, &node_b).map_err(|e| format!("handshake B: {}", e))?;
        println!("✓ test client authenticated to B as Node B");
        client_b.set_read_timeout(Some(Duration::from_secs(8))).unwrap();

        // SUBMIT a message (recipient is an app identity; here we just use pk_b).
        let body = b"hello from A across the mesh";
        let mut payload = pk_b.to_vec();
        payload.extend_from_slice(body);
        client_a.write_all(&encode_frame(FRAME_SUBMIT, &payload))
            .map_err(|e| format!("submit: {}", e))?;
        let ack = read_until(&mut client_a, FRAME_ACK).map_err(|e| format!("read ack: {}", e))?;
        if ack.len() != 33 || ack[32] != 1 {
            return Err(format!("submit not ACKed cleanly: {}", String::from_utf8_lossy(&ack)));
        }
        println!("✓ Send submitted to A and ACKed");

        // B should deliver the finalized message to its client.
        let notify = read_until(&mut client_b, FRAME_NOTIFY)
            .map_err(|e| format!("client_b notify: {}", e))?;
        let got = &notify[32..];
        if got != body {
            return Err(format!("B got unexpected body: {:?}", String::from_utf8_lossy(got)));
        }
        println!("✓ B received cross-mesh message: {:?}", String::from_utf8_lossy(got));
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
