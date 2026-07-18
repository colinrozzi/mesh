//! mesh v3 multi-node integration test.
//!
//! Spawns two member nodes (A on :9447, B on :9448), each knowing the other is
//! a member; B dials A. A message submitted to A propagates to B, finalizes
//! across both, and is delivered to a client on B.

use mesh_testkit::{hex, pubkey, seeded_key, Client};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const ADDR_A: &str = "127.0.0.1:9447";
const ADDR_B: &str = "127.0.0.1:9448";

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
package = "{mesh_dir}/target/wasm32-unknown-unknown/release/mesh.composite.wasm"
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
    let pk_a = pubkey(&node_a);
    let pk_b = pubkey(&node_b);
    println!("Node A pubkey: {}", hex(&pk_a));
    println!("Node B pubkey: {}", hex(&pk_b));

    // Both list the full bootstrap set; B dials A.
    let init_a = format!(
        r#"{{"node_seed":"mesh-multi-node-a-seed","listen_addr":"{ADDR_A}","members":["{a}","{b}"]}}"#,
        a = hex(&pk_a),
        b = hex(&pk_b),
    );
    let init_b = format!(
        r#"{{"node_seed":"mesh-multi-node-b-seed","listen_addr":"{ADDR_B}","members":["{a}","{b}"],"dial":[{{"pubkey":"{a}","address":"{ADDR_A}"}}]}}"#,
        a = hex(&pk_a),
        b = hex(&pk_b),
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
        let mut client_a = Client::connect(ADDR_A, &node_a).map_err(|e| format!("connect A: {}", e))?;
        println!("✓ test client authenticated to A as Node A");
        let mut client_b = Client::connect(ADDR_B, &node_b).map_err(|e| format!("connect B: {}", e))?;
        println!("✓ test client authenticated to B as Node B");

        let body = b"hello from A across the mesh";
        client_a.submit(body).map_err(|e| format!("submit: {}", e))?;
        println!("✓ message submitted to A and ACKed");

        let (_from, got) = client_b.recv_message().map_err(|e| format!("client_b: {}", e))?;
        if got != body {
            return Err(format!("B got unexpected body: {:?}", String::from_utf8_lossy(&got)));
        }
        println!("✓ B received cross-mesh message: {:?}", String::from_utf8_lossy(&got));
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
