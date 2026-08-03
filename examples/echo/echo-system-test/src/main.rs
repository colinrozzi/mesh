//! Tier-2 flagship, end to end: the round-trip oracle re-dressed with REAL executors.
//!
//! Two theater processes, each a role executor supervising its echo node:
//!   - server executor + server node (listens)
//!   - client executor + client node (dials the server → the nodes gossip over TCP)
//! The client authors a Request over RPC; it gossips to the server node; the server
//! executor reacts off its finalized stream and authors a Response; that gossips back
//! and the client matches it off its own stream. Every hop is a real surface — RPC
//! for actions, message-server for events, TCP only node↔node — no std test Client.
//!
//! Asserts the client logs the matching response, and the server logs the reply.

use mesh_testkit::{hex, pubkey, seeded_key};
use std::fs;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const NODE_WASM: &str = "target/wasm32-unknown-unknown/release/mesh_echo.wasm";
const SYS_WASM: &str = "examples/echo/echo-system/target/wasm32-unknown-unknown/release/echo_system.wasm";

const SERVER_SEED: &str = "echo-server-node";
const CLIENT_SEED: &str = "echo-client-node";
const SERVER_ADDR: &str = "127.0.0.1:9495";
const CLIENT_ADDR: &str = "127.0.0.1:9496";
const REQUEST_BODY: &str = "hello echo";

fn write_node_manifest() {
    // The node imports no store functions, so no store handler is needed — which
    // also means both node children can share one manifest without a store collision.
    let toml = format!(
        r#"name = "echo-node"
version = "0.1.0"
package = "{MESH_DIR}/{NODE_WASM}"
static_package = true

[[handler]]
type = "runtime"

[[handler]]
type = "tcp"

[[handler]]
type = "timer"

[[handler]]
type = "message-server"
"#
    );
    fs::write("/tmp/mesh-echo-node.toml", toml).expect("write node manifest");
}

fn write_system_manifest(path: &str, cfg: &str) {
    let toml = format!(
        r#"name = "echo-system"
version = "0.1.0"
package = "{MESH_DIR}/{SYS_WASM}"
static_package = true
initial_state = '{cfg}'

[[handler]]
type = "runtime"

[[handler]]
type = "supervisor"

[[handler]]
type = "rpc"

[[handler]]
type = "message-server"

[[handler]]
type = "timer"
"#
    );
    fs::write(path, toml).expect("write system manifest");
}

fn spawn(manifest: &str, log: &str) -> Child {
    let f = fs::File::create(log).unwrap();
    let e = f.try_clone().unwrap();
    Command::new(THEATER_BIN)
        .args(["spawn", manifest])
        .stdout(Stdio::from(f))
        .stderr(Stdio::from(e))
        .spawn()
        .expect("spawn theater")
}

fn main() {
    let server_pk = hex(&pubkey(&seeded_key(SERVER_SEED)));
    write_node_manifest();

    let server_cfg = format!(
        r#"{{"role":"server","node_manifest":"/tmp/mesh-echo-node.toml","node_seed":"{SERVER_SEED}","node_listen":"{SERVER_ADDR}"}}"#
    );
    let client_cfg = format!(
        r#"{{"role":"client","node_manifest":"/tmp/mesh-echo-node.toml","node_seed":"{CLIENT_SEED}","node_listen":"{CLIENT_ADDR}","dial_pubkey":"{server_pk}","dial_addr":"{SERVER_ADDR}","request_body":"{REQUEST_BODY}"}}"#
    );
    write_system_manifest("/tmp/mesh-echo-server.toml", &server_cfg);
    write_system_manifest("/tmp/mesh-echo-client.toml", &client_cfg);

    // Server first, so its node is listening before the client dials.
    let mut server = spawn("/tmp/mesh-echo-server.toml", "/tmp/mesh-echo-server.log");
    sleep(Duration::from_secs(3));
    let mut client = spawn("/tmp/mesh-echo-client.toml", "/tmp/mesh-echo-client.log");

    // init → 3s tick → request → gossip → react → gossip back → match.
    sleep(Duration::from_secs(12));
    let _ = client.kill();
    let _ = server.kill();
    let _ = client.wait();
    let _ = server.wait();

    let server_log = fs::read_to_string("/tmp/mesh-echo-server.log").unwrap_or_default();
    let client_log = fs::read_to_string("/tmp/mesh-echo-client.log").unwrap_or_default();

    println!("--- server (echo) ---");
    for l in server_log.lines().filter(|l| l.contains("[echo/")) {
        println!("{l}");
    }
    println!("--- client (echo) ---");
    for l in client_log.lines().filter(|l| l.contains("[echo/")) {
        println!("{l}");
    }
    println!("---------------------");

    let server_replied = server_log.contains("authored response");
    let got = client_log.contains("ECHO CLIENT GOT RESPONSE");
    let right_body = client_log.contains(REQUEST_BODY);

    println!("server_replied={server_replied} client_got_response={got} right_body={right_body}");
    if server_replied && got && right_body {
        println!("\nECHO SYSTEM TEST PASSED");
        std::process::exit(0);
    } else {
        eprintln!("\nECHO SYSTEM TEST FAILED");
        std::process::exit(1);
    }
}
