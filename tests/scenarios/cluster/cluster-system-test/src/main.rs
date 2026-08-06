//! Tier-3 reference, end to end: a cluster orchestrator + observer.
//!
//! One theater process. The harness computes an N-node LINE topology (node i dials
//! i-1) and hands the cluster actor one mesh InitConfig per node; the cluster
//! supervisor-spawns all N as children (they gossip over TCP loopback), drives a
//! burst of increments across every node over RPC, subscribes to every node's
//! finalized stream, and polls current-state across the cluster until it converges on
//! the sum. This runner greps the cluster log for the network-converged verdict.
//!
//! Tune with env: CLUSTER_N (nodes, default 4), CLUSTER_M (incrs/node, default 5).

use mesh_testkit::{hex, pubkey, seeded_key};
use serde::Serialize;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const NODE_WASM: &str = "target/wasm32-unknown-unknown/release/mesh_counter.wasm";
const SYS_WASM: &str = "tests/scenarios/cluster/cluster-system/target/wasm32-unknown-unknown/release/cluster_system.wasm";
const BASE_PORT: u16 = 9500;
const LOG: &str = "/tmp/mesh-cluster.log";

#[derive(Serialize)]
struct DialEntry {
    pubkey: String,
    address: String,
}
#[derive(Serialize)]
struct NodeInit {
    node_seed: String,
    listen_addr: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    dial: Vec<DialEntry>,
}
#[derive(Serialize)]
struct ClusterConfig {
    node_manifest: String,
    node_inits: Vec<String>,
    incrs_per_node: u64,
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn write_node_manifest() {
    // No store handler (the node imports no store fns) → all N children share one
    // manifest with no store collision.
    let toml = format!(
        r#"name = "cluster-node"
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
    fs::write("/tmp/mesh-cluster-node.toml", toml).expect("write node manifest");
}

fn write_cluster_manifest(cfg_json: &str) {
    let toml = format!(
        r#"name = "cluster-system"
version = "0.1.0"
package = "{MESH_DIR}/{SYS_WASM}"
static_package = true
initial_state = '{cfg_json}'

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
    fs::write("/tmp/mesh-cluster.toml", toml).expect("write cluster manifest");
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
    let n = env_usize("CLUSTER_N", 4);
    let m = env_usize("CLUSTER_M", 5) as u64;
    println!("=== cluster test: {n} nodes (line), {m} incrs/node, expected count {} ===", n as u64 * m);

    // Compute the line topology: node i dials node i-1.
    let seeds: Vec<String> = (0..n).map(|i| format!("cluster-node-{i}")).collect();
    let pks: Vec<String> = seeds.iter().map(|s| hex(&pubkey(&seeded_key(s)))).collect();
    let addrs: Vec<String> = (0..n).map(|i| format!("127.0.0.1:{}", BASE_PORT + i as u16)).collect();

    let node_inits: Vec<String> = (0..n)
        .map(|i| {
            let dial = if i == 0 {
                Vec::new()
            } else {
                vec![DialEntry { pubkey: pks[i - 1].clone(), address: addrs[i - 1].clone() }]
            };
            let init = NodeInit { node_seed: seeds[i].clone(), listen_addr: addrs[i].clone(), dial };
            serde_json::to_string(&init).unwrap()
        })
        .collect();

    let cfg = ClusterConfig {
        node_manifest: "/tmp/mesh-cluster-node.toml".to_string(),
        node_inits,
        incrs_per_node: m,
    };
    write_node_manifest();
    write_cluster_manifest(&serde_json::to_string(&cfg).unwrap());

    let mut cluster = spawn("/tmp/mesh-cluster.toml", LOG);
    // init spawns N nodes → 2.5s tick (subscribe + drive) → gossip → convergence polls.
    sleep(Duration::from_secs(18));
    let _ = cluster.kill();
    let _ = cluster.wait();

    let log = fs::read_to_string(LOG).unwrap_or_default();
    println!("--- cluster log ---");
    for l in log.lines().filter(|l| l.contains("[cluster]")) {
        println!("{l}");
    }
    println!("-------------------");

    let converged = log.contains("CLUSTER NETWORK CONVERGED");
    if converged {
        println!("\nCLUSTER SYSTEM TEST PASSED");
        std::process::exit(0);
    } else {
        eprintln!("\nCLUSTER SYSTEM TEST FAILED (never converged)");
        std::process::exit(1);
    }
}
