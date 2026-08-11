//! N-node scale / convergence test — pushes the substrate past the two-node oracles
//! into the regime the fold's O(events^3) cost was flagged for.
//!
//! Topology is a LINE: node i dials node i-1, so an event authored at one end must
//! traverse N-1 hops (and rely on per-tick anti-entropy re-advertisement) to reach
//! the other. All N nodes are chat members from genesis. After genesis propagates
//! end-to-end, every node authors a burst of M text messages in a round-robin, which
//! (because propagation is async and multi-hop) builds a WIDE concurrent frontier of
//! ~N*M events with heavy merges — exactly what makes `fold_state_at` expensive.
//!
//! Asserts: every node converges BYTE-FOR-BYTE on the same chat state and the log
//! holds all N*M messages. Prints wall-clock timing so the fold wall (and, after
//! memoization, its removal) is measurable.
//!
//! Tune with env: SCALE_N (nodes, default 5), SCALE_M (msgs/node, default 8).

use mesh_testkit::{hex, pubkey, seeded_key, wait_for_port, Client, MESH_DIR, THEATER_BIN};
use serde::Deserialize;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_chat.wasm";
const BASE_PORT: u16 = 9481;

use chat_protocol::Msg;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn genesis(members: &[[u8; 32]]) -> Vec<u8> {
    chat_protocol::encode(&Msg::Genesis { members: members.iter().map(|m| m.to_vec()).collect() })
}

fn text(body: &str) -> Vec<u8> {
    chat_protocol::encode(&Msg::Text { body: body.to_string() })
}

#[derive(Deserialize)]
struct ChatState {
    members: Vec<(Vec<u8>, Vec<u8>)>,
    log: Vec<ChatMsg>,
}
#[derive(Deserialize)]
struct ChatMsg {
    #[allow(dead_code)]
    id: Vec<u8>,
    #[allow(dead_code)]
    author: Vec<u8>,
    #[allow(dead_code)]
    ts: u64,
    #[allow(dead_code)]
    body: String,
}

fn spawn_node(manifest: &str, log: &str) -> Child {
    let f = fs::File::create(log).unwrap();
    let e = f.try_clone().unwrap();
    Command::new(THEATER_BIN)
        .args(["spawn", manifest])
        .stdout(Stdio::from(f))
        .stderr(Stdio::from(e))
        .spawn()
        .expect("spawn theater")
}

/// Write a mesh_chat manifest for node `i`: its own seed/addr plus a dial to node
/// `i-1` (none for node 0), forming the line.
fn write_manifest(i: usize, addr: &str, prev: Option<(&str, &str)>, store: &str) -> String {
    let path = format!("/tmp/mesh-scale-{i}.toml");
    let dial = match prev {
        Some((pk, paddr)) => format!(r#","dial":[{{"pubkey":"{pk}","address":"{paddr}"}}]"#),
        None => String::new(),
    };
    let init = format!(r#"{{"node_seed":"scale-node-{i}-seed","listen_addr":"{addr}"{dial}}}"#);
    let toml = format!(
        r#"name = "mesh-scale-{i}"
version = "0.1.0"
package = "{MESH_DIR}/{WASM}"
static_package = true
initial_state = '{init}'

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
base_path = "{store}"
store_id = "mesh-scale-{i}"
"#
    );
    fs::write(&path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
    path
}

fn main() {
    let n = env_usize("SCALE_N", 5);
    let m = env_usize("SCALE_M", 8);
    println!("=== mesh N-node scale test: N={n} nodes (line), M={m} msgs/node, {} total ===", n * m);

    let seeds: Vec<_> = (0..n).map(|i| seeded_key(&format!("scale-node-{i}-seed"))).collect();
    let pks: Vec<[u8; 32]> = seeds.iter().map(pubkey).collect();
    let addrs: Vec<String> = (0..n).map(|i| format!("127.0.0.1:{}", BASE_PORT + i as u16)).collect();

    // Spawn the line: node i dials node i-1.
    let mut children: Vec<Child> = Vec::new();
    for i in 0..n {
        let store = format!("/tmp/mesh-scale-store-{i}");
        let _ = fs::remove_dir_all(&store);
        let prev = if i == 0 { None } else { Some((hex(&pks[i - 1]), addrs[i - 1].clone())) };
        let prev_ref = prev.as_ref().map(|(pk, a)| (pk.as_str(), a.as_str()));
        let manifest = write_manifest(i, &addrs[i], prev_ref, &store);
        let child = spawn_node(&manifest, &format!("/tmp/mesh-scale-{i}.log"));
        children.push(child);
        if !wait_for_port(&addrs[i], Duration::from_secs(5)) {
            eprintln!("node {i} failed to listen on {}", addrs[i]);
            kill_all(&mut children);
            std::process::exit(1);
        }
    }
    println!("✓ all {n} nodes listening (line topology, node i → dials node i-1)");
    // Let the dials connect end-to-end.
    std::thread::sleep(Duration::from_millis(1200));

    let result = run(n, m, &seeds, &pks, &addrs);

    kill_all(&mut children);

    match result {
        Ok(()) => {
            println!("\nSCALE TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nSCALE TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}

fn kill_all(children: &mut [Child]) {
    for c in children.iter_mut() {
        let _ = c.kill();
    }
    for c in children.iter_mut() {
        let _ = c.wait();
    }
}

fn run(
    n: usize,
    m: usize,
    seeds: &[mesh_testkit::SigningKey],
    pks: &[[u8; 32]],
    addrs: &[String],
) -> Result<(), String> {
    // Node 0 authors genesis listing all N members.
    let mut c0 = Client::connect(&addrs[0], &seeds[0]).map_err(|e| format!("connect node0: {e}"))?;
    let g = c0.submit(&genesis(pks)).map_err(|e| format!("genesis: {e}"))?;
    println!("✓ genesis authored at node 0 ({}), {} members", hex(&g), n);

    // Wait for genesis to traverse all N-1 hops to the far end.
    let t_prop = Instant::now();
    let mut c_last =
        Client::connect(&addrs[n - 1], &seeds[n - 1]).map_err(|e| format!("connect last: {e}"))?;
    let mut propagated = false;
    for _ in 0..80 {
        let st: ChatState = parse(&mut c_last)?;
        if st.members.len() == n {
            propagated = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    if !propagated {
        return Err(format!("genesis never reached node {} (line end)", n - 1));
    }
    println!("✓ genesis propagated end-to-end ({} hops) in {:?}", n - 1, t_prop.elapsed());

    // Burst: round-robin, every node authors M texts. Interleaving across nodes with
    // async multi-hop propagation makes the events causally concurrent → wide frontier.
    let mut clients: Vec<Client> = Vec::with_capacity(n);
    for i in 0..n {
        clients.push(Client::connect(&addrs[i], &seeds[i]).map_err(|e| format!("connect {i}: {e}"))?);
    }
    let total = n * m;
    let t_burst = Instant::now();
    for j in 0..m {
        for (i, c) in clients.iter_mut().enumerate() {
            let body = format!("node{i}-msg{j}");
            c.submit(&text(&body)).map_err(|e| format!("submit node{i} msg{j}: {e}"))?;
        }
    }
    println!("✓ authored {total} messages ({m}/node) in {:?}", t_burst.elapsed());

    // Convergence: poll all N nodes until every state is byte-identical AND the log
    // holds all N*M messages.
    let t_conv = Instant::now();
    let mut converged = false;
    let mut last_report = String::new();
    for _ in 0..200 {
        let mut states: Vec<Vec<u8>> = Vec::with_capacity(n);
        for c in clients.iter_mut() {
            states.push(c.current_state().map_err(|e| format!("current-state: {e}"))?);
        }
        let all_equal = states.iter().all(|s| s == &states[0]);
        let st0: ChatState = serde_json::from_slice(&states[0]).map_err(|e| format!("parse: {e}"))?;
        last_report = format!("log={}/{total} members={} identical={all_equal}", st0.log.len(), st0.members.len());
        if all_equal && st0.log.len() == total && st0.members.len() == n {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    if !converged {
        return Err(format!("did not converge in {:?} (last: {last_report})", t_conv.elapsed()));
    }
    let conv = t_conv.elapsed();
    println!("✓ all {n} nodes converged byte-for-byte in {conv:?}");
    println!(
        "  → {total} events, {:.1} events/sec end-to-end convergence",
        total as f64 / conv.as_secs_f64().max(0.001)
    );
    Ok(())
}

fn parse(c: &mut Client) -> Result<ChatState, String> {
    let bytes = c.current_state().map_err(|e| format!("current-state: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("parse state: {e}"))
}
