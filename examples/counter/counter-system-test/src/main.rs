//! Tier-1 reference system, end to end: the counter-system executor supervises a
//! counter node and drives it over the two intended surfaces — RPC for actions
//! (author + read verbs) and the message-server stream for live events. A complete
//! running system, the exemplar the fleet copies.
//!
//! We spawn ONE theater actor — the executor — which itself supervisor-spawns its
//! node child. The executor authors a burst of increments (and one invalid one that
//! must be rejected), reads the folded count back over RPC, and logs a verdict; the
//! node pushes each finalized dag-node to the executor's stream handler. This runner
//! greps the executor log for the verdict + the stream events.

use std::fs;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

const THEATER_BIN: &str = "/home/colin/work/theater/target/release/theater";
const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const NODE_WASM: &str = "target/wasm32-unknown-unknown/release/mesh_counter.wasm";
const SYS_WASM: &str = "examples/counter/counter-system/target/wasm32-unknown-unknown/release/counter_system.wasm";
const NODE_LISTEN: &str = "127.0.0.1:9491";
const LOG: &str = "/tmp/mesh-counter-system.log";

fn write_node_manifest() {
    let store = "/tmp/mesh-counter-node-store";
    let _ = fs::remove_dir_all(store);
    let _ = fs::create_dir_all(store);
    let toml = format!(
        r#"name = "counter-node"
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

[[handler]]
type = "store"
base_path = "{store}"
store_id = "counter-node"
"#
    );
    fs::write("/tmp/mesh-counter-node.toml", toml).expect("write node manifest");
}

fn write_system_manifest() {
    // The executor drives the node — it needs supervisor (spawn), rpc (call),
    // message-server (register + handle-send), timer (tick).
    let cfg = format!(
        r#"{{"node_manifest":"/tmp/mesh-counter-node.toml","node_seed":"counter-node-seed","node_listen":"{NODE_LISTEN}"}}"#
    );
    let toml = format!(
        r#"name = "counter-system"
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
    fs::write("/tmp/mesh-counter-system.toml", toml).expect("write system manifest");
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
    write_node_manifest();
    write_system_manifest();

    let mut child = spawn("/tmp/mesh-counter-system.toml", LOG);
    // init → 2.5s tick → RPC round-trip → stream deliveries. Give it room.
    sleep(Duration::from_secs(9));
    let _ = child.kill();
    let _ = child.wait();

    let log = fs::read_to_string(LOG).unwrap_or_default();
    let ok = log.contains("COUNTER-SYSTEM OK");
    let rejected = log.contains("Inc(-5) rejected as expected");
    let stream = log.matches("STREAM finalized event").count();

    println!("--- counter-system log ---");
    for line in log.lines().filter(|l| l.contains("counter-system")) {
        println!("{line}");
    }
    println!("--------------------------");
    println!("verdict-line={ok} rejected={rejected} stream_events={stream}");

    if ok && rejected && stream >= 4 {
        println!("\nCOUNTER-SYSTEM TEST PASSED");
        std::process::exit(0);
    } else {
        eprintln!("\nCOUNTER-SYSTEM TEST FAILED (ok={ok} rejected={rejected} stream={stream}, want >=4)");
        std::process::exit(1);
    }
}
