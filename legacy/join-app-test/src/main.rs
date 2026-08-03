//! Reproduction: a JOINING node with a co-located APP awaiting is-ready — the
//! dev-round-trip topology (sentinelctl) — with BOTH sides composed as apps.
//!
//! `join-test` covers the raw-node self-serve join (no app). `app-test` covers
//! the app-drives-node path but only for GENESIS members. This closes the gap:
//! a node that BOTH self-joins AND has a co-located app awaiting is-ready, with
//! the ADMITTING side also an app (as in the dev pair: responder + sentinelctl).
//!
//!   - App A: admitting side — genesis member (members=[A]), join_allow=[B].
//!   - App B: joiner — members=[A], dial=[A]; waits for the node's is-ready.
//!
//! PASS = app B logs "READY" (its joining node admitted + signalled ready).

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, write_manifest};
use std::process::Child;
use std::time::{Duration, Instant};

const APP_WASM: &str =
    "/home/colin/work/actors/mesh/example-app/target/wasm32-unknown-unknown/release/mesh_example_app.wasm";
const ADDR_A: &str = "127.0.0.1:9560"; // admitting node
const ADDR_B: &str = "127.0.0.1:9561"; // joiner node

fn write_app_manifest(path: &str, name: &str, config_json: &str) {
    let manifest = format!(
        r#"name = "{name}"
version = "0.1.0"
package = "{APP_WASM}"
static_package = true
initial_state = '{config_json}'

[[handler]]
type = "runtime"

[[handler]]
type = "supervisor"

[[handler]]
type = "message-server"
"#
    );
    std::fs::write(path, manifest).expect("write app manifest");
}

fn main() {
    let pa = pubkey(&seeded_key("repro-node-a")); // admitting node
    let pb = pubkey(&seeded_key("repro-node-b")); // joiner node

    // Node manifests the apps spawn (init-state is overridden by each app).
    let node_a = format!(
        r#"{{"node_seed":"repro-node-a","listen_addr":"{ADDR_A}","members":["{}"],"join_allow":["{}"]}}"#,
        hex(&pa),
        hex(&pb)
    );
    let node_b = format!(
        r#"{{"node_seed":"repro-node-b","listen_addr":"{ADDR_B}","members":["{}"],"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#,
        hex(&pa),
        hex(&pa)
    );

    // App A: admitting side. Genesis member (members=[A]); join_allow permits B.
    let app_a = format!(
        r#"{{"label":"A-admit","node_manifest":"/tmp/mesh-joinapp-node-a.toml","node_seed":"repro-node-a","node_listen":"{ADDR_A}","members":["{}"],"join_allow":["{}"],"dial":[],"greeting":"hello from admitter"}}"#,
        hex(&pa),
        hex(&pb)
    );
    // App B: joiner. members=[A] (genesis set, self absent), dials A.
    let app_b = format!(
        r#"{{"label":"B-joiner","node_manifest":"/tmp/mesh-joinapp-node-b.toml","node_seed":"repro-node-b","node_listen":"{ADDR_B}","members":["{}"],"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}],"greeting":"hello from joiner"}}"#,
        hex(&pa),
        hex(&pa)
    );

    for s in ["a", "b"] {
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-joinapp-{s}-store"));
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-joinapp-node-{s}-store"));
    }
    write_manifest("/tmp/mesh-joinapp-node-a.toml", &node_a, "/tmp/mesh-joinapp-node-a-store", "joinapp-node-a");
    write_manifest("/tmp/mesh-joinapp-node-b.toml", &node_b, "/tmp/mesh-joinapp-node-b-store", "joinapp-node-b");
    write_app_manifest("/tmp/mesh-joinapp-a.toml", "joinapp-a", &app_a);
    write_app_manifest("/tmp/mesh-joinapp-b.toml", "joinapp-b", &app_b);

    let mut children: Vec<Child> = Vec::new();
    // Admitting side up first (one-shot dial, no retry — B must find A listening).
    children.push(spawn_mesh("/tmp/mesh-joinapp-a.toml", "/tmp/mesh-joinapp-a.log"));
    let a_up = wait_for_port(ADDR_A, Duration::from_secs(15));
    children.push(spawn_mesh("/tmp/mesh-joinapp-b.toml", "/tmp/mesh-joinapp-b.log"));
    let b_up = wait_for_port(ADDR_B, Duration::from_secs(15));
    if a_up && b_up {
        println!("✓ admitting app A up ({ADDR_A}); joiner app B up ({ADDR_B}) — both composed");
    } else {
        println!("✗ setup failed: a_up={a_up} b_up={b_up}");
    }

    let deadline = Duration::from_secs(30);
    let start = Instant::now();
    let mut b_ready = false;
    while start.elapsed() < deadline && !b_ready {
        b_ready = log_contains("/tmp/mesh-joinapp-b.log", "READY");
        std::thread::sleep(Duration::from_millis(200));
    }

    for mut c in children {
        let _ = c.kill();
        let _ = c.wait();
    }

    if b_ready {
        println!("\nJOIN-APP TEST PASSED — the joining node's app got the READY signal (both composed)");
        std::process::exit(0);
    }
    eprintln!("\nJOIN-APP TEST FAILED — joiner app never received READY within {}s", deadline.as_secs());
    std::process::exit(1);
}

fn log_contains(path: &str, needle: &str) -> bool {
    std::fs::read_to_string(path).map(|s| s.contains(needle)).unwrap_or(false)
}
