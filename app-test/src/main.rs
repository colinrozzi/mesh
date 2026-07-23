//! App-to-app integration test — the co-located message-server path end to end.
//!
//! Two example *app* actors, A and B, each supervise their own mesh node child.
//! A and B's nodes form a 2-node mesh over TCP. Each app `Submit`s a greeting to
//! its node and `Register`s for delivery; once both greetings finalize across
//! both nodes, each app should receive the *other's* greeting back via its
//! `handle-send` callback (logged as "RECEIVED ... hello from ...").
//!
//! This exercises: supervisor.spawn (app → node child), message-server request
//! (app → node commands), send-callback delivery (node → app), and get-self.
//!
//! GATED ON `theater:simple/runtime.get-self`: the example app calls it to learn
//! its own actor-id for Register. Until that primitive lands in theater and the
//! runtime is rebuilt, this test cannot pass (the apps can't self-register). It
//! is wired and compiles; run it once get-self is available.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, write_manifest, MESH_DIR};
use std::process::Child;
use std::time::{Duration, Instant};

const APP_WASM: &str =
    "/home/colin/work/actors/mesh/example-app/target/wasm32-unknown-unknown/release/mesh_example_app.wasm";
const ADDR_A: &str = "127.0.0.1:9550";
const ADDR_B: &str = "127.0.0.1:9551";

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
    // Node identities are derived from the seeds the apps hand their nodes.
    let (pa, pb) = (pubkey(&seeded_key("app-a-node")), pubkey(&seeded_key("app-b-node")));
    let members = format!(r#"["{}","{}"]"#, hex(&pa), hex(&pb));

    // Node configs (what each app hands its node child). Node B dials node A.
    let node_a = format!(
        r#"{{"node_seed":"app-a-node","listen_addr":"{ADDR_A}","members":{members},"dial":[]}}"#
    );
    let node_b = format!(
        r#"{{"node_seed":"app-b-node","listen_addr":"{ADDR_B}","members":{members},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#,
        hex(&pa)
    );

    for s in ["a", "b"] {
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-app-node-{s}-store"));
    }
    write_manifest("/tmp/mesh-app-node-a.toml", &node_a, "/tmp/mesh-app-node-a-store", "app-node-a");
    write_manifest("/tmp/mesh-app-node-b.toml", &node_b, "/tmp/mesh-app-node-b-store", "app-node-b");

    // App configs. Each app rebuilds the same node config from these fields and
    // spawns its node from the manifest above.
    let app_a = format!(
        r#"{{"label":"A","node_manifest":"/tmp/mesh-app-node-a.toml","node_seed":"app-a-node","node_listen":"{ADDR_A}","members":["{}","{}"],"dial":[],"greeting":"hello from A"}}"#,
        hex(&pa), hex(&pb)
    );
    let app_b = format!(
        r#"{{"label":"B","node_manifest":"/tmp/mesh-app-node-b.toml","node_seed":"app-b-node","node_listen":"{ADDR_B}","members":["{}","{}"],"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}],"greeting":"hello from B"}}"#,
        hex(&pa), hex(&pb), hex(&pa)
    );
    write_app_manifest("/tmp/mesh-app-a.toml", "app-a", &app_a);
    write_app_manifest("/tmp/mesh-app-b.toml", "app-b", &app_b);

    let _ = MESH_DIR; // (node manifests already point at the built mesh.wasm)

    // Bring up A's node first and wait for it to listen before starting B —
    // B's node dials A on startup, and the mesh does a one-shot dial with no
    // retry, so if A isn't listening yet the peers never connect. (Reconnect /
    // dial-retry in the substrate would remove this ordering requirement.)
    let mut children: Vec<Child> = Vec::new();
    children.push(spawn_mesh("/tmp/mesh-app-a.toml", "/tmp/mesh-app-a.log"));
    let a_up = wait_for_port(ADDR_A, Duration::from_secs(10));
    children.push(spawn_mesh("/tmp/mesh-app-b.toml", "/tmp/mesh-app-b.log"));
    let up = a_up && wait_for_port(ADDR_B, Duration::from_secs(10));
    if up {
        println!("✓ both apps up; nodes listening on {ADDR_A} / {ADDR_B}");
    } else {
        println!("✗ nodes failed to listen (apps may have failed to spawn their node children)");
    }

    // Each app must receive the other's greeting. Poll both concurrently under
    // one shared deadline (not two sequential ones) and stop as soon as both land.
    let deadline = Duration::from_secs(40);
    let start = Instant::now();
    let (mut a_got_b, mut b_got_a) = (false, false);
    while start.elapsed() < deadline && !(a_got_b && b_got_a) {
        a_got_b = a_got_b || log_contains("/tmp/mesh-app-a.log", "hello from B");
        b_got_a = b_got_a || log_contains("/tmp/mesh-app-b.log", "hello from A");
        std::thread::sleep(Duration::from_millis(200));
    }

    for mut c in children {
        let _ = c.kill();
        let _ = c.wait();
    }

    if a_got_b && b_got_a {
        println!("\nAPP TEST PASSED — apps exchanged messages over the substrate");
        std::process::exit(0);
    }
    eprintln!(
        "\nAPP TEST FAILED — a_got_b={a_got_b} b_got_a={b_got_a}\n(if get-self is not yet in theater, apps cannot self-register — this is expected)"
    );
    std::process::exit(1);
}

/// Whether `path` currently contains `needle`.
fn log_contains(path: &str, needle: &str) -> bool {
    std::fs::read_to_string(path).map(|s| s.contains(needle)).unwrap_or(false)
}
