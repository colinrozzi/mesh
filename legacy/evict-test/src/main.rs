//! mesh QUORUM-EVICTION integration test.
//!
//! Three members A, B, C (fully connected). C is killed WITHOUT departing. A and B
//! each notice C has gone silent past the evict timeout, vote to evict it (majority
//! of 3 = 2), and C leaves — after which A and B finalize a message without it. If
//! eviction failed, that message could never finalize: it would still require C's
//! (dead) witness.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, write_manifest, Client};
use std::process::Child;
use std::time::Duration;

const ADDR_A: &str = "127.0.0.1:9467";
const ADDR_B: &str = "127.0.0.1:9468";
const ADDR_C: &str = "127.0.0.1:9469";

fn main() {
    let a = seeded_key("mesh-evict-a");
    let b = seeded_key("mesh-evict-b");
    let c = seeded_key("mesh-evict-c");
    let (pa, pb) = (pubkey(&a), pubkey(&b));

    let boot = format!(r#"["{}","{}","{}"]"#, hex(&pa), hex(&pb), hex(&pubkey(&c)));
    let et = 5000u64; // short evict timeout so the test doesn't wait the 20s default
    let init_a = format!(
        r#"{{"node_seed":"mesh-evict-a","listen_addr":"{ADDR_A}","members":{boot},"evict_timeout_ms":{et}}}"#
    );
    let init_b = format!(
        r#"{{"node_seed":"mesh-evict-b","listen_addr":"{ADDR_B}","members":{boot},"evict_timeout_ms":{et},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#,
        hex(&pa)
    );
    // C dials both A and B so the mesh is fully connected (each sees all heartbeats).
    let init_c = format!(
        r#"{{"node_seed":"mesh-evict-c","listen_addr":"{ADDR_C}","members":{boot},"evict_timeout_ms":{et},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}},{{"pubkey":"{}","address":"{ADDR_B}"}}]}}"#,
        hex(&pa),
        hex(&pb)
    );

    for s in ["a", "b", "c"] {
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-evict-{s}-store"));
    }
    write_manifest("/tmp/mesh-evict-a.toml", &init_a, "/tmp/mesh-evict-a-store", "mesh-evict-a");
    write_manifest("/tmp/mesh-evict-b.toml", &init_b, "/tmp/mesh-evict-b-store", "mesh-evict-b");
    write_manifest("/tmp/mesh-evict-c.toml", &init_c, "/tmp/mesh-evict-c-store", "mesh-evict-c");

    let mut children: Vec<Child> = Vec::new();
    children.push(spawn_mesh("/tmp/mesh-evict-a.toml", "/tmp/mesh-evict-a.log"));
    assert!(wait_for_port(ADDR_A, Duration::from_secs(5)), "A failed to listen");
    children.push(spawn_mesh("/tmp/mesh-evict-b.toml", "/tmp/mesh-evict-b.log"));
    assert!(wait_for_port(ADDR_B, Duration::from_secs(5)), "B failed to listen");
    let mut c_child = spawn_mesh("/tmp/mesh-evict-c.toml", "/tmp/mesh-evict-c.log");
    assert!(wait_for_port(ADDR_C, Duration::from_secs(5)), "C failed to listen");
    std::thread::sleep(Duration::from_millis(1500)); // full-mesh sync
    println!("✓ 3-member mesh A + B + C up and synced");

    let result = (|children: &mut Vec<Child>| -> Result<(), String> {
        // Kill C without departing — a crashed member.
        let _ = c_child.kill();
        let _ = c_child.wait();
        println!("✓ C killed (did NOT depart)");

        // Wait past the evict timeout + a finality margin for A + B to vote C out.
        std::thread::sleep(Duration::from_secs(13));

        // A submits. If C were still a required member this could never finalize
        // (C is dead); B receiving it proves C was quorum-evicted.
        let mut client_a = Client::connect(ADDR_A, &a).map_err(|e| format!("connect A: {e}"))?;
        let mut client_b = Client::connect(ADDR_B, &b).map_err(|e| format!("connect B: {e}"))?;
        let body = b"survivors carry on";
        client_a.submit(body).map_err(|e| format!("submit: {e}"))?;
        let (_from, got) = client_b.recv_message().map_err(|e| format!("B recv: {e}"))?;
        if got != body {
            return Err(format!("B got wrong body: {:?}", String::from_utf8_lossy(&got)));
        }
        println!("✓ A + B finalized a message without C — C was quorum-evicted");
        Ok(())
    })(&mut children);

    for mut ch in children {
        let _ = ch.kill();
        let _ = ch.wait();
    }

    match result {
        Ok(_) => {
            println!("\nEVICT TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nEVICT TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
