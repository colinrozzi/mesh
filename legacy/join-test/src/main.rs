//! mesh SELF-SERVE JOIN integration test.
//!
//! Unlike `membership-test` (where A explicitly `introduce`s N before N connects),
//! here N joins *itself*: A's config lists N's pubkey in `join_allow`, so when N
//! dials A, A accepts the handshake and **auto-introduces** N. N syncs its own
//! admission and becomes a full member — no out-of-band introduce. We prove it by
//! finalizing a message across {A, N} and delivering it to N.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, write_manifest, Client};
use std::process::Child;
use std::time::Duration;

const ADDR_A: &str = "127.0.0.1:9457";
const ADDR_N: &str = "127.0.0.1:9458";

fn main() {
    let a = seeded_key("mesh-join-a");
    let n = seeded_key("mesh-join-n");
    let (pa, pn) = (pubkey(&a), pubkey(&n));

    // A is the sole bootstrap member. N is NOT a member — but A's `join_allow`
    // permits N to self-join, so no explicit introduce is needed.
    let boot = format!(r#"["{}"]"#, hex(&pa));
    let allow = format!(r#"["{}"]"#, hex(&pn));
    let init_a = format!(
        r#"{{"node_seed":"mesh-join-a","listen_addr":"{ADDR_A}","members":{boot},"join_allow":{allow}}}"#
    );
    let init_n = format!(
        r#"{{"node_seed":"mesh-join-n","listen_addr":"{ADDR_N}","members":{boot},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#,
        hex(&pa)
    );

    for s in ["a", "n"] {
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-join-{s}-store"));
    }
    write_manifest("/tmp/mesh-join-a.toml", &init_a, "/tmp/mesh-join-a-store", "mesh-join-a");
    write_manifest("/tmp/mesh-join-n.toml", &init_n, "/tmp/mesh-join-n-store", "mesh-join-n");

    let mut children: Vec<Child> = Vec::new();
    children.push(spawn_mesh("/tmp/mesh-join-a.toml", "/tmp/mesh-join-a.log"));
    assert!(wait_for_port(ADDR_A, Duration::from_secs(5)), "A failed to listen");
    println!("✓ bootstrap node A up (join_allow = [N])");

    let result = (|children: &mut Vec<Child>| -> Result<(), String> {
        // N self-joins: NO explicit introduce. It dials A; A accepts it
        // (join_allow) and auto-introduces it; N syncs its own admission.
        children.push(spawn_mesh("/tmp/mesh-join-n.toml", "/tmp/mesh-join-n.log"));
        assert!(wait_for_port(ADDR_N, Duration::from_secs(5)), "N failed to listen");
        std::thread::sleep(Duration::from_millis(1500));
        println!("✓ N spawned and self-joined (no explicit introduce)");

        // Prove N is a finalized member: A submits, N receives.
        let mut client_a = Client::connect(ADDR_A, &a).map_err(|e| format!("connect A: {e}"))?;
        let mut client_n = Client::connect(ADDR_N, &n).map_err(|e| format!("connect N: {e}"))?;
        let body = b"self-serve welcome";
        client_a.submit(body).map_err(|e| format!("submit: {e}"))?;
        let (_from, got) = client_n.recv_message().map_err(|e| format!("N recv: {e}"))?;
        if got != body {
            return Err(format!("N got wrong body: {:?}", String::from_utf8_lossy(&got)));
        }
        println!("✓ message finalized across A + N — self-serve joiner is a full member");
        Ok(())
    })(&mut children);

    for mut c in children {
        let _ = c.kill();
        let _ = c.wait();
    }

    match result {
        Ok(_) => {
            println!("\nJOIN TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nJOIN TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
