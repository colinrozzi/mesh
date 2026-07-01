//! mesh dynamic-membership integration test.
//!
//! Bootstrap nodes A + B admit a third node N at runtime, all three finalize a
//! message (delivered to N), then N departs cleanly and the survivors A + B
//! finalize another message without it.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, write_manifest, Client};
use std::process::Child;
use std::time::Duration;

const ADDR_A: &str = "127.0.0.1:9447";
const ADDR_B: &str = "127.0.0.1:9448";
const ADDR_N: &str = "127.0.0.1:9449";

fn main() {
    let a = seeded_key("mesh-membership-a");
    let b = seeded_key("mesh-membership-b");
    let n = seeded_key("mesh-membership-n");
    let (pa, pb, pn) = (pubkey(&a), pubkey(&b), pubkey(&n));

    // Bootstrap set = {A, B}. N is NOT in it — it joins at runtime.
    let boot = format!(r#"["{}","{}"]"#, hex(&pa), hex(&pb));
    let init_a = format!(r#"{{"node_seed":"mesh-membership-a","listen_addr":"{ADDR_A}","members":{boot}}}"#);
    let init_b = format!(r#"{{"node_seed":"mesh-membership-b","listen_addr":"{ADDR_B}","members":{boot},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#, hex(&pa));
    let init_n = format!(r#"{{"node_seed":"mesh-membership-n","listen_addr":"{ADDR_N}","members":{boot},"dial":[{{"pubkey":"{}","address":"{ADDR_A}"}}]}}"#, hex(&pa));

    for s in ["a", "b", "n"] {
        let _ = std::fs::remove_dir_all(format!("/tmp/mesh-mem-{s}-store"));
    }
    write_manifest("/tmp/mesh-mem-a.toml", &init_a, "/tmp/mesh-mem-a-store", "mesh-mem-a");
    write_manifest("/tmp/mesh-mem-b.toml", &init_b, "/tmp/mesh-mem-b-store", "mesh-mem-b");
    write_manifest("/tmp/mesh-mem-n.toml", &init_n, "/tmp/mesh-mem-n-store", "mesh-mem-n");

    let mut children: Vec<Child> = Vec::new();
    children.push(spawn_mesh("/tmp/mesh-mem-a.toml", "/tmp/mesh-mem-a.log"));
    assert!(wait_for_port(ADDR_A, Duration::from_secs(5)), "A failed to listen");
    children.push(spawn_mesh("/tmp/mesh-mem-b.toml", "/tmp/mesh-mem-b.log"));
    assert!(wait_for_port(ADDR_B, Duration::from_secs(5)), "B failed to listen");
    std::thread::sleep(Duration::from_millis(800)); // A–B sync
    println!("✓ bootstrap nodes A + B up and synced");

    let result = (|children: &mut Vec<Child>| -> Result<(), String> {
        // 1) A admits N (finalizes among {A, B}).
        let mut client_a = Client::connect(ADDR_A, &a).map_err(|e| format!("connect A: {e}"))?;
        client_a.introduce(&pn).map_err(|e| format!("introduce: {e}"))?;
        std::thread::sleep(Duration::from_millis(1200));
        println!("✓ A introduced N; introduction finalizing");

        // 2) N joins: it's a member now, so its handshake to A succeeds; it syncs
        //    and starts witnessing.
        children.push(spawn_mesh("/tmp/mesh-mem-n.toml", "/tmp/mesh-mem-n.log"));
        assert!(wait_for_port(ADDR_N, Duration::from_secs(5)), "N failed to listen");
        std::thread::sleep(Duration::from_millis(1500));
        println!("✓ N spawned and joined the network");

        // 3) A message finalizes across {A, B, N} and is delivered to N.
        let mut client_n = Client::connect(ADDR_N, &n).map_err(|e| format!("connect N: {e}"))?;
        let body1 = b"welcome, N";
        client_a.submit(body1).map_err(|e| format!("submit 1: {e}"))?;
        let (_from, got) = client_n.recv_message().map_err(|e| format!("N recv: {e}"))?;
        if got != body1 {
            return Err(format!("N got wrong body: {:?}", String::from_utf8_lossy(&got)));
        }
        println!("✓ message finalized across A, B, N — delivered to the new member");

        // 4) N departs cleanly.
        client_n.depart().map_err(|e| format!("depart: {e}"))?;
        std::thread::sleep(Duration::from_millis(1500));
        println!("✓ N departed; departure finalizing among A, B");

        // 5) The survivors finalize a message without N.
        let mut client_b = Client::connect(ADDR_B, &b).map_err(|e| format!("connect B: {e}"))?;
        let body2 = b"carry on";
        client_a.submit(body2).map_err(|e| format!("submit 2: {e}"))?;
        let (_f, got2) = client_b.recv_message().map_err(|e| format!("B recv: {e}"))?;
        if got2 != body2 {
            return Err(format!("B got wrong body: {:?}", String::from_utf8_lossy(&got2)));
        }
        println!("✓ survivors A + B finalized a message without N");
        Ok(())
    })(&mut children);

    for mut c in children {
        let _ = c.kill();
        let _ = c.wait();
    }

    match result {
        Ok(_) => {
            println!("\nMEMBERSHIP TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nMEMBERSHIP TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
