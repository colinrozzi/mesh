//! v0 chat-plane smoke — the second oracle, proving the dumb core generalizes: the
//! SAME node composed with a DIFFERENT SM (`chat-sm`, OR-Set membership + a text
//! log) runs a real room across two gossiping nodes. Crucially it exercises the
//! **rejection path**: a non-member's post must NOT be delivered, and a
//! `member-add` then admits the same author — the ancestry-relative fold in action.
//!
//! Two nodes (a node authors under its own key, so each identity is a node):
//!   - node A ("alice") on :9453 — genesis creator, sole initial member.
//!   - node B ("bob")   on :9454 — dials A; NOT in genesis until added.
//!
//! Assertions use dedicated receive-only clients so no NOTIFY is ever dropped by an
//! intervening ACK read (a submitting client's `read_ack` skips queued NOTIFYs).
//!   positive: bob's receiver sees alice's "hello".
//!   negative + positive: alice's receiver sees bob's post-add "hi", and NEVER his
//!   pre-add "sneaky" (which, authored before "hi", would arrive first if wrongly
//!   admitted) — so seeing "hi" without "sneaky" proves the non-member post was
//!   rejected.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client};
use std::fs;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_chat.wasm";
const ADDR_A: &str = "127.0.0.1:9453";
const ADDR_B: &str = "127.0.0.1:9454";
const SEED_A: &str = "rsm-chat-alice-seed";
const SEED_B: &str = "rsm-chat-bob-seed";

// ---- chat payload codec: [version u16][kind u8][content] (mirrors chat-sm) ----
const VERSION: u16 = 0;

fn genesis(members: &[[u8; 32]]) -> Vec<u8> {
    let mut o = VERSION.to_be_bytes().to_vec();
    o.push(0);
    o.extend_from_slice(&(members.len() as u16).to_be_bytes());
    for m in members {
        o.extend_from_slice(m);
    }
    o
}

fn text(body: &str) -> Vec<u8> {
    let mut o = VERSION.to_be_bytes().to_vec();
    o.push(1);
    o.extend_from_slice(body.as_bytes());
    o
}

fn member_add(subject: &[u8; 32]) -> Vec<u8> {
    let mut o = VERSION.to_be_bytes().to_vec();
    o.push(2);
    o.extend_from_slice(subject);
    o
}

/// Decode a `text` payload → its body. `None` for any other kind so the receive
/// loop skips genesis/member-add NOTIFYs.
fn decode_text(p: &[u8]) -> Option<String> {
    if p.len() < 3 || u16::from_be_bytes([p[0], p[1]]) != VERSION || p[2] != 1 {
        return None;
    }
    String::from_utf8(p[3..].to_vec()).ok()
}

fn write_manifest(path: &str, seed: &str, addr: &str, dial: Option<(&str, &str)>, store: &str) {
    let dial_json = match dial {
        Some((pubkey_hex, address)) => {
            format!(r#","dial":[{{"pubkey":"{pubkey_hex}","address":"{address}"}}]"#)
        }
        None => String::new(),
    };
    let init = format!(r#"{{"node_seed":"{seed}","listen_addr":"{addr}"{dial_json}}}"#);
    let toml = format!(
        r#"name = "mesh-chat-smoke"
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
store_id = "mesh-chat-smoke"
"#
    );
    fs::write(path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
}

/// Read up to `budget` deliveries, returning the first text body that satisfies
/// `want`. Fails hard if `forbid` is seen first (the negative assertion).
fn expect_text(
    client: &mut Client,
    budget: usize,
    want: impl Fn(&[u8; 32], &str) -> bool,
    forbid: &str,
) -> Result<String, String> {
    for _ in 0..budget {
        let (from, body) = client.recv_message().map_err(|e| format!("recv: {e}"))?;
        if let Some(t) = decode_text(&body) {
            if t == forbid {
                return Err(format!("forbidden message was delivered: {forbid:?}"));
            }
            if want(&from, &t) {
                return Ok(t);
            }
        }
    }
    Err("expected message never arrived".to_string())
}

fn main() {
    let key_a = seeded_key(SEED_A);
    let key_b = seeded_key(SEED_B);
    let pk_a = pubkey(&key_a);
    let pk_b = pubkey(&key_b);
    // Throwaway identities for the receive-only observers (transport is permissive).
    let key_ra = seeded_key("rsm-chat-observer-a");
    let key_rb = seeded_key("rsm-chat-observer-b");
    println!("alice (node A) pubkey: {}", hex(&pk_a));
    println!("bob   (node B) pubkey: {}", hex(&pk_b));

    let _ = fs::remove_dir_all("/tmp/mesh-chat-a-store");
    let _ = fs::remove_dir_all("/tmp/mesh-chat-b-store");
    write_manifest("/tmp/mesh-chat-a.toml", SEED_A, ADDR_A, None, "/tmp/mesh-chat-a-store");
    write_manifest(
        "/tmp/mesh-chat-b.toml",
        SEED_B,
        ADDR_B,
        Some((&hex(&pk_a), ADDR_A)),
        "/tmp/mesh-chat-b-store",
    );

    let mut child_a = spawn_mesh("/tmp/mesh-chat-a.toml", "/tmp/mesh-chat-a.log");
    if !wait_for_port(ADDR_A, Duration::from_secs(5)) {
        eprintln!("node A failed to listen (see /tmp/mesh-chat-a.log)");
        let _ = child_a.kill();
        std::process::exit(1);
    }
    println!("✓ node A listening on {ADDR_A}");

    let mut child_b = spawn_mesh("/tmp/mesh-chat-b.toml", "/tmp/mesh-chat-b.log");
    if !wait_for_port(ADDR_B, Duration::from_secs(5)) {
        eprintln!("node B failed to listen (see /tmp/mesh-chat-b.log)");
        let _ = child_a.kill();
        let _ = child_b.kill();
        std::process::exit(1);
    }
    println!("✓ node B listening on {ADDR_B} (dialing A)");

    std::thread::sleep(Duration::from_millis(800)); // dial + genesis-frontier exchange

    let result = (|| -> Result<(), String> {
        // Receive-only observers connect FIRST so they see the full delivery stream
        // (a node NOTIFYs only newly-final events, never a backlog to a late conn).
        let mut ra = Client::connect(ADDR_A, &key_ra).map_err(|e| format!("connect ra: {e}"))?;
        let mut rb = Client::connect(ADDR_B, &key_rb).map_err(|e| format!("connect rb: {e}"))?;
        let mut ca = Client::connect(ADDR_A, &key_a).map_err(|e| format!("connect A: {e}"))?;
        let mut cb = Client::connect(ADDR_B, &key_b).map_err(|e| format!("connect B: {e}"))?;
        println!("✓ observers + control clients connected");

        // alice creates the room (sole member) and posts.
        ca.submit(&genesis(&[pk_a])).map_err(|e| format!("genesis: {e}"))?;
        ca.submit(&text("hello from alice")).map_err(|e| format!("hello: {e}"))?;
        println!("✓ alice authored genesis + \"hello from alice\"");
        std::thread::sleep(Duration::from_millis(600)); // propagate A→B

        // positive: bob's observer sees alice's message across the mesh.
        let got = expect_text(&mut rb, 8, |from, _| *from == pk_a, "\0none")?;
        println!("✓ bob's node delivered alice's message: {got:?}");

        // bob (not yet a member) tries to post — `author` is PRE-VALIDATED, so this
        // is rejected at submit time with the SM's reason (no never-finalizing event
        // is ever created), not silently dropped downstream.
        match cb.submit(&text("sneaky")) {
            Ok(_) => {
                return Err("bob's non-member post was accepted — author failed to pre-validate".to_string())
            }
            Err(e) => println!("✓ bob's non-member \"sneaky\" rejected at author time: {e}"),
        }
        std::thread::sleep(Duration::from_millis(300));

        // alice admits bob, then bob posts a legitimate message.
        ca.submit(&member_add(&pk_b)).map_err(|e| format!("member-add: {e}"))?;
        println!("✓ alice authored member-add(bob)");
        std::thread::sleep(Duration::from_millis(600)); // propagate A→B so bob sees it

        cb.submit(&text("hi from bob")).map_err(|e| format!("hi: {e}"))?;
        println!("✓ bob authored \"hi from bob\" (now a member)");

        // positive + belt-and-suspenders negative: alice's observer must see bob's
        // post-add message, and NEVER "sneaky" (which — pre-validated away at author
        // time — was never even created, so it can't appear).
        let got = expect_text(&mut ra, 12, |from, t| *from == pk_b && t == "hi from bob", "sneaky")?;
        println!("✓ alice's node delivered bob's post-add message: {got:?}");
        println!("✓ bob's pre-add \"sneaky\" never entered the mesh (rejected at author time)");
        Ok(())
    })();

    let _ = child_a.kill();
    let _ = child_b.kill();
    let _ = child_a.wait();
    let _ = child_b.wait();

    match result {
        Ok(()) => {
            println!("\nCHAT SMOKE TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nCHAT SMOKE TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
