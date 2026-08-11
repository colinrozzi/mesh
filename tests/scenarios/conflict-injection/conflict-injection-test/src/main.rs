//! Fail-loud conflict injection — exercises the ONE safety path that never fires
//! in an honest mesh. DESIGN-rsm.md's only baked-in conflict behavior is "loser
//! inert", surfaced as a `conflict` event. With pre-validated `author` and
//! conflict-free consumers, an honest node never produces an invalid event — so the
//! only way to test the net is a DISHONEST peer that gossips a properly-signed but
//! SM-invalid event, bypassing `author`.
//!
//! Setup: one node hosts a chat room {alice, bob}. A forged event — authored by a
//! NON-MEMBER key, signed correctly, ref: the room genesis (so it's structurally
//! admissible) — is injected via the raw gossip frame. The node ingests it, the
//! fold finds it invalid against its ancestry, and must:
//!   1. emit a `conflict` naming the event + the SM's reason,
//!   2. keep it INERT — never `finalized`, absent from `current-state`,
//!   3. classify it `stranded` via `event-status`.

use mesh_testkit::{
    forge_event, hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client, STATUS_STRANDED,
};
use serde::Deserialize;
use std::fs;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_chat.wasm";
const ADDR: &str = "127.0.0.1:9471";

use chat_protocol::Msg;

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
    body: String,
}

fn write_manifest(path: &str, seed: &str, store: &str) {
    let init = format!(r#"{{"node_seed":"{seed}","listen_addr":"{ADDR}"}}"#);
    let toml = format!(
        r#"name = "mesh-conflict-injection"
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
store_id = "mesh-conflict-injection"
"#
    );
    fs::write(path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
}

fn main() {
    let _ = fs::remove_dir_all("/tmp/mesh-conflict-store");
    write_manifest("/tmp/mesh-conflict.toml", "conflict-alice-seed", "/tmp/mesh-conflict-store");

    let mut node = spawn_mesh("/tmp/mesh-conflict.toml", "/tmp/mesh-conflict.log");
    if !wait_for_port(ADDR, Duration::from_secs(5)) {
        eprintln!("node failed to listen");
        let _ = node.kill();
        std::process::exit(1);
    }
    println!("✓ node listening on {ADDR}");

    let result = run();

    let _ = node.kill();
    let _ = node.wait();

    match result {
        Ok(()) => {
            println!("\nCONFLICT INJECTION TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nCONFLICT INJECTION TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    let key_alice = seeded_key("conflict-alice-seed"); // the node's own key
    let pk_alice = pubkey(&key_alice);
    let pk_bob = pubkey(&seeded_key("conflict-bob-seed"));
    let key_mallory = seeded_key("conflict-mallory-seed"); // the dishonest NON-member
    let pk_mallory = pubkey(&key_mallory);
    println!("alice (member) : {}", hex(&pk_alice));
    println!("mallory (NOT a member): {}", hex(&pk_mallory));

    // alice creates the room {alice, bob}; capture the genesis event hash.
    let mut ca = Client::connect(ADDR, &key_alice).map_err(|e| format!("connect alice: {e}"))?;
    let genesis_hash = ca.submit(&genesis(&[pk_alice, pk_bob])).map_err(|e| format!("genesis: {e}"))?;
    println!("✓ room {{alice, bob}} created (genesis {})", hex(&genesis_hash));

    // Observer connects BEFORE the injection so it catches the conflict broadcast.
    let mut obs = Client::connect(ADDR, &seeded_key("conflict-observer")).map_err(|e| format!("connect obs: {e}"))?;

    // Forge mallory's invalid event: a genesis-rooted (self_parent = None) text that
    // refs the room genesis, so it is structurally admissible; but mallory is not a
    // member, so `validate` must reject it against its ancestry.
    let (evil_id, evil_bytes) = forge_event(&key_mallory, 1, None, &[genesis_hash], &text("evil forged message"));
    let mut mallory = Client::connect(ADDR, &key_mallory).map_err(|e| format!("connect mallory: {e}"))?;
    mallory.gossip_raw(&evil_bytes).map_err(|e| format!("inject: {e}"))?;
    println!("✓ dishonest peer injected a signed non-member event ({})", hex(&evil_id));

    // 1. the node must surface it as a conflict naming the event + the SM's reason.
    let (conflict_id, reason) = obs.recv_conflict().map_err(|e| format!("await conflict: {e}"))?;
    if conflict_id != evil_id {
        return Err(format!("conflict names {}, expected {}", hex(&conflict_id), hex(&evil_id)));
    }
    if !reason.to_lowercase().contains("member") {
        return Err(format!("conflict reason unexpected: {reason:?}"));
    }
    println!("✓ node fired CONFLICT for the forged event: {reason:?}");

    // 2. it must be inert — stranded per event-status, absent from current-state.
    let status = ca.event_status(&evil_id).map_err(|e| format!("event-status: {e}"))?;
    if status != STATUS_STRANDED {
        return Err(format!("event-status(evil) = {status}, want stranded ({STATUS_STRANDED})"));
    }
    println!("✓ event-status(forged) = stranded");

    let bytes = ca.current_state().map_err(|e| format!("current-state: {e}"))?;
    let st: ChatState = serde_json::from_slice(&bytes).map_err(|e| format!("parse state: {e}"))?;
    if st.log.iter().any(|m| m.body == "evil forged message") {
        return Err("the forged message leaked into current-state".to_string());
    }
    let members: std::collections::BTreeSet<Vec<u8>> = st.members.iter().map(|(pk, _)| pk.clone()).collect();
    if members.contains(&pk_mallory.to_vec()) {
        return Err("mallory was wrongly admitted as a member".to_string());
    }
    if members.len() != 2 {
        return Err(format!("membership perturbed: {} members, want 2", members.len()));
    }
    println!("✓ forged event is inert: absent from the log, mallory not a member, room intact");
    Ok(())
}
