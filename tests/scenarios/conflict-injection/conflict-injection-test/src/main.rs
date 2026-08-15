//! Fail-loud conflict injection — the ONE node-safety path that never fires in an
//! honest mesh. DESIGN-rsm.md's only baked-in conflict behavior is "loser inert",
//! surfaced as a `conflict` event. With pre-validated `author`, an honest node never
//! produces an SM-invalid event — so the only way to exercise the net is a DISHONEST
//! peer that gossips a properly-signed but SM-invalid event, bypassing `author`.
//!
//! Vehicle: bank-sm (a mesh-owned example — this is a NODE property, not a bank one).
//! alice is minted 100. A forged transfer of 999 out of alice — signed by a stranger
//! key, ref: the mint (so it is structurally admissible) — is injected via the raw
//! gossip frame. bank's `validate` rejects it (insufficient balance) against its
//! ancestry, and the node must:
//!   1. emit a `conflict` naming the event + the SM's reason,
//!   2. keep it INERT — never `finalized`, absent from `current-state`,
//!   3. classify it `stranded` via `event-status`.

use bank_protocol::{BankState, Cmd};
use mesh_testkit::{
    forge_event, hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client, STATUS_STRANDED,
};
use std::fs;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_bank.wasm";
const ADDR: &str = "127.0.0.1:9471";

fn mint(to: &str, amount: u64) -> Vec<u8> {
    bank_protocol::encode(&Cmd::Mint { to: to.to_string(), amount })
}
fn transfer(from: &str, to: &str, amount: u64) -> Vec<u8> {
    bank_protocol::encode(&Cmd::Transfer { from: from.to_string(), to: to.to_string(), amount })
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
    let key_mallory = seeded_key("conflict-mallory-seed"); // the dishonest peer
    let pk_mallory = pubkey(&key_mallory);
    println!("alice (node)   : {}", hex(&pk_alice));
    println!("mallory (forger): {}", hex(&pk_mallory));

    // alice mints herself 100 (the honest origin of value); capture the mint hash.
    let mut ca = Client::connect(ADDR, &key_alice).map_err(|e| format!("connect alice: {e}"))?;
    let mint_hash = ca.submit(&mint("alice", 100)).map_err(|e| format!("mint: {e}"))?;
    println!("✓ alice minted 100 (mint {})", hex(&mint_hash));

    // Observer connects BEFORE the injection so it catches the conflict broadcast.
    let mut obs = Client::connect(ADDR, &seeded_key("conflict-observer")).map_err(|e| format!("connect obs: {e}"))?;

    // Forge mallory's invalid event: a transfer of 999 out of alice (she has 100), ref:
    // the mint (so it is structurally admissible). alice's balance is insufficient, so
    // `validate` must reject it against its ancestry.
    let (evil_id, evil_bytes) =
        forge_event(&key_mallory, 1, None, &[mint_hash], &transfer("alice", "mallory", 999));
    let mut mallory = Client::connect(ADDR, &key_mallory).map_err(|e| format!("connect mallory: {e}"))?;
    mallory.gossip_raw(&evil_bytes).map_err(|e| format!("inject: {e}"))?;
    println!("✓ dishonest peer injected a signed over-balance transfer ({})", hex(&evil_id));

    // 1. the node must surface it as a conflict naming the event + the SM's reason.
    let (conflict_id, reason) = obs.recv_conflict().map_err(|e| format!("await conflict: {e}"))?;
    if conflict_id != evil_id {
        return Err(format!("conflict names {}, expected {}", hex(&conflict_id), hex(&evil_id)));
    }
    let r = reason.to_lowercase();
    if !r.contains("balance") && !r.contains("insufficient") {
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
    let st: BankState = bank_protocol::decode_state(&bytes)
        .ok_or_else(|| "decode current-state (BankState)".to_string())?;
    if st.balance("alice") != 100 {
        return Err(format!("alice perturbed: balance {}, want 100", st.balance("alice")));
    }
    if st.balance("mallory") != 0 {
        return Err(format!("mallory wrongly credited: balance {}", st.balance("mallory")));
    }
    println!("✓ forged event is inert: alice still 100, mallory never credited");
    Ok(())
}
