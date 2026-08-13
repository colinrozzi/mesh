//! The conflict frontier, LIVE — a double-spend the substrate admits.
//!
//! bank-sm is the first CONFLICT-PRONE consumer: a Transfer is valid iff the source
//! holds the amount, judged against the event's own ancestry. So two transfers from
//! one wallet, authored concurrently across a partition, are EACH valid — and v0's
//! admission-final finality makes both final. After heal the network converges
//! consistently... on an overdrawn (negative) balance.
//!
//!   mint alice 100   (seeded to both A and B)
//!   ── partition ──
//!   A: transfer alice→bob 60     ┐ concurrent, each valid vs its own ancestry (alice=100)
//!   B: transfer alice→carol 60   ┘
//!   ── heal ──
//!   → EVERY node converges byte-for-byte on: alice = -20, bob = 60, carol = 60.
//!
//! The substrate is CONSISTENT (both nodes agree) but the currency invariant is
//! violated. That is the precise signature of the conflict-prone frontier: admission-
//! final finality is consistent yet NOT sufficient for a currency — the deferred
//! witness-finality bundle is what would reject one transfer. This test PASSES by
//! documenting exactly that: convergence holds AND alice is negative.
//!
//! Partition model mirrors the confluence test: A and B are stable and never dial each
//! other; a bridge node is "the network" (spawn = link, kill = partition, respawn = heal).

use bank_protocol::{BankState, Cmd};
use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client};
use std::fs;
use std::process::Child;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_bank.wasm";
const ADDR_A: &str = "127.0.0.1:9521";
const ADDR_B: &str = "127.0.0.1:9522";
const ADDR_C: &str = "127.0.0.1:9523"; // seed bridge
const ADDR_C2: &str = "127.0.0.1:9524"; // heal bridge (fresh port avoids TIME_WAIT)

fn mint(to: &str, amount: u64) -> Vec<u8> {
    bank_protocol::encode(&Cmd::Mint { to: to.to_string(), amount })
}
fn transfer(from: &str, to: &str, amount: u64) -> Vec<u8> {
    bank_protocol::encode(&Cmd::Transfer { from: from.to_string(), to: to.to_string(), amount })
}

// The node's `current-state` returns bank-sm's TYPED state on the wire (Graph ABI), so the
// harness decodes it through the SAME `BankState` schema the SM folds — no serde, one owner.
fn parse_state(bytes: &[u8]) -> Result<BankState, String> {
    bank_protocol::decode_state(bytes).ok_or_else(|| "decode current-state (BankState)".to_string())
}

fn write_manifest(path: &str, seed: &str, addr: &str, dials: &[(&str, &str)], store: &str) {
    let dial_json = if dials.is_empty() {
        String::new()
    } else {
        let entries: Vec<String> = dials
            .iter()
            .map(|(pk, a)| format!(r#"{{"pubkey":"{pk}","address":"{a}"}}"#))
            .collect();
        format!(r#","dial":[{}]"#, entries.join(","))
    };
    let init = format!(r#"{{"node_seed":"{seed}","listen_addr":"{addr}"{dial_json}}}"#);
    let toml = format!(
        r#"name = "mesh-bank-ds"
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
store_id = "mesh-bank-ds"
"#
    );
    fs::write(path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
}

fn spawn(seed: &str, addr: &str, dials: &[(&str, &str)], tag: &str) -> Result<Child, String> {
    let manifest = format!("/tmp/mesh-bankds-{tag}.toml");
    let store = format!("/tmp/mesh-bankds-{tag}-store");
    write_manifest(&manifest, seed, addr, dials, &store);
    let child = spawn_mesh(&manifest, &format!("/tmp/mesh-bankds-{tag}.log"));
    if !wait_for_port(addr, Duration::from_secs(5)) {
        return Err(format!("node {tag} failed to listen on {addr}"));
    }
    Ok(child)
}

fn state_of(addr: &str, observer_seed: &str) -> Result<(Vec<u8>, BankState), String> {
    let key = seeded_key(observer_seed);
    let mut c = Client::connect(addr, &key).map_err(|e| format!("connect {addr}: {e}"))?;
    let bytes = c.current_state().map_err(|e| format!("current-state {addr}: {e}"))?;
    let parsed = parse_state(&bytes)?;
    Ok((bytes, parsed))
}

fn run() -> Result<(), String> {
    let key_a = seeded_key("bankds-a-seed");
    let key_b = seeded_key("bankds-b-seed");
    let (hex_a, hex_b) = (hex(&pubkey(&key_a)), hex(&pubkey(&key_b)));

    let mut node_a = spawn("bankds-a-seed", ADDR_A, &[], "a")?;
    let mut node_b = spawn("bankds-b-seed", ADDR_B, &[], "b")?;
    println!("✓ A and B up (never dial each other)");

    let mut nodes: Vec<Child> = Vec::new();
    macro_rules! bail {
        ($e:expr) => {{
            let _ = node_a.kill();
            let _ = node_b.kill();
            for mut n in nodes.drain(..) {
                let _ = n.kill();
            }
            return Err($e);
        }};
    }
    macro_rules! ok_or_bail {
        ($r:expr, $ctx:expr) => {
            match $r {
                Ok(v) => v,
                Err(e) => bail!(format!("{}: {e}", $ctx)),
            }
        };
    }

    // --- seed: bridge up, then mint alice 100; live broadcast seeds both A and B ---
    let seed_bridge = ok_or_bail!(
        spawn("bankds-seed-bridge", ADDR_C, &[(&hex_a, ADDR_A), (&hex_b, ADDR_B)], "c"),
        "seed bridge"
    );
    nodes.push(seed_bridge);
    std::thread::sleep(Duration::from_millis(400));
    {
        let mut ca = ok_or_bail!(Client::connect(ADDR_A, &key_a), "connect A");
        ok_or_bail!(ca.submit(&mint("alice", 100)), "mint alice 100");
    }
    println!("✓ minted alice=100 (bridge relaying)");
    std::thread::sleep(Duration::from_millis(900));

    for (addr, seed) in [(ADDR_A, "obs-a1"), (ADDR_B, "obs-b1")] {
        let (_, st) = ok_or_bail!(state_of(addr, seed), "seed checkpoint");
        if st.balance("alice") != 100 {
            bail!(format!("seed failed: {addr} sees alice={}, want 100", st.balance("alice")));
        }
    }
    println!("✓ alice=100 seeded to BOTH A and B");

    // --- partition ---
    if let Some(mut b) = nodes.pop() {
        let _ = b.kill();
        let _ = b.wait();
    }
    std::thread::sleep(Duration::from_millis(400));
    println!("✓ bridge killed → A | B partitioned");

    // --- concurrent double-spend: each side transfers 60 out of alice's 100 ---
    {
        let mut ca = ok_or_bail!(Client::connect(ADDR_A, &key_a), "connect A");
        let mut cb = ok_or_bail!(Client::connect(ADDR_B, &key_b), "connect B");
        ok_or_bail!(ca.submit(&transfer("alice", "bob", 60)), "A: alice→bob 60");
        ok_or_bail!(cb.submit(&transfer("alice", "carol", 60)), "B: alice→carol 60");
    }
    std::thread::sleep(Duration::from_millis(400));
    println!("✓ concurrent: A spent alice→bob 60 | B spent alice→carol 60 (each saw alice=100)");

    {
        let (_, sa) = ok_or_bail!(state_of(ADDR_A, "obs-a2"), "A partitioned state");
        let (_, sb) = ok_or_bail!(state_of(ADDR_B, "obs-b2"), "B partitioned state");
        if sa.balance("alice") != 40 || sa.balance("bob") != 60 {
            bail!(format!("A should show alice=40,bob=60; got alice={},bob={}", sa.balance("alice"), sa.balance("bob")));
        }
        if sb.balance("alice") != 40 || sb.balance("carol") != 60 {
            bail!(format!("B should show alice=40,carol=60; got alice={},carol={}", sb.balance("alice"), sb.balance("carol")));
        }
        println!("✓ partition confirmed divergent: A={{alice:40,bob:60}}, B={{alice:40,carol:60}}");
    }

    // --- heal ---
    let heal_bridge = ok_or_bail!(
        spawn("bankds-heal-bridge", ADDR_C2, &[(&hex_a, ADDR_A), (&hex_b, ADDR_B)], "c2"),
        "heal bridge"
    );
    nodes.push(heal_bridge);
    println!("✓ heal bridge up → gossip reconciling both chains");

    // --- payoff: poll until A and B converge byte-for-byte ---
    let mut bytes_a = Vec::new();
    let mut sa_final = None;
    let mut converged = false;
    for _ in 0..24 {
        std::thread::sleep(Duration::from_millis(300));
        if let (Ok((ba, sa)), Ok((bb, _))) = (state_of(ADDR_A, "obs-a3"), state_of(ADDR_B, "obs-b3")) {
            if ba == bb && !ba.is_empty() {
                bytes_a = ba;
                sa_final = Some(sa);
                converged = true;
                break;
            }
        }
    }
    if !converged {
        bail!("current-state did NOT converge after heal (~7s)".to_string());
    }
    let sa = sa_final.unwrap();
    println!("✓ current-state CONVERGED byte-for-byte across A and B ({} bytes)", bytes_a.len());

    // The frontier: BOTH transfers finalized (admission-final) → alice overdrawn. The
    // substrate is consistent but the invariant is broken.
    if sa.balance("alice") != -20 || sa.balance("bob") != 60 || sa.balance("carol") != 60 {
        bail!(format!(
            "expected the double-spend to land: alice=-20,bob=60,carol=60; got alice={},bob={},carol={}",
            sa.balance("alice"),
            sa.balance("bob"),
            sa.balance("carol")
        ));
    }
    println!("✓ DOUBLE-SPEND ADMITTED: alice={} (100−60−60), bob=60, carol=60", sa.balance("alice"));
    println!("  → the substrate is CONSISTENT (both nodes agree) but the currency invariant is broken:");
    println!("    admission-final finality is not sufficient for a conflict-prone SM. This is the");
    println!("    exact case the deferred witness-finality bundle would reject one transfer for.");

    let _ = node_a.kill();
    let _ = node_b.kill();
    for mut n in nodes.drain(..) {
        let _ = n.kill();
    }
    Ok(())
}

fn main() {
    for tag in ["a", "b", "c", "c2"] {
        let _ = fs::remove_dir_all(format!("/tmp/mesh-bankds-{tag}-store"));
    }
    let result = run();
    let _ = std::process::Command::new("pkill").args(["-x", "theater"]).status();
    match result {
        Ok(()) => {
            println!("\nBANK DOUBLE-SPEND TEST PASSED (frontier documented)");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nBANK DOUBLE-SPEND TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
