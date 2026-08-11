//! Confluence under partition — the flagship integration test.
//!
//! Attacks DESIGN-rsm.md principle 4 (correctness rests on CONFLUENCE, not
//! linearization). Two members author CONCURRENTLY while partitioned, then the
//! network heals; the converged state must be identical everywhere AND semantically
//! correct — specifically the OR-Set add-wins / ancestry-relative claim:
//!
//!   alice `member-remove(bob)`   ┐ concurrent, neither in the other's ancestry
//!   bob   `text("hi")`           ┘
//!   → after heal, on EVERY node: bob is removed AND bob's message still stands
//!     (it was valid in its own causal past, which never saw the removal).
//!
//! Partition model (nodes dial once, no reconnect): A and B are stable participants
//! that never dial each other; a bridge node is "the network". The seed bridge
//! relays alice's genesis to both; then it is KILLED (partition) while A and B
//! author concurrently in isolation; then a fresh heal bridge reconnects them and
//! gossip reconciles the two chains. A and B stay up the whole time, so this is a
//! true link partition — no participant restarts.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client};
use serde::Deserialize;
use std::fs;
use std::process::Child;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_chat.wasm";
const ADDR_A: &str = "127.0.0.1:9461";
const ADDR_B: &str = "127.0.0.1:9462";
const ADDR_C: &str = "127.0.0.1:9463"; // seed bridge
const ADDR_C2: &str = "127.0.0.1:9464"; // heal bridge (fresh port avoids TIME_WAIT)

// ---- chat payloads via chat-protocol (Graph-ABI / packr) — one owner, no drift ----
use chat_protocol::Msg;

fn genesis(members: &[[u8; 32]]) -> Vec<u8> {
    chat_protocol::encode(&Msg::Genesis { members: members.iter().map(|m| m.to_vec()).collect() })
}

fn text(body: &str) -> Vec<u8> {
    chat_protocol::encode(&Msg::Text { body: body.to_string() })
}

fn member_remove(subject: &[u8; 32]) -> Vec<u8> {
    chat_protocol::encode(&Msg::MemberRemove { subject: subject.to_vec() })
}

// ---- current-state (chat-sm ChatState) parse ----
#[derive(Deserialize)]
struct ChatState {
    members: Vec<(Vec<u8>, Vec<u8>)>, // OR-Set (pubkey, tag) entries
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

impl ChatState {
    fn parse(bytes: &[u8]) -> Result<ChatState, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("parse current-state: {e}"))
    }
    fn distinct_members(&self) -> Vec<Vec<u8>> {
        let mut m: Vec<Vec<u8>> = self.members.iter().map(|(pk, _)| pk.clone()).collect();
        m.sort();
        m.dedup();
        m
    }
    fn has_body(&self, b: &str) -> bool {
        self.log.iter().any(|m| m.body == b)
    }
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
        r#"name = "mesh-confluence"
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
store_id = "mesh-confluence"
"#
    );
    fs::write(path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
}

fn spawn(seed: &str, addr: &str, dials: &[(&str, &str)], tag: &str) -> Result<Child, String> {
    let manifest = format!("/tmp/mesh-conf-{tag}.toml");
    let store = format!("/tmp/mesh-conf-{tag}-store");
    write_manifest(&manifest, seed, addr, dials, &store);
    let child = spawn_mesh(&manifest, &format!("/tmp/mesh-conf-{tag}.log"));
    if !wait_for_port(addr, Duration::from_secs(5)) {
        return Err(format!("node {tag} failed to listen on {addr}"));
    }
    Ok(child)
}

/// Connect a throwaway observer to `addr`, query current-state, parse it. Returns
/// the raw bytes too (for byte-identical convergence comparison).
fn state_of(addr: &str, observer_seed: &str) -> Result<(Vec<u8>, ChatState), String> {
    let key = seeded_key(observer_seed);
    let mut c = Client::connect(addr, &key).map_err(|e| format!("connect {addr}: {e}"))?;
    let bytes = c.current_state().map_err(|e| format!("current-state {addr}: {e}"))?;
    let parsed = ChatState::parse(&bytes)?;
    Ok((bytes, parsed))
}

fn run() -> Result<(), String> {
    let key_a = seeded_key("conf-alice-seed");
    let key_b = seeded_key("conf-bob-seed");
    let pk_a = pubkey(&key_a);
    let pk_b = pubkey(&key_b);
    let (hex_a, hex_b) = (hex(&pk_a), hex(&pk_b));
    println!("alice pk: {hex_a}");
    println!("bob   pk: {hex_b}");

    // A and B are stable participants; they never dial each other. The bridge is
    // the only link between them.
    let mut node_a = spawn("conf-alice-seed", ADDR_A, &[], "a")?;
    let mut node_b = spawn("conf-bob-seed", ADDR_B, &[], "b")?;
    println!("✓ A and B up (never dial each other)");

    // teardown-all guard threaded through the fallible steps.
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

    // --- seed: bring the bridge up FIRST, then alice authors genesis; the LIVE
    //     broadcast path (A→bridge→B) seeds both, and the bridge backfills the
    //     genesis chain's deps for B. ---
    let seed_bridge = ok_or_bail!(
        spawn("conf-seed-bridge", ADDR_C, &[(&hex_a, ADDR_A), (&hex_b, ADDR_B)], "c"),
        "seed bridge"
    );
    nodes.push(seed_bridge);
    std::thread::sleep(Duration::from_millis(400)); // bridge links to A and B
    {
        let mut ca = ok_or_bail!(Client::connect(ADDR_A, &key_a), "connect A");
        ok_or_bail!(ca.submit(&genesis(&[pk_a, pk_b])), "genesis");
    }
    println!("✓ alice authored genesis {{alice, bob}} (bridge relaying)");
    std::thread::sleep(Duration::from_millis(900)); // propagate A→bridge→B

    for (addr, seed) in [(ADDR_A, "obs-a1"), (ADDR_B, "obs-b1")] {
        let (_, st) = ok_or_bail!(state_of(addr, seed), "seed checkpoint");
        if st.distinct_members().len() != 2 {
            bail!(format!("seed failed: {addr} sees {} members, want 2", st.distinct_members().len()));
        }
    }
    println!("✓ room seeded to BOTH A and B (members = {{alice, bob}})");

    // --- partition: kill the bridge; A and B can no longer reach each other ---
    if let Some(mut b) = nodes.pop() {
        let _ = b.kill();
        let _ = b.wait();
    }
    std::thread::sleep(Duration::from_millis(400));
    println!("✓ bridge killed → A | B partitioned");

    // --- concurrent authoring across the partition ---
    {
        let mut ca = ok_or_bail!(Client::connect(ADDR_A, &key_a), "connect A");
        let mut cb = ok_or_bail!(Client::connect(ADDR_B, &key_b), "connect B");
        ok_or_bail!(ca.submit(&member_remove(&pk_b)), "remove(bob)");
        ok_or_bail!(cb.submit(&text("hi")), "text(hi)");
    }
    std::thread::sleep(Duration::from_millis(400));
    println!("✓ concurrent: alice removed bob | bob posted \"hi\" (neither saw the other)");

    // checkpoint: the partition is REAL — the two sides diverge right now.
    {
        let (_, sa) = ok_or_bail!(state_of(ADDR_A, "obs-a2"), "A partitioned state");
        let (_, sb) = ok_or_bail!(state_of(ADDR_B, "obs-b2"), "B partitioned state");
        if sa.distinct_members().len() != 1 {
            bail!(format!("A should have removed bob (1 member), has {}", sa.distinct_members().len()));
        }
        if !sb.has_body("hi") || sb.distinct_members().len() != 2 {
            bail!("B should still see bob as a member and hold \"hi\"".to_string());
        }
        println!("✓ partition confirmed divergent: A={{1 member, no msg}}, B={{2 members, \"hi\"}}");
    }

    // --- heal: a fresh bridge reconnects the partitions and gossip reconciles ---
    let heal_bridge = ok_or_bail!(
        spawn("conf-heal-bridge", ADDR_C2, &[(&hex_a, ADDR_A), (&hex_b, ADDR_B)], "c2"),
        "heal bridge"
    );
    nodes.push(heal_bridge);
    println!("✓ heal bridge up → gossip reconciling both chains");

    // --- the payoff: poll until A and B converge (gossip is asynchronous) ---
    let mut bytes_a = Vec::new();
    let mut sa_final = None;
    let mut converged = false;
    for _ in 0..24 {
        std::thread::sleep(Duration::from_millis(300));
        let a = state_of(ADDR_A, "obs-a3");
        let b = state_of(ADDR_B, "obs-b3");
        if let (Ok((ba, sa)), Ok((bb, _))) = (a, b) {
            if ba == bb && !ba.is_empty() {
                bytes_a = ba;
                sa_final = Some(sa);
                converged = true;
                break;
            }
        }
    }
    if !converged {
        let a = state_of(ADDR_A, "obs-adiv").map(|(b, _)| b).unwrap_or_default();
        let b = state_of(ADDR_B, "obs-bdiv").map(|(b, _)| b).unwrap_or_default();
        bail!(format!(
            "current-state did NOT converge after heal (~7s):\n  A: {}\n  B: {}",
            String::from_utf8_lossy(&a),
            String::from_utf8_lossy(&b)
        ));
    }
    let sa = sa_final.unwrap();
    println!("✓ current-state CONVERGED byte-for-byte across A and B ({} bytes)", bytes_a.len());

    let members = sa.distinct_members();
    if members.len() != 1 || members[0] != pk_a.to_vec() {
        bail!(format!("membership wrong: want {{alice}}, got {} members", members.len()));
    }
    if !sa.has_body("hi") {
        bail!("bob's concurrent \"hi\" was LOST from the converged state — a finalized event is missing from current-state (non-confluent global fold, DESIGN-rsm.md principle 4)".to_string());
    }
    println!("✓ semantics hold: bob removed AND bob's concurrent \"hi\" still stands (add-wins / ancestry-relative)");

    let _ = node_a.kill();
    let _ = node_b.kill();
    for mut n in nodes.drain(..) {
        let _ = n.kill();
    }
    Ok(())
}

fn main() {
    // clean stores from any prior run
    for tag in ["a", "b", "c", "c2"] {
        let _ = fs::remove_dir_all(format!("/tmp/mesh-conf-{tag}-store"));
    }

    let result = run();

    // best-effort teardown
    let _ = std::process::Command::new("pkill").args(["-x", "theater"]).status();

    match result {
        Ok(()) => {
            println!("\nCONFLUENCE TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nCONFLUENCE TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
