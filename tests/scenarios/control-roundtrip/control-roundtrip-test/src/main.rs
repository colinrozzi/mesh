//! v0 control-plane round-trip oracle — the end-to-end proof that a dumb node
//! composed with the real `control-sm` runs a full genesis → join → command →
//! response cycle across two gossiping nodes and delivers the answer.
//!
//! A node authors under its OWN key (SUBMIT is "author this payload on my chain"),
//! so the two control-plane identities are two separate nodes:
//!   - node S ("sentinel") on :9451 — owns genesis + answers commands.
//!   - node M ("manager")  on :9452 — dials S; joins, then issues a command.
//! Both run the SAME composed artifact (`mesh_control.wasm` = node ⊕ control-sm,
//! composed with the FIXED packr — the 0.12.2 nix binary produces an artifact that
//! fails to load; see the mesh-rsm-reshape memory). A client on M waits for the
//! response to come back — that delivery is the pass condition.
//!
//! Flow (each SUBMIT authors under the receiving node's key):
//!   1. cs → S: Genesis{members=[S], join_allow=[M], command_allow=[M]}
//!   2. cm → M: JoinRequest            (M ∈ join_allow → admitted, members={S,M})
//!   3. cm → M: Command{1,"list"}      (M ∈ members ∧ command_allow → journalled)
//!   4. cs → S: Response{1, author=M}  (S ∈ members, matches pending cmd → answered)
//!   5. cm receives the Response NOTIFY → decode → assert corr/author/result.

use mesh_testkit::{hex, pubkey, seeded_key, spawn_mesh, wait_for_port, Client};
use std::fs;
use std::time::Duration;

const MESH_DIR: &str = "/home/colin/work/actors/mesh";
const WASM: &str = "target/wasm32-unknown-unknown/release/mesh_control.wasm";
const ADDR_S: &str = "127.0.0.1:9451";
const ADDR_M: &str = "127.0.0.1:9452";
const SEED_S: &str = "rsm-control-sentinel-seed";
const SEED_M: &str = "rsm-control-manager-seed";

// ---- control payloads via control-protocol (Graph-ABI / packr) — one owner, no drift ----
// This harness is the "system" side: it encodes the typed `Msg` it Submits and decodes
// the typed `Msg` it receives, through the SAME schema control-sm now takes as its payload.
use control_protocol::Msg;

fn genesis(members: &[[u8; 32]], join_allow: &[[u8; 32]], command_allow: &[[u8; 32]]) -> Vec<u8> {
    let v = |ks: &[[u8; 32]]| -> Vec<Vec<u8>> { ks.iter().map(|k| k.to_vec()).collect() };
    control_protocol::encode(&Msg::Genesis {
        members: v(members),
        join_allow: v(join_allow),
        command_allow: v(command_allow),
    })
}

fn join_request() -> Vec<u8> {
    control_protocol::encode(&Msg::JoinRequest)
}

fn command(corr_id: u64, verb: &str, args: &[u8]) -> Vec<u8> {
    control_protocol::encode(&Msg::Command { corr_id, verb: verb.to_string(), args: args.to_vec() })
}

fn response(corr_id: u64, cmd_author: &[u8; 32], result: &[u8]) -> Vec<u8> {
    control_protocol::encode(&Msg::Response {
        corr_id,
        cmd_author: cmd_author.to_vec(),
        result: result.to_vec(),
    })
}

/// Decode a Response payload → (corr_id, cmd_author, result). `None` for any other
/// kind, so the receive loop can skip the genesis/join/command NOTIFYs.
fn decode_response(p: &[u8]) -> Option<(u64, [u8; 32], Vec<u8>)> {
    match control_protocol::decode(p)? {
        Msg::Response { corr_id, cmd_author, result } => {
            let author: [u8; 32] = cmd_author.try_into().ok()?;
            Some((corr_id, author, result))
        }
        _ => None,
    }
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
        r#"name = "mesh-control-rt"
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
store_id = "mesh-control-rt"
"#
    );
    fs::write(path, toml).expect("write manifest");
    let _ = fs::create_dir_all(store);
}

fn main() {
    let key_s = seeded_key(SEED_S);
    let key_m = seeded_key(SEED_M);
    let pk_s = pubkey(&key_s);
    let pk_m = pubkey(&key_m);
    println!("sentinel (node S) pubkey: {}", hex(&pk_s));
    println!("manager  (node M) pubkey: {}", hex(&pk_m));

    let _ = fs::remove_dir_all("/tmp/mesh-rt-s-store");
    let _ = fs::remove_dir_all("/tmp/mesh-rt-m-store");
    write_manifest("/tmp/mesh-rt-s.toml", SEED_S, ADDR_S, None, "/tmp/mesh-rt-s-store");
    write_manifest(
        "/tmp/mesh-rt-m.toml",
        SEED_M,
        ADDR_M,
        Some((&hex(&pk_s), ADDR_S)),
        "/tmp/mesh-rt-m-store",
    );

    let mut child_s = spawn_mesh("/tmp/mesh-rt-s.toml", "/tmp/mesh-rt-s.log");
    if !wait_for_port(ADDR_S, Duration::from_secs(5)) {
        eprintln!("node S failed to listen (see /tmp/mesh-rt-s.log)");
        let _ = child_s.kill();
        std::process::exit(1);
    }
    println!("✓ node S listening on {ADDR_S}");

    let mut child_m = spawn_mesh("/tmp/mesh-rt-m.toml", "/tmp/mesh-rt-m.log");
    if !wait_for_port(ADDR_M, Duration::from_secs(5)) {
        eprintln!("node M failed to listen (see /tmp/mesh-rt-m.log)");
        let _ = child_s.kill();
        let _ = child_m.kill();
        std::process::exit(1);
    }
    println!("✓ node M listening on {ADDR_M} (dialing S)");

    // Let M finish dialing S and exchange the (empty) genesis frontier.
    std::thread::sleep(Duration::from_millis(800));

    let result = (|| -> Result<(), String> {
        let mut cs = Client::connect(ADDR_S, &key_s).map_err(|e| format!("connect S: {e}"))?;
        let mut cm = Client::connect(ADDR_M, &key_m).map_err(|e| format!("connect M: {e}"))?;
        println!("✓ clients authenticated to S and M");

        // 1. sentinel authors genesis: members={S}, join/command allow={M}.
        cs.submit(&genesis(&[pk_s], &[pk_m], &[pk_m])).map_err(|e| format!("genesis: {e}"))?;
        println!("✓ S authored genesis");
        std::thread::sleep(Duration::from_millis(600)); // propagate S→M

        // 2. manager joins (gated by join_allow).
        cm.submit(&join_request()).map_err(|e| format!("join: {e}"))?;
        println!("✓ M authored join-request");
        std::thread::sleep(Duration::from_millis(600)); // propagate M→S

        // 3. manager issues a command.
        cm.submit(&command(1, "list", b"")).map_err(|e| format!("command: {e}"))?;
        println!("✓ M authored command(corr=1, list)");
        std::thread::sleep(Duration::from_millis(600)); // propagate M→S so S can answer

        // 4. sentinel answers it. S must already hold the command (via M→S gossip) or
        //    validate() rejects the response for lack of a matching pending command —
        //    so a delivered response also proves bidirectional gossip end-to-end.
        cs.submit(&response(1, &pk_m, b"ok")).map_err(|e| format!("response: {e}"))?;
        println!("✓ S authored response(corr=1) → ok");

        // 5. the manager's client must receive that response as a finalized dag-node.
        //    Skip any genesis/join/command deliveries still queued.
        let mut response_id = None;
        for _ in 0..8 {
            let dn = cm.recv_finalized().map_err(|e| format!("recv on M: {e}"))?;
            if let Some((corr, cmd_author, res)) = decode_response(&dn.payload) {
                if dn.author != pk_s {
                    return Err(format!("response from unexpected author {}", hex(&dn.author)));
                }
                if corr != 1 || cmd_author != pk_m {
                    return Err(format!("response keyed to wrong command: corr={corr}"));
                }
                if res != b"ok" {
                    return Err(format!("wrong result: {:?}", String::from_utf8_lossy(&res)));
                }
                if dn.deps.is_empty() {
                    return Err("finalized dag-node carried no deps".to_string());
                }
                println!("✓ M received finalized response(corr=1) from S: {:?} ({} deps)", String::from_utf8_lossy(&res), dn.deps.len());
                response_id = Some(dn.id);
                break;
            }
        }
        let response_id = response_id.ok_or("manager never received the response after 8 deliveries")?;

        // Interface 2 read verbs, exercised against the just-delivered event.
        let status = cm.event_status(&response_id).map_err(|e| format!("event-status: {e}"))?;
        if status != mesh_testkit::STATUS_FINALIZED {
            return Err(format!("event-status(response) = {status}, want finalized"));
        }
        let unknown = cm.event_status(&[0u8; 32]).map_err(|e| format!("event-status: {e}"))?;
        if unknown != mesh_testkit::STATUS_UNKNOWN {
            return Err(format!("event-status(nonexistent) = {unknown}, want unknown"));
        }
        println!("✓ event-status: response=finalized, unknown-hash=unknown");

        let anc = cm.ancestry(&response_id).map_err(|e| format!("ancestry: {e}"))?;
        if anc.is_empty() {
            return Err("ancestry(response) is empty — should include the whole cycle".to_string());
        }
        let wit = cm.witnesses(&response_id).map_err(|e| format!("witnesses: {e}"))?;
        println!("✓ inspection: ancestry(response)={} events, witnesses={}", anc.len(), wit.len());

        // current-state must have CONVERGED byte-for-byte across both nodes — the
        // clearest proof the finalized fold is identical everywhere.
        std::thread::sleep(Duration::from_millis(300));
        let state_s = cs.current_state().map_err(|e| format!("current-state S: {e}"))?;
        let state_m = cm.current_state().map_err(|e| format!("current-state M: {e}"))?;
        if state_s.is_empty() {
            return Err("current-state is empty".to_string());
        }
        if state_s != state_m {
            return Err("current-state DIVERGED between S and M".to_string());
        }
        println!("✓ current-state converged on S and M ({} bytes, identical)", state_s.len());
        Ok(())
    })();

    let _ = child_s.kill();
    let _ = child_m.kill();
    let _ = child_s.wait();
    let _ = child_m.wait();

    match result {
        Ok(()) => {
            println!("\nCONTROL ROUND-TRIP TEST PASSED");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("\nCONTROL ROUND-TRIP TEST FAILED: {e}");
            std::process::exit(1);
        }
    }
}
