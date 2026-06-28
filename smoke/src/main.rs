//! mesh v3 smoke test — single node, message-passing reducer.
//!
//! A client authenticates as the member node, SUBMITs a message addressed to
//! bob, and observes it delivered (NOTIFY) after it finalizes — immediate, since
//! a single-member network finalizes its own events at once.

use mesh_testkit::{hex, pubkey, seeded_key, Client};

const ADDR: &str = "127.0.0.1:9447";

fn main() {
    // The mesh derives its key from node_seed = "mesh-smoke-v1:root" (manifest),
    // so the member key is sha256 of exactly that string.
    let node = seeded_key("mesh-smoke-v1:root");
    let bob = seeded_key("mesh-smoke-v1:bob");
    let node_pk = pubkey(&node);
    let bob_pk = pubkey(&bob);

    if std::env::args().any(|a| a == "--pubkeys") {
        println!("node (member)   = {}", hex(&node_pk));
        println!("bob (recipient) = {}", hex(&bob_pk));
        return;
    }

    let mut client = Client::connect(ADDR, &node).expect("handshake");
    println!("✓ authenticated as member node");

    let body = b"hello bob, this is a message via mesh v3";
    let event = client.submit(&bob_pk, body).expect("submit");
    println!("✓ message submitted and ACKed (event {}…)", &hex(&event)[..16]);

    let (from, got) = client.recv_message().expect("delivery");
    assert_eq!(from, node_pk, "message should be from the authoring node");
    assert_eq!(got, body, "delivered body mismatch");
    println!("✓ received committed delivery: {:?}", String::from_utf8_lossy(&got));

    println!("\nALL CHECKS PASSED");
}
