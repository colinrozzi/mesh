//! mesh smoke test — single node.
//!
//! A client authenticates as the member node, SUBMITs a payload, and observes it
//! delivered (NOTIFY) after it finalizes — immediate, since a single-member
//! network finalizes its own events at once.

use mesh_testkit::{hex, pubkey, seeded_key, Client};

const ADDR: &str = "127.0.0.1:9447";

fn main() {
    // The mesh derives its key from node_seed = "mesh-smoke-v1:root" (manifest),
    // so the member key is sha256 of exactly that string.
    let node = seeded_key("mesh-smoke-v1:root");
    let node_pk = pubkey(&node);

    if std::env::args().any(|a| a == "--pubkeys") {
        println!("node (member) = {}", hex(&node_pk));
        return;
    }

    let mut client = Client::connect(ADDR, &node).expect("handshake");
    println!("✓ authenticated as member node");

    let payload = b"hello, this is a message via mesh";
    let event = client.submit(payload).expect("submit");
    println!("✓ payload submitted and ACKed (event {}…)", &hex(&event)[..16]);

    let (from, got) = client.recv_message().expect("delivery");
    assert_eq!(from, node_pk, "message should be from the authoring node");
    assert_eq!(got, payload, "delivered payload mismatch");
    println!("✓ received committed delivery: {:?}", String::from_utf8_lossy(&got));

    println!("\nALL CHECKS PASSED");
}
