//! Derive a node's hex pubkey from its `node_seed` — the exact derivation the
//! mesh node uses at init (`sha256(seed)` → ed25519 verifying key). Lets you
//! fill `members` / `join_allow` / `command_allow` without spawning a node or
//! ever handing the seed to anyone else.
//!
//!   cargo run --manifest-path testkit/Cargo.toml --bin keygen -- <node_seed>

use mesh_testkit::{hex, pubkey, seeded_key};

fn main() {
    let seed = match std::env::args().nth(1) {
        Some(s) => s,
        None => {
            eprintln!("usage: keygen <node_seed>");
            std::process::exit(2);
        }
    };
    println!("{}", hex(&pubkey(&seeded_key(&seed))));
}
