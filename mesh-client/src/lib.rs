//! `mesh-client` — the client side of the mesh substrate, as a plain Rust library.
//!
//! An actor that wants to talk to a mesh node adds this as a normal Cargo
//! dependency and calls these helpers instead of re-implementing the envelope +
//! message-server protocol. Under packr 0.11.0 (composition retired) there is no
//! fusion or link step — this crate simply compiles into the actor's cdylib.
//!
//! It is **host-agnostic**: the actor declares its own host imports (the
//! `theater:simple/message-server-host.request` binding et al.) and passes the
//! `request` function in. So `mesh-client` never touches the host ABI — it owns
//! only the protocol (the `mesh-api` envelope) on top of whatever transport you
//! give it.
//!
//! ## Usage sketch
//!
//! ```ignore
//! // In your actor, bound to the host:
//! #[import(module = "theater:simple/message-server-host", name = "request")]
//! fn ms_request(actor_id: String, msg: Vec<u8>) -> Result<Vec<u8>, String>;
//!
//! // Spawn your mesh node child with a config from `node_config`, then:
//! let hash = mesh_client::submit(ms_request, &node_id, b"hello")?;
//! mesh_client::register(ms_request, &node_id, &my_actor_id)?;
//!
//! // In handle-send (the delivery callback):
//! if let Some((from, body)) = mesh_client::delivery(&msg) { /* ... */ }
//! ```

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

pub use mesh_api::{Hash, Incoming, PubKey};

/// The shape of the host `message-server-host.request` import: send `msg` to the
/// actor `node` and get its reply. Pass your bound import directly.
pub trait Request: FnOnce(String, Vec<u8>) -> Result<Vec<u8>, String> {}
impl<F: FnOnce(String, Vec<u8>) -> Result<Vec<u8>, String>> Request for F {}

/// Submit a payload to `node`; returns the committed event hash.
pub fn submit<F: Request>(request: F, node: &str, payload: &[u8]) -> Result<Hash, String> {
    command(request, node, mesh_api::encode_submit(payload))
}

/// Ask `node` to admit `member` (author an `Introduce`); returns the event hash.
pub fn introduce<F: Request>(request: F, node: &str, member: &PubKey) -> Result<Hash, String> {
    command(request, node, mesh_api::encode_introduce(member))
}

/// Ask `node` to leave the network (author a `Depart`); returns the event hash.
pub fn depart<F: Request>(request: F, node: &str) -> Result<Hash, String> {
    command(request, node, mesh_api::encode_depart())
}

/// Subscribe this actor (`app_id` = your own actor-id) for committed-payload
/// delivery from `node`. Deliveries then arrive as `handle-send` calls; decode
/// them with [`delivery`].
pub fn register<F: Request>(request: F, node: &str, app_id: &str) -> Result<(), String> {
    // Register acks with an all-zero hash; any Ok reply means we're subscribed.
    request(node.into(), mesh_api::encode_register(app_id)).map(|_| ())
}

/// Decode a message received in `handle-send` into [`Incoming`] — either a
/// `Ready` signal (the node is admitted + synced, act now) or a committed
/// `Delivery{from, body}`. `None` if malformed. Prefer this over [`delivery`].
pub fn incoming(msg: &[u8]) -> Option<Incoming> {
    mesh_api::decode_incoming(msg)
}

/// Decode a delivery received in `handle-send` into `(from, body)` — the
/// committed payload and its author. `None` if the message isn't a delivery
/// (e.g. a `Ready` signal — use [`incoming`] to see those).
pub fn delivery(msg: &[u8]) -> Option<(PubKey, Vec<u8>)> {
    mesh_api::decode_delivery(msg)
}

/// Build a mesh node's `InitConfig` JSON to hand `supervisor.spawn` (as a
/// `Value::String`). `members` are hex pubkeys of the full member set; `dial` is
/// `(pubkey_hex, address)` peers to outbound-connect on start.
pub fn node_config(seed: &str, listen_addr: &str, members: &[&str], dial: &[(&str, &str)]) -> String {
    format!(
        r#"{{"node_seed":"{}","listen_addr":"{}","members":{},"dial":{}}}"#,
        seed,
        listen_addr,
        hex_array(members),
        dial_array(dial),
    )
}

/// Send a command and decode its ack into the committed event hash.
fn command<F: Request>(request: F, node: &str, cmd: Vec<u8>) -> Result<Hash, String> {
    let reply = request(node.into(), cmd)?;
    mesh_api::decode_ack(&reply)
}

fn hex_array(items: &[&str]) -> String {
    let mut s = String::from("[");
    for (i, m) in items.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('"');
        s.push_str(m);
        s.push('"');
    }
    s.push(']');
    s
}

fn dial_array(dial: &[(&str, &str)]) -> String {
    let mut s = String::from("[");
    for (i, (pk, addr)) in dial.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(r#"{{"pubkey":"{}","address":"{}"}}"#, pk, addr));
    }
    s.push(']');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    // A stub `request` that captures the command bytes and returns a canned reply.
    #[test]
    fn submit_encodes_and_decodes_the_ack() {
        let hash = submit(
            |_node, cmd| {
                // node should have received a Submit command carrying the payload.
                assert!(mesh_api::decode_command(&cmd).is_some());
                // reply with an ok-ack for a known hash.
                Ok(mesh_api::encode_ack(true, &[9u8; 32], ""))
            },
            "node-123",
            b"hello",
        )
        .unwrap();
        assert_eq!(hash, [9u8; 32]);
    }

    #[test]
    fn submit_surfaces_a_rejected_ack() {
        let err = submit(|_n, _c| Ok(mesh_api::encode_ack(false, &[0u8; 32], "not a member")), "n", b"x")
            .unwrap_err();
        assert_eq!(err, "not a member");
    }

    #[test]
    fn register_is_ok_on_any_reply() {
        assert!(register(|_n, _c| Ok(mesh_api::encode_ack(true, &[0u8; 32], "")), "n", "app-1").is_ok());
        assert!(register(|_n, _c| Err("down".into()), "n", "app-1").is_err());
    }

    #[test]
    fn delivery_round_trips() {
        let msg = mesh_api::encode_delivery(&[3u8; 32], b"hi");
        let (from, body) = delivery(&msg).unwrap();
        assert_eq!(from, [3u8; 32]);
        assert_eq!(body, b"hi");
    }

    #[test]
    fn node_config_builds_expected_json() {
        let cfg = node_config("seed-a", "127.0.0.1:9550", &["aa", "bb"], &[("aa", "127.0.0.1:9551")]);
        assert_eq!(
            cfg,
            r#"{"node_seed":"seed-a","listen_addr":"127.0.0.1:9550","members":["aa","bb"],"dial":[{"pubkey":"aa","address":"127.0.0.1:9551"}]}"#
        );
    }
}
