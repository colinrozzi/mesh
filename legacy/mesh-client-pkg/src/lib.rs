//! `mesh-client-pkg` — the mesh client as a **composable packr package**.
//!
//! This is the headline of the composition pilot: instead of every actor
//! vendoring the mesh protocol (or source-depending the [`mesh-client`] library
//! and wiring its own message-server import), an actor pulls the *whole* mesh
//! interface in with one `packr compose` link. The mesh interface is authored
//! once, here, and reused everywhere (sentinel, inbox, …).
//!
//! ## Shape
//!
//! This component **owns the host I/O** — it declares the residual host import
//! `theater:simple/message-server-host.request` and makes the calls to the mesh
//! node itself. Consumers therefore never touch the message-server plumbing;
//! they just call the exported `mesh` functions. That is what makes it an
//! *async service component*: `submit`/`introduce`/`depart`/`register` suspend
//! on the host call, which — because packr composition gives each component its
//! own memory and wasmtime suspends the whole fiber at the host boundary — works
//! transparently through the sync bridging shim (pack M3).
//!
//! ## Composition
//!
//! Built as a plain cdylib (`--export-memory --no-entry`). The consumer is the
//! `entry`; this package is a non-entry provider. Example manifest:
//!
//! ```toml
//! [[component]]
//! name = "sentinel"
//! wasm = "sentinel.wasm"
//! entry = true
//!
//! [[component]]
//! name = "mesh-client"
//! wasm = "mesh_client_pkg.wasm"
//!
//! # sentinel's `mesh.submit` import is satisfied by this package's `submit`.
//! [[link]]
//! consumer = "sentinel"
//! import   = "mesh.submit"
//! provider = "mesh-client"
//! export   = "submit"
//! # …one [[link]] per mesh function the consumer uses.
//! ```
//!
//! `theater:simple/message-server-host.request` is intentionally left unlinked —
//! it survives as a residual import that the theater host supplies at
//! instantiate. Interface hashes are embedded by `pack_types!`, so a version-
//! skewed link (consumer built against a different `mesh` signature) is rejected
//! at compose time rather than failing at runtime.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mesh_api::control::{Control, CTRL_COMMAND, CTRL_LIFECYCLE, CTRL_RESPONSE};
use packr_guest::{export, import_from};

packr_guest::setup_guest!();

packr_guest::pack_types! {
    imports {
        // The one host call this component owns. Left residual at compose time;
        // theater provides it at instantiate. Async — the suspension point.
        theater:simple/message-server-host {
            request: func(actor-id: string, msg: list<u8>) -> result<list<u8>, string>,
        }
    }
    exports {
        // The mesh interface — the TRANSPORT. Any actor requires these and is
        // composed with this package. hash / pubkey are list<u8> (32) across the
        // boundary.
        mesh {
            submit: func(node: string, payload: list<u8>) -> result<list<u8>, string>,
            introduce: func(node: string, member: list<u8>) -> result<list<u8>, string>,
            depart: func(node: string) -> result<list<u8>, string>,
            register: func(node: string, app-id: string) -> result<bool, string>,
            delivery: func(msg: list<u8>) -> option<tuple<list<u8>, list<u8>>>,
            is-ready: func(msg: list<u8>) -> bool,
            node-config: func(seed: string, listen: string, members: list<string>, dial: list<tuple<string, string>>) -> string,
        }
        // The mesh-control interface — the app-level CONTROL envelope carried
        // INSIDE a Submit payload (the node never parses it). Opt-in: transport
        // consumers ignore it; control consumers (orchestrator / sentinel) link
        // it too. encode-* build a payload to submit; control-kind + decode-*
        // parse a delivered payload. Same codec as mesh-api's Control.
        // target is a 32-byte pubkey as list<u8>; encode-* return empty on a
        // bad-length target.
        mesh-control {
            encode-command: func(corr-id: u64, target: list<u8>, cmd: string, args: list<u8>) -> list<u8>,
            encode-response: func(corr-id: u64, target: list<u8>, result: list<u8>) -> list<u8>,
            encode-lifecycle: func(event: u8, actor-id: string, ts: u64, data: list<u8>) -> list<u8>,
            control-kind: func(bytes: list<u8>) -> option<u8>,
            decode-command: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, string, list<u8>>>,
            decode-response: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, list<u8>>>,
            decode-lifecycle: func(bytes: list<u8>) -> option<tuple<u8, string, u64, list<u8>>>,
        }
    }
}

/// The residual host import — send `msg` to actor `node`, get its reply. This is
/// the async boundary the exported functions suspend on. Since packr-guest 0.12.1
/// `#[import_from]` decodes returns via `FromValue`, so the `Result` return binds
/// directly (no newtype shim needed).
#[import_from("theater:simple/message-server-host", name = "request")]
fn request(actor_id: String, msg: Vec<u8>) -> Result<Vec<u8>, String>;

/// Submit a payload to `node`; returns the committed event hash (32 bytes).
#[export]
fn submit(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    mesh_client::submit(request, &node, &payload).map(|h| h.to_vec())
}

/// Ask `node` to admit `member` (a 32-byte pubkey); returns the event hash.
#[export]
fn introduce(node: String, member: Vec<u8>) -> Result<Vec<u8>, String> {
    let member = to_key(&member)?;
    mesh_client::introduce(request, &node, &member).map(|h| h.to_vec())
}

/// Ask `node` to leave the network; returns the event hash.
#[export]
fn depart(node: String) -> Result<Vec<u8>, String> {
    mesh_client::depart(request, &node).map(|h| h.to_vec())
}

/// Subscribe this actor (`app_id` = its own actor-id) for committed-payload
/// delivery from `node`. Deliveries then arrive as `handle-send` messages.
/// Returns `true` on success (the ack carries no data — any Ok reply subscribes).
#[export]
fn register(node: String, app_id: String) -> Result<bool, String> {
    mesh_client::register(request, &node, &app_id).map(|()| true)
}

/// Decode a delivery received in `handle-send` into `(from, body)` — the
/// committed payload and its 32-byte author. Pure; no host call. `None` if the
/// message isn't a delivery.
#[export]
fn delivery(msg: Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)> {
    mesh_client::delivery(&msg).map(|(from, body)| (from.to_vec(), body))
}

/// Whether a `handle-send` message is the one-shot Ready signal (node admitted +
/// synced). Pair with `delivery` to route: `is-ready` first, else `delivery`.
#[export(name = "is-ready")]
fn is_ready(msg: Vec<u8>) -> bool {
    matches!(mesh_client::incoming(&msg), Some(mesh_client::Incoming::Ready))
}

/// Build a mesh node's `InitConfig` JSON to hand `supervisor.spawn`. `members`
/// are hex pubkeys of the full member set; `dial` is `(pubkey_hex, address)`
/// peers to outbound-connect on start. Pure; no host call.
#[export(name = "node-config")]
fn node_config(
    seed: String,
    listen: String,
    members: Vec<String>,
    dial: Vec<(String, String)>,
) -> String {
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    let dial: Vec<(&str, &str)> =
        dial.iter().map(|(pk, addr)| (pk.as_str(), addr.as_str())).collect();
    mesh_client::node_config(&seed, &listen, &members, &dial)
}

// ---- mesh-control: the app-level control envelope (see the interface above) ----

/// Build a Command payload addressed to `target`. Empty on a bad-length target.
#[export(name = "encode-command")]
fn encode_command(corr_id: u64, target: Vec<u8>, cmd: String, args: Vec<u8>) -> Vec<u8> {
    match to_key(&target) {
        Ok(target) => Control::Command { corr_id, target, cmd, args }.encode(),
        Err(_) => Vec::new(),
    }
}

/// Build a Response payload addressed back to `target` (the requester). Empty on
/// a bad-length target.
#[export(name = "encode-response")]
fn encode_response(corr_id: u64, target: Vec<u8>, result: Vec<u8>) -> Vec<u8> {
    match to_key(&target) {
        Ok(target) => Control::Response { corr_id, target, result }.encode(),
        Err(_) => Vec::new(),
    }
}

/// Build a Lifecycle payload (broadcast; no addressee).
#[export(name = "encode-lifecycle")]
fn encode_lifecycle(event: u8, actor_id: String, ts: u64, data: Vec<u8>) -> Vec<u8> {
    Control::Lifecycle { event, actor_id, ts, data }.encode()
}

/// Peek a control payload's kind (1=command, 2=response, 3=lifecycle). `Some(k)`
/// guarantees the matching `decode-*` succeeds; `None` if it isn't a valid
/// control envelope.
#[export(name = "control-kind")]
fn control_kind(bytes: Vec<u8>) -> Option<u8> {
    match Control::decode(&bytes)? {
        Control::Command { .. } => Some(CTRL_COMMAND),
        Control::Response { .. } => Some(CTRL_RESPONSE),
        Control::Lifecycle { .. } => Some(CTRL_LIFECYCLE),
    }
}

/// Decode a Command payload into (corr-id, target, cmd, args).
#[export(name = "decode-command")]
fn decode_command(bytes: Vec<u8>) -> Option<(u64, Vec<u8>, String, Vec<u8>)> {
    match Control::decode(&bytes)? {
        Control::Command { corr_id, target, cmd, args } => Some((corr_id, target.to_vec(), cmd, args)),
        _ => None,
    }
}

/// Decode a Response payload into (corr-id, target, result).
#[export(name = "decode-response")]
fn decode_response(bytes: Vec<u8>) -> Option<(u64, Vec<u8>, Vec<u8>)> {
    match Control::decode(&bytes)? {
        Control::Response { corr_id, target, result } => Some((corr_id, target.to_vec(), result)),
        _ => None,
    }
}

/// Decode a Lifecycle payload into (event, actor-id, ts, data).
#[export(name = "decode-lifecycle")]
fn decode_lifecycle(bytes: Vec<u8>) -> Option<(u8, String, u64, Vec<u8>)> {
    match Control::decode(&bytes)? {
        Control::Lifecycle { event, actor_id, ts, data } => Some((event, actor_id, ts, data)),
        _ => None,
    }
}

/// Convert a boundary `list<u8>` into a fixed 32-byte key, rejecting the wrong
/// length with a clear error rather than panicking on a bad `try_into`.
fn to_key(bytes: &[u8]) -> Result<mesh_client::PubKey, String> {
    bytes
        .try_into()
        .map_err(|_| alloc::format!("expected a 32-byte key, got {} bytes", bytes.len()))
}
