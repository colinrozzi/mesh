//! Minimal ENTRY component standing in for a real consumer (sentinel) in the
//! mesh-client compose smoke.
//!
//! It imports `mesh.submit` and re-exports it as `run` — so composing it with
//! `mesh-client-pkg` (which provides `mesh` and owns the residual
//! `message-server-host.request`) exercises the whole path a real consumer hits:
//! entry → link shim → mesh-client provider → residual host call → back. It also
//! binds two `mesh-control` functions to round-trip a lifecycle envelope.
//!
//! `Result`/`Option` returns bind directly (packr-guest 0.12.1+ decodes
//! `#[import_from]` returns via `FromValue`) — no `TryFrom<Value>` shim.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{export, import_from};

packr_guest::setup_guest!();

packr_guest::pack_types! {
    imports {
        // The consumer must declare the COMPLETE `mesh` interface to match the
        // provider's interface hash — the hash covers every binding, not just the
        // ones called. (This is what the shared mesh.pact guarantees.) Only the
        // functions actually invoked need an `#[import_from]` binding below.
        mesh {
            submit: func(node: string, payload: list<u8>) -> result<list<u8>, string>,
            introduce: func(node: string, member: list<u8>) -> result<list<u8>, string>,
            depart: func(node: string) -> result<list<u8>, string>,
            register: func(node: string, app-id: string) -> result<bool, string>,
            delivery: func(msg: list<u8>) -> option<tuple<list<u8>, list<u8>>>,
            is-ready: func(msg: list<u8>) -> bool,
            node-config: func(seed: string, listen: string, members: list<string>, dial: list<tuple<string, string>>) -> string,
        }
        // Full mesh-control interface declared for the hash; we bind only the two
        // functions the roundtrip export exercises.
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
    exports {
        run: func(node: string, payload: list<u8>) -> result<list<u8>, string>,
        // Encode a lifecycle envelope via the composed mesh-control, decode it
        // back, and report whether it survived the round-trip across the boundary.
        roundtrip-lifecycle: func(event: u8, actor-id: string, ts: u64, data: list<u8>) -> bool,
    }
}

// packr-guest 0.12.1+ decodes `#[import_from]` returns via `FromValue`, so
// `Result`/`Option` returns bind directly — no `TryFrom<Value>` shim.

#[import_from("mesh-control", name = "encode-lifecycle")]
fn enc_lifecycle(event: u8, actor_id: String, ts: u64, data: Vec<u8>) -> Vec<u8>;

#[import_from("mesh-control", name = "decode-lifecycle")]
fn dec_lifecycle(bytes: Vec<u8>) -> Option<(u8, String, u64, Vec<u8>)>;

/// Prove the mesh-control envelope round-trips through the composed component.
#[export(name = "roundtrip-lifecycle")]
fn roundtrip_lifecycle(event: u8, actor_id: String, ts: u64, data: Vec<u8>) -> bool {
    let bytes = enc_lifecycle(event, actor_id.clone(), ts, data.clone());
    match dec_lifecycle(bytes) {
        Some((e, a, t, d)) => e == event && a == actor_id && t == ts && d == data,
        None => false,
    }
}

#[import_from("mesh", name = "submit")]
fn submit(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String>;

/// Forward straight to the composed `mesh.submit`.
#[export]
fn run(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    submit(node, payload)
}
