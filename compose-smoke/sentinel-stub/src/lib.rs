//! Minimal ENTRY component standing in for a real consumer (sentinel) in the
//! mesh-client compose smoke.
//!
//! It imports `mesh.submit` and re-exports it as `run` — so composing it with
//! `mesh-client-pkg` (which provides `mesh` and owns the residual
//! `message-server-host.request`) exercises the whole path a real consumer hits:
//! entry → link shim → mesh-client provider → residual host call → back.
//!
//! Note the [`SubmitReply`] newtype: importing a `Result`-returning function via
//! `#[import_from]` needs the same `TryFrom<Value>` bridge the mesh-client
//! package uses, because the macro decodes returns with `TryFrom<Value>` (not
//! `FromValue`). Every consumer of a `Result`-returning `mesh` function hits
//! this until the macro is fixed upstream — worth surfacing to sentinel-dev.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::composite_abi::{ConversionError, FromValue, Value};
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

/// `TryFrom<Value>` bridge for decode-lifecycle's `option<tuple<...>>` return
/// (same import_from gap as the Result case).
struct LifeOpt(Option<(u8, String, u64, Vec<u8>)>);
impl TryFrom<Value> for LifeOpt {
    type Error = ConversionError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        Ok(LifeOpt(FromValue::from_value(v)?))
    }
}

#[import_from("mesh-control", name = "encode-lifecycle")]
fn enc_lifecycle(event: u8, actor_id: String, ts: u64, data: Vec<u8>) -> Vec<u8>;

#[import_from("mesh-control", name = "decode-lifecycle")]
fn dec_lifecycle_raw(bytes: Vec<u8>) -> LifeOpt;

/// Prove the mesh-control envelope round-trips through the composed component.
#[export(name = "roundtrip-lifecycle")]
fn roundtrip_lifecycle(event: u8, actor_id: String, ts: u64, data: Vec<u8>) -> bool {
    let bytes = enc_lifecycle(event, actor_id.clone(), ts, data.clone());
    match dec_lifecycle_raw(bytes).0 {
        Some((e, a, t, d)) => e == event && a == actor_id && t == ts && d == data,
        None => false,
    }
}

/// `TryFrom<Value>` bridge for the `result<list<u8>, string>` return — see the
/// module note and the mesh-client package's `HostReply`.
struct SubmitReply(Result<Vec<u8>, String>);

impl TryFrom<Value> for SubmitReply {
    type Error = ConversionError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        Ok(SubmitReply(FromValue::from_value(v)?))
    }
}

#[import_from("mesh", name = "submit")]
fn submit_raw(node: String, payload: Vec<u8>) -> SubmitReply;

/// Forward straight to the composed `mesh.submit`.
#[export]
fn run(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    submit_raw(node, payload).0
}
