//! `control-protocol` — the control-plane payload schema, as a typed value.
//!
//! The 5 kinds as a `#[derive(GraphValue)]` enum that marshals through the Graph ABI, so
//! the node hands `control-sm` a real `Msg` (typed payload — the node is generic over the
//! payload `p`) and consumers build+encode the same type. `encode`/`decode` are one-liners
//! over the ABI — no hand-rolled cursor. One codec, one owner, zero wire drift.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Msg {
    /// The bootstrap: seed members + allow-lists (valid only as the causal root).
    Genesis { members: Vec<Vec<u8>>, join_allow: Vec<Vec<u8>>, command_allow: Vec<Vec<u8>> },
    JoinRequest,
    Depart,
    Command { corr_id: u64, verb: String, args: Vec<u8> },
    Response { corr_id: u64, cmd_author: Vec<u8>, result: Vec<u8> },
}

/// Encode a control payload via the Graph ABI.
pub fn encode(msg: &Msg) -> Vec<u8> {
    abi_encode(&Value::from(msg.clone())).unwrap_or_default()
}

/// Decode a control payload; `None` on anything not a well-formed `Msg`.
pub fn decode(payload: &[u8]) -> Option<Msg> {
    abi_decode(payload).ok().and_then(|v| Msg::try_from(v).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn round_trips_every_kind() {
        let k = |n: u8| vec![n; 32];
        for m in [
            Msg::Genesis { members: vec![k(1)], join_allow: vec![k(2)], command_allow: vec![k(2)] },
            Msg::JoinRequest,
            Msg::Depart,
            Msg::Command { corr_id: 7, verb: "list".into(), args: vec![1, 2, 3] },
            Msg::Response { corr_id: 7, cmd_author: k(2), result: b"ok".to_vec() },
        ] {
            assert_eq!(decode(&encode(&m)), Some(m));
        }
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[1, 2, 3, 4, 5]), None);
    }
}
