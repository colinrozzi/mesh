//! `echo-protocol` — the request/response payload, as a TYPED value.
//!
//! Two kinds as a `#[derive(GraphValue)]` enum: `Request { body }` and
//! `Response { req_id, result }`. `encode`/`decode` marshal through the Graph ABI —
//! no hand-rolled cursor. The correlation id is the Request's own sm-event id (the
//! client learns it as `author`'s returned hash); a `Response` names it. Codec only.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Msg {
    Request { body: Vec<u8> },
    /// `req_id` is the Request's 32-byte sm-event id (a byte list on the wire).
    Response { req_id: Vec<u8>, result: Vec<u8> },
}

/// Encode a message via the Graph ABI.
pub fn encode(msg: &Msg) -> Vec<u8> {
    abi_encode(&Value::from(msg.clone())).unwrap_or_default()
}

/// Decode a payload. `None` on anything that is not a well-formed `Msg`.
pub fn decode(payload: &[u8]) -> Option<Msg> {
    abi_decode(payload).ok().and_then(|v| Msg::try_from(v).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for m in [
            Msg::Request { body: b"ping".to_vec() },
            Msg::Response { req_id: [7u8; 32].to_vec(), result: b"pong".to_vec() },
        ] {
            assert_eq!(decode(&encode(&m)), Some(m));
        }
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[9, 9, 9]), None);
    }
}
