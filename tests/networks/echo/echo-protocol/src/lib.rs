//! `echo-protocol` — the request/response payload wire for the tier-2 reference.
//!
//! Two kinds, `[version: u16 BE][kind: u8][content]`:
//!   - `Request { body }`            kind 0, content = body bytes.
//!   - `Response { req_id, result }` kind 1, content = `[req_id: 32][result]`.
//!
//! The correlation id is the Request's own **sm-event id** (the substrate assigns it;
//! the client learns it as `author`'s returned hash). A `Response` names that id, so
//! the SM can require the Request be in the Response's ancestry and the client can
//! match the reply. Codec only — validity/fold live in `echo-sm`.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::vec::Vec;

pub const VERSION: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    Request { body: Vec<u8> },
    Response { req_id: [u8; 32], result: Vec<u8> },
}

pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut out = VERSION.to_be_bytes().to_vec();
    match msg {
        Msg::Request { body } => {
            out.push(0);
            out.extend_from_slice(body);
        }
        Msg::Response { req_id, result } => {
            out.push(1);
            out.extend_from_slice(req_id);
            out.extend_from_slice(result);
        }
    }
    out
}

pub fn decode(payload: &[u8]) -> Option<Msg> {
    if payload.len() < 3 || u16::from_be_bytes([payload[0], payload[1]]) != VERSION {
        return None;
    }
    let body = &payload[3..];
    match payload[2] {
        0 => Some(Msg::Request { body: body.to_vec() }),
        1 => {
            let req_id: [u8; 32] = body.get(0..32)?.try_into().ok()?;
            Some(Msg::Response { req_id, result: body[32..].to_vec() })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let r = Msg::Request { body: b"ping".to_vec() };
        assert_eq!(decode(&encode(&r)), Some(r));
        let resp = Msg::Response { req_id: [7u8; 32], result: b"pong".to_vec() };
        assert_eq!(decode(&encode(&resp)), Some(resp));
    }

    #[test]
    fn rejects_bad() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0, 1, 0]), None); // wrong version
        assert_eq!(decode(&[0, 0, 9]), None); // unknown kind
        assert_eq!(decode(&[0, 0, 1, 1, 2]), None); // Response req_id truncated
    }
}
