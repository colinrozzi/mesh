//! `echo-sm` — a request/response journal as an RSM `state-machine` component.
//!
//! The SM layer of the tier-2 reference stack. It is control-sm boiled down to the
//! bare request/response pattern (no membership, no authz): a `Request` is always
//! admissible; a `Response{req_id}` is valid iff `req_id` names a `Request` in the
//! Response's causal past (**ancestry-relative** — the server can only answer a
//! request it has actually seen). Pure, structure-blind, and conflict-free under a
//! single responder (one Response per request), so admission-final. Imports
//! `echo-protocol` for the codec.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use echo_protocol::{decode, Msg};
use packr_guest::export;
use serde::{Deserialize, Serialize};

#[cfg(not(test))]
packr_guest::setup_guest!();

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct EchoState {
    /// (request event id, body).
    requests: Vec<(Vec<u8>, Vec<u8>)>,
    /// (req_id, result).
    responses: Vec<(Vec<u8>, Vec<u8>)>,
}

impl EchoState {
    fn decode(bytes: &[u8]) -> EchoState {
        if bytes.is_empty() {
            return EchoState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn has_request(&self, id: &[u8]) -> bool {
        self.requests.iter().any(|(k, _)| k == id)
    }
}

fn do_validate(payload: &[u8], state: &[u8]) -> Result<bool, String> {
    if payload.is_empty() {
        return Ok(true); // inert graft
    }
    let s = EchoState::decode(state);
    match decode(payload).ok_or_else(|| "undecodable echo payload".to_string())? {
        Msg::Request { .. } => Ok(true),
        Msg::Response { req_id, .. } => {
            if s.has_request(&req_id) {
                Ok(true)
            } else {
                Err("response names an unknown request".to_string())
            }
        }
    }
}

fn do_apply(id: &[u8], payload: &[u8], state: &[u8]) -> Vec<u8> {
    let mut s = EchoState::decode(state);
    match decode(payload) {
        Some(Msg::Request { body }) => s.requests.push((id.to_vec(), body)),
        // First response per request wins (single responder → conflict-free).
        Some(Msg::Response { req_id, result })
            if !s.responses.iter().any(|(k, _)| k.as_slice() == req_id) =>
        {
            s.responses.push((req_id.to_vec(), result));
        }
        _ => {}
    }
    s.encode()
}

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    EchoState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&payload, &state)
}

#[export]
fn apply(id: Vec<u8>, _author: Vec<u8>, _timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8> {
    do_apply(&id, &payload, &state)
}

#[export]
fn members(_state: Vec<u8>) -> Vec<Vec<u8>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_protocol::encode;

    #[test]
    fn request_then_response_round_trip() {
        let mut s = EchoState::default().encode();
        // a request with event id [1;32]
        let req = encode(&Msg::Request { body: b"ping".to_vec() });
        assert!(do_validate(&req, &s).is_ok());
        s = do_apply(&[1u8; 32], &req, &s);
        // a response naming that request is valid; naming an unknown one is not
        let resp = encode(&Msg::Response { req_id: [1u8; 32], result: b"pong".to_vec() });
        assert!(do_validate(&resp, &s).is_ok());
        let bad = encode(&Msg::Response { req_id: [9u8; 32], result: b"x".to_vec() });
        assert!(do_validate(&bad, &s).is_err(), "unknown request");
        s = do_apply(&[2u8; 32], &resp, &s);
        let st = EchoState::decode(&s);
        assert_eq!(st.responses.len(), 1);
        assert_eq!(st.responses[0].1, b"pong");
    }

    #[test]
    fn response_before_request_is_rejected() {
        // ancestry-relative: without the request in state, the response is invalid.
        let s = EchoState::default().encode();
        let resp = encode(&Msg::Response { req_id: [1u8; 32], result: b"pong".to_vec() });
        assert!(do_validate(&resp, &s).is_err());
    }
}
