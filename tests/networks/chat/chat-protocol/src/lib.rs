//! `chat-protocol` — the chat network's payload schema, as a typed value.
//!
//! `Msg` is a `#[derive(GraphValue)]` enum that marshals through the Graph ABI, so the
//! node hands `chat-sm` a real `Msg` (typed payload) and the test harnesses build+encode
//! the same type — one owner, no drift. `encode`/`decode` are one-liners over the ABI.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{decode as abi_decode, encode as abi_encode, GraphValue, Value};

/// A chat event. `Genesis` seeds the room's members; `Text` posts; `MemberAdd`/`Remove`
/// mutate the OR-Set membership.
#[derive(Debug, Clone, PartialEq, Eq, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub enum Msg {
    Genesis { members: Vec<Vec<u8>> },
    Text { body: String },
    MemberAdd { subject: Vec<u8> },
    MemberRemove { subject: Vec<u8> },
}

/// Encode a chat message via the Graph ABI.
pub fn encode(msg: &Msg) -> Vec<u8> {
    abi_encode(&Value::from(msg.clone())).unwrap_or_default()
}

/// Decode a payload; `None` on anything not a well-formed `Msg`.
pub fn decode(payload: &[u8]) -> Option<Msg> {
    abi_decode(payload).ok().and_then(|v| Msg::try_from(v).ok())
}
