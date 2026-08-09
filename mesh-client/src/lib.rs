//! `mesh-client` — the mesh system SDK (v2).
//!
//! An executor is a theater actor that drives its own mesh node over two surfaces:
//! **theater RPC** for actions/questions and the **message-server stream** for live
//! events. Both are quirk-laden (the theater-RPC edges mesh-dev field-reported: returns
//! are double-wrapped in `result`, a validation rejection is carried IN-BAND as
//! `tuple<ok,data>` so it doesn't fault the actor, a no-arg verb still needs a non-empty
//! param, and the finalized dag-node arrives as a framed byte blob). This crate wraps
//! all of it behind a [`Session`], so an executor writes app logic instead of glue.
//!
//! **Transport is injected**, not imported here: the executor declares its own
//! `theater:simple/rpc.call` host binding and hands it to [`Session::new`]. That keeps
//! this a plain `no_std` library (no composition, no import-propagation questions) — the
//! executor owns its actor interface (init / handle-tick / handle-send), the SDK owns
//! the wire.
//!
//! ```ignore
//! #[import(module = "theater:simple/rpc", name = "call")]
//! fn rpc_call(actor_id: String, function: String, params: Value, options: Value) -> Value;
//!
//! let s = Session::new(node_id, rpc_call);
//! s.subscribe(&my_id)?;                 // start the finalized stream
//! let h = s.author(&payload)?;          // take an action
//! let bytes = s.current_state()?;       // ask a question
//! // in handle-send:
//! if let Some(Event::Finalized(node)) = Session::decode_event(&msg) { /* react */ }
//! ```

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::{Value, ValueType};

/// The executor's `theater:simple/rpc.call` binding: `(actor-id, function, params,
/// options) -> value`.
pub type RpcFn = fn(String, String, Value, Value) -> Value;

/// A finalized dag-node from the stream (sm-event envelope + opaque payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagNode {
    pub id: [u8; 32],
    pub author: [u8; 32],
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

/// An event pushed over the message-server stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Finalized(DagNode),
    Conflict { id: [u8; 32], reason: String },
}

/// Drives one mesh node. Cheap to construct per handler call (holds only the node's
/// actor-id and the injected `rpc` binding).
pub struct Session {
    node_id: String,
    rpc: RpcFn,
}

impl Session {
    pub fn new(node_id: String, rpc: RpcFn) -> Self {
        Session { node_id, rpc }
    }

    // ---- actions / questions (RPC) ----

    /// `author(payload) -> hash`. A validation rejection comes back IN-BAND as
    /// `Err(reason)` (the node stays alive), not as a transport error.
    pub fn author(&self, payload: &[u8]) -> Result<[u8; 32], String> {
        let ret = self.call("my:mesh.author", Value::from(payload.to_vec()))?;
        match ret {
            Value::Tuple(items) if items.len() == 2 => {
                let mut it = items.into_iter();
                let ok = matches!(it.next(), Some(Value::Bool(true)));
                let data = Vec::<u8>::try_from(it.next().unwrap())
                    .map_err(|e| format!("author data: {:?}", e))?;
                if ok {
                    hash32(&data)
                } else {
                    Err(String::from_utf8_lossy(&data).into_owned())
                }
            }
            other => Err(format!("author: unexpected ret {:?}", other)),
        }
    }

    /// `current-state() -> bytes` — the folded SM state at the current frontier.
    pub fn current_state(&self) -> Result<Vec<u8>, String> {
        let ret = self.call("my:mesh.current-state", no_arg())?;
        Vec::<u8>::try_from(ret).map_err(|e| format!("current-state: {:?}", e))
    }

    /// `event-status(hash) -> u8` (unknown / pending / finalized / stranded).
    pub fn event_status(&self, id: &[u8; 32]) -> Result<u8, String> {
        let ret = self.call("my:mesh.event-status", Value::from(id.to_vec()))?;
        u8::try_from(ret).map_err(|e| format!("event-status: {:?}", e))
    }

    /// `subscribe(my-actor-id)` — register this executor for the finalized stream.
    pub fn subscribe(&self, my_id: &str) -> Result<(), String> {
        self.call("my:mesh.subscribe", Value::String(my_id.to_string())).map(|_| ())
    }

    /// Call any verb by name and get its `ret` value — the generic escape hatch a caller
    /// uses to reach a network's TYPED interface (e.g. `call("my:counter.increment", n)`),
    /// peeling the two `result` layers (transport wrapper + the node's export result).
    pub fn call(&self, func: &str, params: Value) -> Result<Value, String> {
        let out = (self.rpc)(self.node_id.clone(), func.to_string(), params, none());
        unwrap_result(unwrap_result(out)?)
    }

    // ---- events (stream) ----

    /// Decode a message-server stream frame into an [`Event`]. Pure — call it from the
    /// executor's `handle-send`. Returns `None` for anything that isn't a finalized or
    /// conflict frame.
    pub fn decode_event(msg: &[u8]) -> Option<Event> {
        const FRAME_FINALIZED: u8 = 0x93;
        const FRAME_CONFLICT: u8 = 0x95;
        // frame = [len: u32][kind: u8][body...]
        if msg.len() < 5 {
            return None;
        }
        let kind = msg[4];
        let body = &msg[5..];
        match kind {
            FRAME_FINALIZED => {
                // [id 32][author 32][ts u64][ndeps u16][deps..][payload]
                if body.len() < 74 {
                    return None;
                }
                let id: [u8; 32] = body[0..32].try_into().ok()?;
                let author: [u8; 32] = body[32..64].try_into().ok()?;
                let timestamp = u64::from_be_bytes(body[64..72].try_into().ok()?);
                let ndeps = u16::from_be_bytes([body[72], body[73]]) as usize;
                let payload_start = 74 + ndeps * 32;
                if body.len() < payload_start {
                    return None;
                }
                Some(Event::Finalized(DagNode {
                    id,
                    author,
                    timestamp,
                    payload: body[payload_start..].to_vec(),
                }))
            }
            FRAME_CONFLICT => {
                // [id 32][reason utf8]
                if body.len() < 32 {
                    return None;
                }
                let id: [u8; 32] = body[0..32].try_into().ok()?;
                Some(Event::Conflict { id, reason: String::from_utf8_lossy(&body[32..]).into_owned() })
            }
            _ => None,
        }
    }
}

/// `none` for the RPC call options (default timeout).
fn none() -> Value {
    Value::Option { inner_type: ValueType::Bool, value: None }
}

/// A no-arg verb still needs a non-empty param (an empty tuple does not reach the guest
/// as `tuple<state>`); an empty byte-list flattens cleanly and the node ignores it.
fn no_arg() -> Value {
    Value::from(Vec::<u8>::new())
}

fn hash32(b: &[u8]) -> Result<[u8; 32], String> {
    b.try_into().map_err(|_| format!("expected a 32-byte hash, got {} bytes", b.len()))
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::String(s) => s,
        o => format!("{:?}", o),
    }
}

/// Strip ONE `result` layer — theater encodes it as either `Value::Result` or a
/// `result`-typed `Value::Variant`; a non-result value passes through unchanged.
fn unwrap_result(v: Value) -> Result<Value, String> {
    match v {
        Value::Result { value: Ok(b), .. } => Ok(*b),
        Value::Result { value: Err(b), .. } => Err(value_to_string(*b)),
        Value::Variant { tag: 0, mut payload, .. } if !payload.is_empty() => Ok(payload.remove(0)),
        Value::Variant { tag: 1, payload, .. } => {
            Err(payload.into_iter().next().map(value_to_string).unwrap_or_else(|| "rpc error".to_string()))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_finalized_frame() {
        // build a finalized frame: [len][0x93][id 32][author 32][ts 8][ndeps=0][payload]
        let mut body = Vec::new();
        body.extend_from_slice(&[1u8; 32]); // id
        body.extend_from_slice(&[2u8; 32]); // author
        body.extend_from_slice(&7u64.to_be_bytes()); // ts
        body.extend_from_slice(&0u16.to_be_bytes()); // ndeps
        body.extend_from_slice(b"payload"); // payload
        let mut frame = ((body.len() + 1) as u32).to_be_bytes().to_vec();
        frame.push(0x93);
        frame.extend_from_slice(&body);

        match Session::decode_event(&frame) {
            Some(Event::Finalized(n)) => {
                assert_eq!(n.id, [1u8; 32]);
                assert_eq!(n.author, [2u8; 32]);
                assert_eq!(n.timestamp, 7);
                assert_eq!(n.payload, b"payload");
            }
            other => panic!("expected Finalized, got {:?}", other),
        }
    }

    #[test]
    fn decode_conflict_frame() {
        let mut body = [9u8; 32].to_vec();
        body.extend_from_slice(b"not a member");
        let mut frame = ((body.len() + 1) as u32).to_be_bytes().to_vec();
        frame.push(0x95);
        frame.extend_from_slice(&body);
        match Session::decode_event(&frame) {
            Some(Event::Conflict { id, reason }) => {
                assert_eq!(id, [9u8; 32]);
                assert_eq!(reason, "not a member");
            }
            other => panic!("expected Conflict, got {:?}", other),
        }
    }

    #[test]
    fn ignores_non_stream_frames() {
        assert_eq!(Session::decode_event(&[]), None);
        assert_eq!(Session::decode_event(&[0, 0, 0, 1, 0x10]), None); // some other frame kind
    }
}
