//! App-layer CONTROL envelope, carried *inside* a mesh Submit payload.
//!
//! This is a layer ABOVE the substrate: the mesh node never parses it — it just
//! broadcasts the opaque Submit payload and delivers it (`from[32] || body`) to
//! every registered app. Two mesh members (e.g. an orchestrator and a sentinel)
//! use this envelope to run request/response + lifecycle over the broadcast DAG.
//!
//! Correlation + addressing (why no mesh change is needed):
//!   - every delivery already carries `from` = the AUTHENTICATED author pubkey
//!     (the node signs each event), so the responder always knows who asked.
//!   - `corr_id` ties a Response to its Command; it only needs to be unique per
//!     requester (a monotonic counter suffices, since `from` is authenticated).
//!   - `target` is the addressee pubkey. Every member receives every finalized
//!     payload, so each applies two one-line filters: drop `target != me`, and
//!     drop self-delivery (`from == me` — a member receives its OWN Submits).
//!
//! Trust model: the mesh vouches for IDENTITY + ADMISSION (from is authenticated,
//! non-members can't participate); command AUTHORIZATION (which admitted members
//! may drive an app's ops) is app-level config, not membership.
//!
//! Wire: a leading kind byte, then fixed fields, then length-prefixed (u32 BE)
//! variable fields, with the final variable field taking the remaining bytes.

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::PubKey;

// ---- envelope kinds ----
pub const CTRL_COMMAND: u8 = 0x01; // corr_id | target | cmd | args
pub const CTRL_RESPONSE: u8 = 0x02; // corr_id | target(=requester) | result
pub const CTRL_LIFECYCLE: u8 = 0x03; // event | ts | actor_id | data

// ---- lifecycle event bytes (mirrors sentinel's emitters) ----
pub const LIFE_HEARTBEAT: u8 = 0x01;
pub const LIFE_CHILD_CRASH: u8 = 0x02;
pub const LIFE_CHILD_RESPAWN: u8 = 0x03;
pub const LIFE_CHILD_BLOCKED: u8 = 0x04;
pub const LIFE_CHILD_EXTSTOP: u8 = 0x05;

/// A decoded control envelope. Ride these inside a mesh Submit payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// A command addressed to `target`, correlated by `corr_id`. `cmd` is the
    /// op name (e.g. "start"); `args` is its opaque argument blob (e.g. JSON).
    Command {
        corr_id: u64,
        target: PubKey,
        cmd: String,
        args: Vec<u8>,
    },
    /// A response to the command with matching `corr_id`, addressed back to the
    /// original requester (`target` = the command's authenticated `from`).
    /// `result` is the opaque result blob (e.g. the op's JSON response, or an
    /// `{"error":...}` on failure / unauthorized).
    Response {
        corr_id: u64,
        target: PubKey,
        result: Vec<u8>,
    },
    /// A fire-and-forget lifecycle signal, broadcast to every monitor member.
    /// No addressing — filter on `kind == CTRL_LIFECYCLE`.
    Lifecycle {
        event: u8,
        actor_id: String,
        ts: u64,
        data: Vec<u8>,
    },
}

impl Control {
    /// The addressee pubkey for Command/Response (`None` for Lifecycle, which is
    /// broadcast). Use for the `target == me` delivery filter.
    pub fn target(&self) -> Option<&PubKey> {
        match self {
            Control::Command { target, .. } | Control::Response { target, .. } => Some(target),
            Control::Lifecycle { .. } => None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            Control::Command {
                corr_id,
                target,
                cmd,
                args,
            } => {
                let mut v = Vec::with_capacity(1 + 8 + 32 + 4 + cmd.len() + args.len());
                v.push(CTRL_COMMAND);
                v.extend_from_slice(&corr_id.to_be_bytes());
                v.extend_from_slice(target);
                put_bytes(&mut v, cmd.as_bytes()); // length-prefixed (args is the tail)
                v.extend_from_slice(args);
                v
            }
            Control::Response {
                corr_id,
                target,
                result,
            } => {
                let mut v = Vec::with_capacity(1 + 8 + 32 + result.len());
                v.push(CTRL_RESPONSE);
                v.extend_from_slice(&corr_id.to_be_bytes());
                v.extend_from_slice(target);
                v.extend_from_slice(result); // result is the tail
                v
            }
            Control::Lifecycle {
                event,
                actor_id,
                ts,
                data,
            } => {
                let mut v = Vec::with_capacity(1 + 1 + 8 + 4 + actor_id.len() + data.len());
                v.push(CTRL_LIFECYCLE);
                v.push(*event);
                v.extend_from_slice(&ts.to_be_bytes());
                put_bytes(&mut v, actor_id.as_bytes()); // length-prefixed (data is the tail)
                v.extend_from_slice(data);
                v
            }
        }
    }

    /// Decode a control envelope. `None` on any malformed / truncated input.
    pub fn decode(bytes: &[u8]) -> Option<Control> {
        let (kind, rest) = bytes.split_first()?;
        match *kind {
            CTRL_COMMAND => {
                let (corr_id, rest) = take_u64(rest)?;
                let (target, rest) = take_pubkey(rest)?;
                let (cmd, args) = take_bytes(rest)?;
                Some(Control::Command {
                    corr_id,
                    target,
                    cmd: String::from_utf8(cmd).ok()?,
                    args: args.to_vec(),
                })
            }
            CTRL_RESPONSE => {
                let (corr_id, rest) = take_u64(rest)?;
                let (target, rest) = take_pubkey(rest)?;
                Some(Control::Response {
                    corr_id,
                    target,
                    result: rest.to_vec(),
                })
            }
            CTRL_LIFECYCLE => {
                let (event, rest) = rest.split_first()?;
                let (ts, rest) = take_u64(rest)?;
                let (actor_id, data) = take_bytes(rest)?;
                Some(Control::Lifecycle {
                    event: *event,
                    actor_id: String::from_utf8(actor_id).ok()?,
                    ts,
                    data: data.to_vec(),
                })
            }
            _ => None,
        }
    }
}

// ---- length-prefix helpers (u32 BE) ----

fn put_bytes(v: &mut Vec<u8>, b: &[u8]) {
    v.extend_from_slice(&(b.len() as u32).to_be_bytes());
    v.extend_from_slice(b);
}

/// Split off a u32-length-prefixed byte field, returning `(field, remainder)`.
fn take_bytes(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    if bytes.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let rest = &bytes[4..];
    if rest.len() < len {
        return None;
    }
    Some((rest[..len].to_vec(), &rest[len..]))
}

fn take_u64(bytes: &[u8]) -> Option<(u64, &[u8])> {
    if bytes.len() < 8 {
        return None;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[..8]);
    Some((u64::from_be_bytes(b), &bytes[8..]))
}

fn take_pubkey(bytes: &[u8]) -> Option<(PubKey, &[u8])> {
    if bytes.len() < 32 {
        return None;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&bytes[..32]);
    Some((pk, &bytes[32..]))
}

// ---- ergonomic constructors ----

impl Control {
    pub fn command(corr_id: u64, target: PubKey, cmd: &str, args: &[u8]) -> Control {
        Control::Command {
            corr_id,
            target,
            cmd: cmd.to_string(),
            args: args.to_vec(),
        }
    }
    pub fn response(corr_id: u64, target: PubKey, result: &[u8]) -> Control {
        Control::Response {
            corr_id,
            target,
            result: result.to_vec(),
        }
    }
    pub fn lifecycle(event: u8, actor_id: &str, ts: u64, data: &[u8]) -> Control {
        Control::Lifecycle {
            event,
            actor_id: actor_id.to_string(),
            ts,
            data: data.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trips() {
        let c = Control::command(42, [7u8; 32], "start", br#"{"name":"x"}"#);
        assert_eq!(Control::decode(&c.encode()), Some(c));
    }

    #[test]
    fn response_round_trips() {
        let r = Control::response(42, [3u8; 32], br#"{"ok":true}"#);
        assert_eq!(Control::decode(&r.encode()), Some(r));
    }

    #[test]
    fn lifecycle_round_trips() {
        let l = Control::lifecycle(LIFE_CHILD_CRASH, "worker-1", 1_700_000_000, br#"{"reason":"exit"}"#);
        assert_eq!(Control::decode(&l.encode()), Some(l));
    }

    #[test]
    fn empty_args_and_actor_id_round_trip() {
        let c = Control::command(0, [0u8; 32], "list", b"");
        assert_eq!(Control::decode(&c.encode()), Some(c));
        let l = Control::lifecycle(LIFE_HEARTBEAT, "", 0, br#"{"children":0}"#);
        assert_eq!(Control::decode(&l.encode()), Some(l));
    }

    #[test]
    fn target_filter_and_kinds() {
        let me = [9u8; 32];
        let cmd = Control::command(1, me, "health", b"");
        assert_eq!(cmd.target(), Some(&me));
        let life = Control::lifecycle(LIFE_HEARTBEAT, "", 0, b"");
        assert_eq!(life.target(), None); // lifecycle is broadcast, not addressed
    }

    #[test]
    fn malformed_and_unknown_kind_are_none() {
        assert!(Control::decode(&[]).is_none());
        assert!(Control::decode(&[0xFF, 1, 2, 3]).is_none()); // unknown kind
        assert!(Control::decode(&[CTRL_COMMAND, 1, 2]).is_none()); // truncated corr_id
        // truncated length-prefixed cmd (claims 99 bytes, none present)
        let mut bad = alloc::vec![CTRL_COMMAND];
        bad.extend_from_slice(&1u64.to_be_bytes());
        bad.extend_from_slice(&[0u8; 32]);
        bad.extend_from_slice(&99u32.to_be_bytes());
        assert!(Control::decode(&bad).is_none());
    }
}
