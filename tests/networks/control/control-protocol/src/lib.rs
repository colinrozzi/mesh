//! `control-protocol` — the control-plane payload wire, and nothing else.
//!
//! The 5 kinds + their `[version: u16 BE][kind: u8][content]` encoding, shared by
//! `control-sm` (validate/apply over decoded kinds) and the sentinel executors
//! (`sentinel.system` / `sentinelctl.system`, which encode what they Submit and decode
//! `finalized` payloads). One codec, one owner, zero wire drift. No state-machine
//! logic lives here — that is `control-sm`, which imports this.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

/// Wire version; bump on any incompatible codec change.
pub const VERSION: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// The bootstrap: seed members + allow-lists (valid only as the causal root).
    Genesis { members: Vec<Vec<u8>>, join_allow: Vec<Vec<u8>>, command_allow: Vec<Vec<u8>> },
    JoinRequest,
    Depart,
    Command { corr_id: u64, verb: String, args: Vec<u8> },
    Response { corr_id: u64, cmd_author: Vec<u8>, result: Vec<u8> },
}

fn put_keys(out: &mut Vec<u8>, keys: &[Vec<u8>]) {
    out.extend_from_slice(&(keys.len() as u16).to_be_bytes());
    for k in keys {
        out.extend_from_slice(k);
    }
}

/// Build a control payload. Public so a system/harness encodes what it Submits.
pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut out = VERSION.to_be_bytes().to_vec();
    match msg {
        Msg::Genesis { members, join_allow, command_allow } => {
            out.push(0);
            put_keys(&mut out, members);
            put_keys(&mut out, join_allow);
            put_keys(&mut out, command_allow);
        }
        Msg::JoinRequest => out.push(1),
        Msg::Depart => out.push(2),
        Msg::Command { corr_id, verb, args } => {
            out.push(3);
            out.extend_from_slice(&corr_id.to_be_bytes());
            out.extend_from_slice(&(verb.len() as u16).to_be_bytes());
            out.extend_from_slice(verb.as_bytes());
            out.extend_from_slice(args);
        }
        Msg::Response { corr_id, cmd_author, result } => {
            out.push(4);
            out.extend_from_slice(&corr_id.to_be_bytes());
            out.extend_from_slice(cmd_author);
            out.extend_from_slice(result);
        }
    }
    out
}

/// Decode a control payload. `None` on wrong version, unknown kind, or truncation.
pub fn decode(payload: &[u8]) -> Option<Msg> {
    let mut c = Cur { b: payload, p: 0 };
    if c.u16()? != VERSION {
        return None;
    }
    match c.u8()? {
        0 => Some(Msg::Genesis {
            members: c.keys()?,
            join_allow: c.keys()?,
            command_allow: c.keys()?,
        }),
        1 => Some(Msg::JoinRequest),
        2 => Some(Msg::Depart),
        3 => Some(Msg::Command { corr_id: c.u64()?, verb: c.string()?, args: c.rest() }),
        4 => Some(Msg::Response { corr_id: c.u64()?, cmd_author: c.key()?, result: c.rest() }),
        _ => None,
    }
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let e = self.p.checked_add(n)?;
        if e > self.b.len() {
            return None;
        }
        let s = &self.b[self.p..e];
        self.p = e;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u64(&mut self) -> Option<u64> {
        let b = self.take(8)?;
        Some(u64::from_be_bytes(b.try_into().ok()?))
    }
    fn key(&mut self) -> Option<Vec<u8>> {
        Some(self.take(32)?.to_vec())
    }
    fn keys(&mut self) -> Option<Vec<Vec<u8>>> {
        let n = self.u16()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.key()?);
        }
        Some(out)
    }
    fn string(&mut self) -> Option<String> {
        let n = self.u16()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }
    fn rest(&mut self) -> Vec<u8> {
        self.b[self.p..].to_vec()
    }
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
    fn rejects_bad() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0, 1, 0]), None); // wrong version
        assert_eq!(decode(&[0, 0, 9]), None); // unknown kind
    }
}
