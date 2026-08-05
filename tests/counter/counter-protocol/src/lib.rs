//! `counter-protocol` — the counter SM's payload wire, and NOTHING else.
//!
//! The reference RSM stack is four layers with one owner each: **executor** (app
//! logic) / **protocol crate** (this — the payload codec) / **SM** (validate+apply
//! over decoded kinds) / **node** (the dumb core). This crate is the codec layer: it
//! defines the payload `Cmd` kinds and their `[version][kind][content]` encoding, so
//! `counter-sm` and the counter executor share ONE encode/decode and can never drift
//! (the wire-mismatch bug class). It holds no state-machine logic — validity and the
//! fold live in `counter-sm`, which imports this.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::vec::Vec;

/// Wire version; bump on any incompatible codec change.
pub const VERSION: u16 = 0;

/// A counter command. `Inc` moves the count by a signed delta; `Reset` zeroes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Inc(i64),
    Reset,
}

/// Encode a command as `[version: u16 BE][kind: u8][content]`.
/// kind 0 = Inc (content = i64 BE), kind 1 = Reset (no content).
pub fn encode(cmd: &Cmd) -> Vec<u8> {
    let mut out = VERSION.to_be_bytes().to_vec();
    match cmd {
        Cmd::Inc(n) => {
            out.push(0);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Cmd::Reset => out.push(1),
    }
    out
}

/// Decode a payload. Returns `None` on wrong version, unknown kind, or truncation.
pub fn decode(payload: &[u8]) -> Option<Cmd> {
    if payload.len() < 3 {
        return None;
    }
    if u16::from_be_bytes([payload[0], payload[1]]) != VERSION {
        return None;
    }
    match payload[2] {
        0 => {
            let c = payload.get(3..11)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(c);
            Some(Cmd::Inc(i64::from_be_bytes(b)))
        }
        1 => Some(Cmd::Reset),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for cmd in [Cmd::Inc(1), Cmd::Inc(-7), Cmd::Inc(1_000_000), Cmd::Reset] {
            assert_eq!(decode(&encode(&cmd)), Some(cmd));
        }
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0, 0]), None); // truncated
        assert_eq!(decode(&[0, 1, 0]), None); // wrong version
        assert_eq!(decode(&[0, 0, 9]), None); // unknown kind
        assert_eq!(decode(&[0, 0, 0, 1, 2, 3]), None); // Inc truncated
    }
}
