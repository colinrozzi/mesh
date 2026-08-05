//! `bank-protocol` — the currency payload wire.
//!
//! `[version: u16 BE][kind: u8][content]`:
//!   - `Mint { to, amount }`         kind 0 — creates money (always valid).
//!   - `Transfer { from, to, amount }` kind 1 — valid iff `from` holds `amount`.
//!
//! Wallets are named by short strings. Strings are length-prefixed (`u8` len).
//! Codec only — validity + the fold live in `bank-sm`.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

pub const VERSION: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Mint { to: String, amount: u64 },
    Transfer { from: String, to: String, amount: u64 },
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

pub fn encode(cmd: &Cmd) -> Vec<u8> {
    let mut out = VERSION.to_be_bytes().to_vec();
    match cmd {
        Cmd::Mint { to, amount } => {
            out.push(0);
            put_str(&mut out, to);
            out.extend_from_slice(&amount.to_be_bytes());
        }
        Cmd::Transfer { from, to, amount } => {
            out.push(1);
            put_str(&mut out, from);
            put_str(&mut out, to);
            out.extend_from_slice(&amount.to_be_bytes());
        }
    }
    out
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
    fn str(&mut self) -> Option<String> {
        let n = self.take(1)?[0] as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }
    fn u64(&mut self) -> Option<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Some(u64::from_be_bytes(a))
    }
}

pub fn decode(payload: &[u8]) -> Option<Cmd> {
    if payload.len() < 3 || u16::from_be_bytes([payload[0], payload[1]]) != VERSION {
        return None;
    }
    let mut c = Cur { b: payload, p: 3 };
    match payload[2] {
        0 => Some(Cmd::Mint { to: c.str()?, amount: c.u64()? }),
        1 => Some(Cmd::Transfer { from: c.str()?, to: c.str()?, amount: c.u64()? }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn round_trips() {
        let m = Cmd::Mint { to: "alice".to_string(), amount: 100 };
        assert_eq!(decode(&encode(&m)), Some(m));
        let t = Cmd::Transfer { from: "alice".to_string(), to: "bob".to_string(), amount: 30 };
        assert_eq!(decode(&encode(&t)), Some(t));
    }

    #[test]
    fn rejects_bad() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0, 1, 0]), None); // wrong version
        assert_eq!(decode(&[0, 0, 9]), None); // unknown kind
    }
}
