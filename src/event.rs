//! Event types + signing + verification + canonical encoding (v2).
//!
//! Every event references a single `parent` event hash. State is purely
//! derived by walking the DAG from genesis. There is no `parent_state_hash`
//! in events — see DESIGN.md.
//!
//! Wire format (hand-rolled, deterministic):
//!
//!   parent:     32 bytes (event hash; all-zero = genesis sentinel)
//!   author:     32 bytes pubkey
//!   op_kind:    1 byte
//!   op_payload: per-kind variable
//!   signature:  64 bytes ed25519 over sha256(parent || author || op_kind || op_payload)
//!
//! Op kinds:
//!   0 = NodeIntroduce
//!   1 = MailboxCreate
//!   2 = Revoke
//!   3 = Send
//!   4 = Witness
//!
//! Op encodings:
//!   NodeIntroduce: subject[32] + u16(name.len) + name
//!   MailboxCreate: subject[32] + u16(name.len) + name
//!   Revoke:        subject[32]
//!   Send:          recipient[32] + u32(payload.len) + payload
//!   Witness:       u16(also_cite.len) + also_cite[N * 32]

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];
pub type PubKey = [u8; 32];
pub type Sig = [u8; 64];

/// The genesis "parent" sentinel — all zeros. The very first real event
/// in a network uses this as its `parent`. Only the root Node may use
/// the genesis parent.
pub const GENESIS_PARENT: Hash = [0u8; 32];

#[derive(Clone, Debug)]
pub enum Op {
    NodeIntroduce { subject: PubKey, name: String },
    MailboxCreate { subject: PubKey, name: String },
    Revoke { subject: PubKey },
    Send { recipient: PubKey, payload: Vec<u8> },
    Witness { also_cite: Vec<Hash> },
}

impl Op {
    pub fn kind_byte(&self) -> u8 {
        match self {
            Op::NodeIntroduce { .. } => 0,
            Op::MailboxCreate { .. } => 1,
            Op::Revoke { .. } => 2,
            Op::Send { .. } => 3,
            Op::Witness { .. } => 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub parent: Hash,
    pub author: PubKey,
    pub op: Op,
    pub signature: Sig,
}

impl Event {
    /// Compute the hash that gets signed (parent || author || op_kind || op_payload).
    pub fn signing_hash(parent: &Hash, author: &PubKey, op: &Op) -> Hash {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(author);
        hasher.update([op.kind_byte()]);
        let mut payload_buf = Vec::new();
        encode_op_payload(op, &mut payload_buf);
        hasher.update(&payload_buf);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result);
        out
    }

    /// Whole-event hash — referenced as `parent` by descendants and cited
    /// by Witnesses. SHA-256 over the canonical encoding (including sig).
    pub fn event_hash(&self) -> Hash {
        let bytes = self.encode();
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result);
        out
    }

    pub fn verify_signature(&self) -> Result<(), String> {
        let hash = Self::signing_hash(&self.parent, &self.author, &self.op);
        let vk = VerifyingKey::from_bytes(&self.author)
            .map_err(|e| format!("bad pubkey: {}", e))?;
        let sig = Signature::from_bytes(&self.signature);
        vk.verify(&hash, &sig)
            .map_err(|e| format!("signature: {}", e))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(160);
        out.extend_from_slice(&self.parent);
        out.extend_from_slice(&self.author);
        out.push(self.op.kind_byte());
        encode_op_payload(&self.op, &mut out);
        out.extend_from_slice(&self.signature);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let mut cur = Cursor::new(buf);
        let parent = cur.take_array::<32>()?;
        let author = cur.take_array::<32>()?;
        let kind = cur.take(1)?[0];
        let op = decode_op(kind, &mut cur)?;
        let signature = cur.take_array::<64>()?;
        Ok(Event { parent, author, op, signature })
    }
}

/// A forward-only reader over a byte slice with centralized bounds-checking,
/// so the hand-rolled decoders below can't run off the end of the buffer.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.pos.checked_add(n).ok_or("length overflow")?;
        if end > self.buf.len() {
            return Err(format!(
                "unexpected end of input: need {} bytes at offset {}, have {}",
                n,
                self.pos,
                self.buf.len() - self.pos
            ));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn take_u16(&mut self) -> Result<usize, String> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]) as usize)
    }

    fn take_u32(&mut self) -> Result<usize, String> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }
}

fn encode_op_payload(op: &Op, out: &mut Vec<u8>) {
    match op {
        Op::NodeIntroduce { subject, name } | Op::MailboxCreate { subject, name } => {
            out.extend_from_slice(subject);
            out.extend_from_slice(&(name.len() as u16).to_be_bytes());
            out.extend_from_slice(name.as_bytes());
        }
        Op::Revoke { subject } => {
            out.extend_from_slice(subject);
        }
        Op::Send { recipient, payload } => {
            out.extend_from_slice(recipient);
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(payload);
        }
        Op::Witness { also_cite } => {
            out.extend_from_slice(&(also_cite.len() as u16).to_be_bytes());
            for h in also_cite {
                out.extend_from_slice(h);
            }
        }
    }
}

fn decode_op(kind: u8, cur: &mut Cursor) -> Result<Op, String> {
    match kind {
        // 0 = NodeIntroduce, 1 = MailboxCreate — same payload shape.
        0 | 1 => {
            let subject = cur.take_array::<32>()?;
            let name_len = cur.take_u16()?;
            let name = core::str::from_utf8(cur.take(name_len)?)
                .map_err(|_| "name not utf-8".to_string())?
                .to_string();
            Ok(if kind == 0 {
                Op::NodeIntroduce { subject, name }
            } else {
                Op::MailboxCreate { subject, name }
            })
        }
        2 => Ok(Op::Revoke {
            subject: cur.take_array::<32>()?,
        }),
        3 => {
            let recipient = cur.take_array::<32>()?;
            let payload_len = cur.take_u32()?;
            let payload = cur.take(payload_len)?.to_vec();
            Ok(Op::Send { recipient, payload })
        }
        4 => {
            let count = cur.take_u16()?;
            let mut also_cite = Vec::with_capacity(count);
            for _ in 0..count {
                also_cite.push(cur.take_array::<32>()?);
            }
            Ok(Op::Witness { also_cite })
        }
        _ => Err(format!("unknown op kind: {}", kind)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn signed(sk: &SigningKey, parent: Hash, op: Op) -> Event {
        let author = sk.verifying_key().to_bytes();
        let signing_hash = Event::signing_hash(&parent, &author, &op);
        let signature = sk.sign(&signing_hash).to_bytes();
        Event { parent, author, op, signature }
    }

    fn sample_ops() -> Vec<Op> {
        alloc::vec![
            Op::NodeIntroduce { subject: [2u8; 32], name: "node-b".to_string() },
            Op::MailboxCreate { subject: [3u8; 32], name: "alice".to_string() },
            Op::MailboxCreate { subject: [3u8; 32], name: String::new() }, // empty name
            Op::Revoke { subject: [3u8; 32] },
            Op::Send { recipient: [3u8; 32], payload: b"hello bob".to_vec() },
            Op::Send { recipient: [3u8; 32], payload: Vec::new() }, // empty payload
            Op::Witness { also_cite: alloc::vec![[4u8; 32], [5u8; 32]] },
            Op::Witness { also_cite: Vec::new() }, // no citations
        ]
    }

    #[test]
    fn round_trips_and_verifies_every_op() {
        let sk = key(1);
        for op in sample_ops() {
            let kind = op.kind_byte();
            let ev = signed(&sk, [9u8; 32], op);
            let bytes = ev.encode();
            let decoded = Event::decode(&bytes).expect("decode");
            // Re-encoding the decoded event must reproduce the exact bytes.
            assert_eq!(decoded.encode(), bytes, "re-encode mismatch for kind {}", kind);
            decoded.verify_signature().expect("signature verifies");
        }
    }

    #[test]
    fn decode_rejects_every_truncation() {
        let sk = key(1);
        let ev = signed(&sk, [0u8; 32], Op::Send { recipient: [3u8; 32], payload: b"abc".to_vec() });
        let bytes = ev.encode();
        for n in 0..bytes.len() {
            assert!(Event::decode(&bytes[..n]).is_err(), "prefix of len {} should fail", n);
        }
        assert!(Event::decode(&bytes).is_ok(), "full bytes should decode");
    }

    #[test]
    fn decode_rejects_unknown_op_kind() {
        let sk = key(1);
        let bytes = signed(&sk, [0u8; 32], Op::Revoke { subject: [3u8; 32] }).encode();
        let mut bad = bytes.clone();
        bad[64] = 0xff; // op_kind byte sits right after parent(32)+author(32)
        assert!(Event::decode(&bad).is_err());
    }

    #[test]
    fn tampered_signature_fails_verification() {
        let sk = key(1);
        let mut ev = signed(&sk, [0u8; 32], Op::Revoke { subject: [3u8; 32] });
        ev.signature[0] ^= 0xff;
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn wrong_author_fails_verification() {
        let sk = key(1);
        let mut ev = signed(&sk, [0u8; 32], Op::Revoke { subject: [3u8; 32] });
        ev.author = key(2).verifying_key().to_bytes();
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn event_hash_is_stable_and_distinct() {
        let sk = key(1);
        let a = signed(&sk, [0u8; 32], Op::Revoke { subject: [3u8; 32] });
        let b = signed(&sk, [0u8; 32], Op::Revoke { subject: [4u8; 32] });
        assert_eq!(a.event_hash(), a.event_hash());
        assert_ne!(a.event_hash(), b.event_hash());
    }
}
