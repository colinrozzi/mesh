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
        if buf.len() < 32 + 32 + 1 + 64 {
            return Err("event too short".to_string());
        }
        let mut pos = 0;
        let mut parent = [0u8; 32];
        parent.copy_from_slice(&buf[pos..pos + 32]);
        pos += 32;
        let mut author = [0u8; 32];
        author.copy_from_slice(&buf[pos..pos + 32]);
        pos += 32;
        let kind = buf[pos];
        pos += 1;
        let op = decode_op_payload(kind, buf, &mut pos)?;
        if buf.len() < pos + 64 {
            return Err("event missing signature".to_string());
        }
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&buf[pos..pos + 64]);
        Ok(Event { parent, author, op, signature })
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

fn decode_op_payload(kind: u8, buf: &[u8], pos: &mut usize) -> Result<Op, String> {
    match kind {
        0 | 1 => {
            // NodeIntroduce | MailboxCreate — same payload shape
            if buf.len() < *pos + 32 + 2 {
                return Err("introduce/create truncated header".to_string());
            }
            let mut subject = [0u8; 32];
            subject.copy_from_slice(&buf[*pos..*pos + 32]);
            *pos += 32;
            let name_len = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]) as usize;
            *pos += 2;
            if buf.len() < *pos + name_len {
                return Err("introduce/create truncated name".to_string());
            }
            let name = core::str::from_utf8(&buf[*pos..*pos + name_len])
                .map_err(|_| "name not utf-8")?
                .to_string();
            *pos += name_len;
            Ok(if kind == 0 {
                Op::NodeIntroduce { subject, name }
            } else {
                Op::MailboxCreate { subject, name }
            })
        }
        2 => {
            if buf.len() < *pos + 32 {
                return Err("revoke truncated".to_string());
            }
            let mut subject = [0u8; 32];
            subject.copy_from_slice(&buf[*pos..*pos + 32]);
            *pos += 32;
            Ok(Op::Revoke { subject })
        }
        3 => {
            if buf.len() < *pos + 32 + 4 {
                return Err("send truncated header".to_string());
            }
            let mut recipient = [0u8; 32];
            recipient.copy_from_slice(&buf[*pos..*pos + 32]);
            *pos += 32;
            let plen = u32::from_be_bytes([
                buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3],
            ]) as usize;
            *pos += 4;
            if buf.len() < *pos + plen {
                return Err("send truncated payload".to_string());
            }
            let payload = buf[*pos..*pos + plen].to_vec();
            *pos += plen;
            Ok(Op::Send { recipient, payload })
        }
        4 => {
            if buf.len() < *pos + 2 {
                return Err("witness truncated header".to_string());
            }
            let count = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]) as usize;
            *pos += 2;
            if buf.len() < *pos + count * 32 {
                return Err("witness truncated also_cite".to_string());
            }
            let mut also_cite = Vec::with_capacity(count);
            for _ in 0..count {
                let mut h = [0u8; 32];
                h.copy_from_slice(&buf[*pos..*pos + 32]);
                *pos += 32;
                also_cite.push(h);
            }
            Ok(Op::Witness { also_cite })
        }
        _ => Err(format!("unknown op kind: {}", kind)),
    }
}
