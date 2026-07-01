//! Event types + signing + verification + canonical encoding.
//!
//! Each node keeps a self-rooted log: every event names its author's previous
//! event via `self_parent` (`None` for that node's genesis), plus zero or more
//! `refs` — foreign heads it has grafted, which double as witnesses. `payload`
//! is opaque to the substrate. `system` is the one thing the substrate *does*
//! interpret: membership changes (Introduce/Depart), which it must read to know
//! who votes on finality. See DESIGN.md.
//!
//! Canonical wire format (hand-rolled, deterministic, byte-stable across
//! machines and language ports):
//!
//!   author:       32 bytes (ed25519 verifying key)
//!   self_parent:  1 tag byte (0 = none, 1 = present) + 32 bytes iff present
//!   refs:         u16 count (BE) + count * 32 bytes
//!   payload:      u32 len (BE) + len bytes
//!   system:       1 tag byte (0 = none, 1 = Introduce, 2 = Depart) + node[32] iff 1|2
//!   signature:    64 bytes — ed25519 over sha256(all of the above, sans sig)

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];
pub type PubKey = [u8; 32];
pub type Sig = [u8; 64];

/// Substrate-interpreted membership operations. Unlike `payload`, the substrate
/// reads these directly to derive the live member set (see dag.rs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemOp {
    /// Admit `node` to the member set.
    Introduce { node: PubKey },
    /// Remove `node` from the member set (self-announced).
    Depart { node: PubKey },
}

#[derive(Clone, Debug)]
pub struct Event {
    /// The member node that signed this event.
    pub author: PubKey,
    /// This author's previous event. `None` only for the author's genesis.
    /// Not required to be unique — an author that forks is admitted as
    /// concurrent siblings (see DESIGN.md / dag.rs).
    pub self_parent: Option<Hash>,
    /// Foreign heads grafted by this event. These are the witnessing +
    /// dissemination edges; together with `self_parent` they form the DAG's
    /// back-edges.
    pub refs: Vec<Hash>,
    /// Opaque application bytes. Empty = a pure graft / heartbeat / witness.
    pub payload: Vec<u8>,
    /// Membership op, if this is a system event. `None` for ordinary events.
    pub system: Option<SystemOp>,
    pub signature: Sig,
}

impl Event {
    /// Build and sign an event with `signing_key`.
    pub fn sign(
        signing_key: &SigningKey,
        self_parent: Option<Hash>,
        refs: Vec<Hash>,
        payload: Vec<u8>,
        system: Option<SystemOp>,
    ) -> Event {
        let author = signing_key.verifying_key().to_bytes();
        let signing_hash = Self::signing_hash(&author, &self_parent, &refs, &payload, &system);
        let signature = signing_key.sign(&signing_hash).to_bytes();
        Event { author, self_parent, refs, payload, system, signature }
    }

    /// The hash that gets signed: sha256 over the canonical encoding minus the
    /// signature. Static so a builder can compute it before signing.
    pub fn signing_hash(
        author: &PubKey,
        self_parent: &Option<Hash>,
        refs: &[Hash],
        payload: &[u8],
        system: &Option<SystemOp>,
    ) -> Hash {
        let mut buf = Vec::with_capacity(32 + 33 + 2 + refs.len() * 32 + 4 + payload.len() + 33);
        encode_unsigned(author, self_parent, refs, payload, system, &mut buf);
        sha256(&buf)
    }

    /// Whole-event hash — referenced by `self_parent`/`refs` of other events.
    /// SHA-256 over the full canonical encoding (including signature).
    pub fn event_hash(&self) -> Hash {
        sha256(&self.encode())
    }

    pub fn verify_signature(&self) -> Result<(), String> {
        let hash =
            Self::signing_hash(&self.author, &self.self_parent, &self.refs, &self.payload, &self.system);
        let vk = VerifyingKey::from_bytes(&self.author).map_err(|e| format!("bad pubkey: {}", e))?;
        let sig = Signature::from_bytes(&self.signature);
        vk.verify(&hash, &sig).map_err(|e| format!("signature: {}", e))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(32 + 33 + 2 + self.refs.len() * 32 + 4 + self.payload.len() + 33 + 64);
        encode_unsigned(&self.author, &self.self_parent, &self.refs, &self.payload, &self.system, &mut out);
        out.extend_from_slice(&self.signature);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let mut cur = Cursor::new(buf);
        let author = cur.take_array::<32>()?;
        let self_parent = match cur.take(1)?[0] {
            0 => None,
            1 => Some(cur.take_array::<32>()?),
            t => return Err(format!("bad self_parent tag: {}", t)),
        };
        let ref_count = cur.take_u16()?;
        let mut refs = Vec::with_capacity(ref_count);
        for _ in 0..ref_count {
            refs.push(cur.take_array::<32>()?);
        }
        let payload_len = cur.take_u32()?;
        let payload = cur.take(payload_len)?.to_vec();
        let system = match cur.take(1)?[0] {
            0 => None,
            1 => Some(SystemOp::Introduce { node: cur.take_array::<32>()? }),
            2 => Some(SystemOp::Depart { node: cur.take_array::<32>()? }),
            t => return Err(format!("bad system tag: {}", t)),
        };
        let signature = cur.take_array::<64>()?;
        Ok(Event { author, self_parent, refs, payload, system, signature })
    }
}

fn encode_unsigned(
    author: &PubKey,
    self_parent: &Option<Hash>,
    refs: &[Hash],
    payload: &[u8],
    system: &Option<SystemOp>,
    out: &mut Vec<u8>,
) {
    out.extend_from_slice(author);
    match self_parent {
        None => out.push(0),
        Some(h) => {
            out.push(1);
            out.extend_from_slice(h);
        }
    }
    out.extend_from_slice(&(refs.len() as u16).to_be_bytes());
    for h in refs {
        out.extend_from_slice(h);
    }
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    match system {
        None => out.push(0),
        Some(SystemOp::Introduce { node }) => {
            out.push(1);
            out.extend_from_slice(node);
        }
        Some(SystemOp::Depart { node }) => {
            out.push(2);
            out.extend_from_slice(node);
        }
    }
}

fn sha256(bytes: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// A forward-only reader over a byte slice with centralized bounds-checking, so
/// the hand-rolled decoder can't run off the end of the buffer.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn samples(sk: &SigningKey) -> Vec<Event> {
        alloc::vec![
            Event::sign(sk, None, Vec::new(), Vec::new(), None), // genesis
            Event::sign(sk, Some([1u8; 32]), Vec::new(), Vec::new(), None), // plain append
            Event::sign(sk, Some([1u8; 32]), alloc::vec![[2u8; 32]], Vec::new(), None), // graft
            Event::sign(sk, Some([1u8; 32]), alloc::vec![[2u8; 32]], b"hi".to_vec(), None), // graft+payload
            Event::sign(sk, Some([1u8; 32]), Vec::new(), Vec::new(), Some(SystemOp::Introduce { node: [7u8; 32] })),
            Event::sign(sk, Some([1u8; 32]), Vec::new(), Vec::new(), Some(SystemOp::Depart { node: [8u8; 32] })),
        ]
    }

    #[test]
    fn round_trips_and_verifies() {
        let sk = key(1);
        for ev in samples(&sk) {
            let bytes = ev.encode();
            let decoded = Event::decode(&bytes).expect("decode");
            assert_eq!(decoded.encode(), bytes, "re-encode must reproduce bytes");
            decoded.verify_signature().expect("signature verifies");
            assert_eq!(decoded.self_parent, ev.self_parent);
            assert_eq!(decoded.refs, ev.refs);
            assert_eq!(decoded.payload, ev.payload);
            assert_eq!(decoded.system, ev.system);
        }
    }

    #[test]
    fn signature_covers_the_system_op() {
        // Tampering with the system op must invalidate the signature — otherwise
        // membership could be forged on a validly-signed event.
        let sk = key(1);
        let mut ev = Event::sign(
            &sk,
            Some([1u8; 32]),
            Vec::new(),
            Vec::new(),
            Some(SystemOp::Introduce { node: [7u8; 32] }),
        );
        ev.system = Some(SystemOp::Introduce { node: [9u8; 32] }); // swap the admitted node
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn decode_rejects_every_truncation() {
        let sk = key(1);
        let ev = Event::sign(
            &sk,
            Some([1u8; 32]),
            alloc::vec![[2u8; 32]],
            b"abc".to_vec(),
            Some(SystemOp::Depart { node: [8u8; 32] }),
        );
        let bytes = ev.encode();
        for n in 0..bytes.len() {
            assert!(Event::decode(&bytes[..n]).is_err(), "prefix len {} should fail", n);
        }
        assert!(Event::decode(&bytes).is_ok());
    }

    #[test]
    fn decode_rejects_bad_tags() {
        let sk = key(1);
        let bytes = Event::sign(&sk, None, Vec::new(), Vec::new(), None).encode();
        // self_parent tag at offset 32.
        let mut bad_sp = bytes.clone();
        bad_sp[32] = 7;
        assert!(Event::decode(&bad_sp).is_err());
    }

    #[test]
    fn tampered_payload_fails_verification() {
        let sk = key(1);
        let mut ev = Event::sign(&sk, Some([1u8; 32]), Vec::new(), b"x".to_vec(), None);
        ev.payload[0] ^= 0xff;
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn event_hash_is_stable_and_distinct() {
        let sk = key(1);
        let a = Event::sign(&sk, Some([1u8; 32]), Vec::new(), b"a".to_vec(), None);
        let b = Event::sign(&sk, Some([1u8; 32]), Vec::new(), b"b".to_vec(), None);
        assert_eq!(a.event_hash(), a.event_hash());
        assert_ne!(a.event_hash(), b.event_hash());
    }
}
