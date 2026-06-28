//! Event types + signing + verification + canonical encoding (v3).
//!
//! Each node keeps a self-rooted log: every event names its author's previous
//! event via `self_parent` (`None` for that node's genesis), plus zero or more
//! `refs` — foreign heads it has grafted, which double as witnesses. The
//! substrate treats `payload` as opaque bytes. See DESIGN-v3.md.
//!
//! Canonical wire format (hand-rolled, deterministic, byte-stable across
//! machines and language ports):
//!
//!   author:       32 bytes (ed25519 verifying key)
//!   self_parent:  1 tag byte (0 = none, 1 = present) + 32 bytes iff present
//!   refs:         u16 count (BE) + count * 32 bytes
//!   payload:      u32 len (BE) + len bytes
//!   signature:    64 bytes — ed25519 over sha256(all of the above, sans sig)

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];
pub type PubKey = [u8; 32];
pub type Sig = [u8; 64];

#[derive(Clone, Debug)]
pub struct Event {
    /// The member node that signed this event.
    pub author: PubKey,
    /// This author's previous event. `None` only for the author's genesis.
    /// Not required to be unique — an author that forks is admitted as
    /// concurrent siblings (see DESIGN-v3.md / dag.rs).
    pub self_parent: Option<Hash>,
    /// Foreign heads grafted by this event. These are the witnessing +
    /// dissemination edges; together with `self_parent` they form the DAG's
    /// back-edges.
    pub refs: Vec<Hash>,
    /// Opaque application bytes. Empty = a pure graft / heartbeat / witness.
    pub payload: Vec<u8>,
    pub signature: Sig,
}

impl Event {
    /// The hash that gets signed: sha256 over the canonical encoding minus the
    /// signature. Static so a builder can compute it before signing.
    pub fn signing_hash(
        author: &PubKey,
        self_parent: &Option<Hash>,
        refs: &[Hash],
        payload: &[u8],
    ) -> Hash {
        let mut buf = Vec::with_capacity(32 + 33 + 2 + refs.len() * 32 + 4 + payload.len());
        encode_unsigned(author, self_parent, refs, payload, &mut buf);
        sha256(&buf)
    }

    /// Whole-event hash — referenced by `self_parent`/`refs` of other events.
    /// SHA-256 over the full canonical encoding (including signature).
    pub fn event_hash(&self) -> Hash {
        sha256(&self.encode())
    }

    pub fn verify_signature(&self) -> Result<(), String> {
        let hash = Self::signing_hash(&self.author, &self.self_parent, &self.refs, &self.payload);
        let vk = VerifyingKey::from_bytes(&self.author).map_err(|e| format!("bad pubkey: {}", e))?;
        let sig = Signature::from_bytes(&self.signature);
        vk.verify(&hash, &sig).map_err(|e| format!("signature: {}", e))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(32 + 33 + 2 + self.refs.len() * 32 + 4 + self.payload.len() + 64);
        encode_unsigned(&self.author, &self.self_parent, &self.refs, &self.payload, &mut out);
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
        let signature = cur.take_array::<64>()?;
        Ok(Event { author, self_parent, refs, payload, signature })
    }
}

fn encode_unsigned(
    author: &PubKey,
    self_parent: &Option<Hash>,
    refs: &[Hash],
    payload: &[u8],
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
    use ed25519_dalek::{Signer, SigningKey};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn signed(sk: &SigningKey, self_parent: Option<Hash>, refs: Vec<Hash>, payload: Vec<u8>) -> Event {
        let author = sk.verifying_key().to_bytes();
        let signing_hash = Event::signing_hash(&author, &self_parent, &refs, &payload);
        let signature = sk.sign(&signing_hash).to_bytes();
        Event { author, self_parent, refs, payload, signature }
    }

    fn samples(sk: &SigningKey) -> Vec<Event> {
        alloc::vec![
            signed(sk, None, Vec::new(), Vec::new()),                       // genesis
            signed(sk, Some([1u8; 32]), Vec::new(), Vec::new()),            // plain append
            signed(sk, Some([1u8; 32]), alloc::vec![[2u8; 32]], Vec::new()), // single graft
            signed(sk, Some([1u8; 32]), alloc::vec![[2u8; 32], [3u8; 32]], b"hello".to_vec()), // graft + payload
            signed(sk, None, alloc::vec![[9u8; 32]], b"genesis with payload".to_vec()),
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
        }
    }

    #[test]
    fn decode_rejects_every_truncation() {
        let sk = key(1);
        let ev = signed(&sk, Some([1u8; 32]), alloc::vec![[2u8; 32]], b"abc".to_vec());
        let bytes = ev.encode();
        for n in 0..bytes.len() {
            assert!(Event::decode(&bytes[..n]).is_err(), "prefix len {} should fail", n);
        }
        assert!(Event::decode(&bytes).is_ok());
    }

    #[test]
    fn decode_rejects_bad_self_parent_tag() {
        let sk = key(1);
        let bytes = signed(&sk, None, Vec::new(), Vec::new()).encode();
        let mut bad = bytes.clone();
        bad[32] = 7; // self_parent tag sits right after the 32-byte author
        assert!(Event::decode(&bad).is_err());
    }

    #[test]
    fn tampered_payload_fails_verification() {
        let sk = key(1);
        let mut ev = signed(&sk, Some([1u8; 32]), Vec::new(), b"x".to_vec());
        ev.payload[0] ^= 0xff;
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn wrong_author_fails_verification() {
        let sk = key(1);
        let mut ev = signed(&sk, None, Vec::new(), Vec::new());
        ev.author = key(2).verifying_key().to_bytes();
        assert!(ev.verify_signature().is_err());
    }

    #[test]
    fn event_hash_is_stable_and_distinct() {
        let sk = key(1);
        let a = signed(&sk, Some([1u8; 32]), Vec::new(), b"a".to_vec());
        let b = signed(&sk, Some([1u8; 32]), Vec::new(), b"b".to_vec());
        assert_eq!(a.event_hash(), a.event_hash());
        assert_ne!(a.event_hash(), b.event_hash());
    }

    #[test]
    fn self_parent_presence_changes_the_hash() {
        let sk = key(1);
        let g = signed(&sk, None, Vec::new(), Vec::new());
        let p = signed(&sk, Some([0u8; 32]), Vec::new(), Vec::new());
        // None vs Some(all-zeros) must not collide — the tag byte distinguishes them.
        assert_ne!(g.event_hash(), p.event_hash());
    }
}
