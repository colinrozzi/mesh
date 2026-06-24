//! Persistence layer: converts between the in-memory structures and the flat
//! JSON strings stored in `ActorState`, plus the hex helpers they share.
//!
//! IMPORTANT: this is NOT the wire/event format. The event and frame encodings
//! (event.rs, wire.rs) are hand-rolled and canonical because they're signed and
//! sent across the network. This module only serializes the actor's *own* state
//! for theater's value store, so it uses serde_json freely — nothing here is
//! signed, hashed, or language-portable.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::conn::ConnState;
use crate::dag::Dag;
use crate::event::{Event, Hash, PubKey};

// ---- hex ----

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub fn from_hex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("bad hex at byte {}", i))?;
    }
    Ok(out)
}

pub fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex".to_string());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| format!("bad hex at {}", i))?);
    }
    Ok(out)
}

// ---- connections ----

pub fn connections_to_json(conns: &BTreeMap<String, ConnState>) -> String {
    serde_json::to_string(conns).unwrap_or_else(|_| "{}".to_string())
}

pub fn connections_from_json(s: &str) -> BTreeMap<String, ConnState> {
    serde_json::from_str(s).unwrap_or_default()
}

// ---- witness-pending hashes ----

pub fn pending_to_json(hashes: &[Hash]) -> String {
    let hexed: Vec<String> = hashes.iter().map(|h| hex(h)).collect();
    serde_json::to_string(&hexed).unwrap_or_else(|_| "[]".to_string())
}

pub fn pending_from_json(s: &str) -> Vec<Hash> {
    let hexed: Vec<String> = serde_json::from_str(s).unwrap_or_default();
    hexed.into_iter().filter_map(|h| from_hex32(&h).ok()).collect()
}

// ---- peer nodes (genesis pre-admits), stored as [[pubkey_hex, name], ...] ----

pub fn peer_nodes_to_json(peers: &[(PubKey, String)]) -> String {
    let mapped: Vec<(String, String)> =
        peers.iter().map(|(pk, name)| (hex(pk), name.clone())).collect();
    serde_json::to_string(&mapped).unwrap_or_else(|_| "[]".to_string())
}

pub fn peer_nodes_from_json(s: &str) -> Vec<(PubKey, String)> {
    let raw: Vec<(String, String)> = serde_json::from_str(s).unwrap_or_default();
    raw.into_iter()
        .filter_map(|(pk_hex, name)| from_hex32(&pk_hex).ok().map(|pk| (pk, name)))
        .collect()
}

// ---- DAG ----

#[derive(serde::Serialize, serde::Deserialize)]
struct DagBlob {
    /// event_hash_hex -> event_bytes_hex (the canonical Event::encode()).
    events_hex: BTreeMap<String, String>,
}

pub fn dag_to_json(dag: &Dag) -> String {
    let mut events_hex = BTreeMap::new();
    for (h, ev) in &dag.events {
        events_hex.insert(hex(h), hex(&ev.encode()));
    }
    serde_json::to_string(&DagBlob { events_hex }).unwrap_or_else(|_| "{}".to_string())
}

/// Rebuild a Dag from persisted state. These events were already validated
/// when first ingested, so we `rehydrate` (rebuild indices) instead of
/// re-running signature + rule checks on every callback.
pub fn dag_from_json(
    s: &str,
    root_pubkey_hex: &str,
    peer_nodes_json: &str,
) -> Result<Dag, String> {
    let blob: DagBlob =
        serde_json::from_str(s).unwrap_or(DagBlob { events_hex: BTreeMap::new() });
    let root = from_hex32(root_pubkey_hex)?;
    let peer_nodes = peer_nodes_from_json(peer_nodes_json);
    let mut events = Vec::with_capacity(blob.events_hex.len());
    for ev_hex in blob.events_hex.values() {
        let bytes = from_hex(ev_hex)?;
        if let Ok(ev) = Event::decode(&bytes) {
            events.push(ev);
        }
    }
    Ok(Dag::rehydrate(root, peer_nodes, events))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, GENESIS_PARENT};
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn hex_round_trips() {
        let bytes: Vec<u8> = (0u8..=255).collect();
        assert_eq!(from_hex(&hex(&bytes)).unwrap(), bytes);
        assert_eq!(from_hex32(&hex(&[7u8; 32])).unwrap(), [7u8; 32]);
    }

    #[test]
    fn from_hex32_rejects_bad_input() {
        assert!(from_hex32("abc").is_err()); // wrong length
        assert!(from_hex32(&"zz".repeat(32)).is_err()); // non-hex digits
    }

    #[test]
    fn pending_round_trips() {
        let hashes = alloc::vec![[1u8; 32], [2u8; 32]];
        assert_eq!(pending_from_json(&pending_to_json(&hashes)), hashes);
    }

    #[test]
    fn peer_nodes_round_trip() {
        let peers = alloc::vec![([3u8; 32], "node-b".to_string())];
        assert_eq!(peer_nodes_from_json(&peer_nodes_to_json(&peers)), peers);
    }

    #[test]
    fn dag_persists_and_rehydrates() {
        let root = SigningKey::from_bytes(&[1u8; 32]);
        let root_pk = root.verifying_key().to_bytes();
        let mut dag = Dag::new(root_pk, Vec::new());

        let op = Op::MailboxCreate { subject: [7u8; 32], name: "alice".to_string() };
        let signing_hash = Event::signing_hash(&GENESIS_PARENT, &root_pk, &op);
        let create = Event {
            parent: GENESIS_PARENT,
            author: root_pk,
            op,
            signature: root.sign(&signing_hash).to_bytes(),
        };
        let h = create.event_hash();
        dag.ingest(create).unwrap();

        let json = dag_to_json(&dag);
        let restored = dag_from_json(&json, &hex(&root_pk), "[]").unwrap();
        assert!(restored.events.contains_key(&h));
        assert_eq!(restored.events.len(), dag.events.len());
        // The rehydrated DAG derives the same state.
        assert!(restored.state_at(&h).unwrap().is_member(&[7u8; 32]));
    }
}
