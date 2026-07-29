//! Persistence layer: converts between the in-memory structures and the flat
//! JSON strings stored in `ActorState`, plus the hex helpers they share.
//!
//! IMPORTANT: this is NOT the wire/event format. The event encoding (event.rs)
//! is hand-rolled and canonical because it's signed and sent across the network.
//! This module only serializes the actor's *own* state for theater's value
//! store, so it uses serde_json freely — nothing here is signed or
//! language-portable.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::conn::ConnState;
use crate::dag::Dag;
use crate::event::{Event, Hash};

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
    if !s.len().is_multiple_of(2) {
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

// ---- hash lists (orphan buffer / frontier persistence) ----

pub fn hashes_to_json(hashes: &[Hash]) -> String {
    let hexed: Vec<String> = hashes.iter().map(|h| hex(h)).collect();
    serde_json::to_string(&hexed).unwrap_or_else(|_| "[]".to_string())
}

pub fn hashes_from_json(s: &str) -> Vec<Hash> {
    let hexed: Vec<String> = serde_json::from_str(s).unwrap_or_default();
    hexed.into_iter().filter_map(|h| from_hex32(&h).ok()).collect()
}

// ---- event lists (the persisted orphan buffer) ----

pub fn events_to_json(events: &[Event]) -> String {
    let hexed: Vec<String> = events.iter().map(|e| hex(&e.encode())).collect();
    serde_json::to_string(&hexed).unwrap_or_else(|_| "[]".to_string())
}

pub fn events_from_json(s: &str) -> Vec<Event> {
    let hexed: Vec<String> = serde_json::from_str(s).unwrap_or_default();
    hexed
        .into_iter()
        .filter_map(|h| from_hex(&h).ok())
        .filter_map(|b| Event::decode(&b).ok())
        .collect()
}

// ---- DAG ----

#[derive(serde::Serialize, serde::Deserialize, Default)]
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

/// Rebuild a Dag from persisted state. These events were already validated when
/// first ingested, so we `rehydrate` (rebuild indices) instead of re-running
/// signature + structural checks.
pub fn dag_from_json(s: &str) -> Result<Dag, String> {
    let blob: DagBlob = serde_json::from_str(s).unwrap_or_default();
    let mut events = Vec::with_capacity(blob.events_hex.len());
    for ev_hex in blob.events_hex.values() {
        let bytes = from_hex(ev_hex)?;
        if let Ok(ev) = Event::decode(&bytes) {
            events.push(ev);
        }
    }
    Ok(Dag::rehydrate(events))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

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
    fn hashes_round_trip() {
        let hashes = alloc::vec![[1u8; 32], [2u8; 32]];
        assert_eq!(hashes_from_json(&hashes_to_json(&hashes)), hashes);
    }

    #[test]
    fn dag_persists_and_rehydrates() {
        let a = SigningKey::from_bytes(&[1u8; 32]);
        let mut dag = Dag::new();

        let g = Event::sign(&a, 0, None, Vec::new(), b"hi".to_vec());
        let h = g.event_hash();
        dag.ingest(g).unwrap();

        let json = dag_to_json(&dag);
        let restored = dag_from_json(&json).unwrap();
        assert!(restored.has(&h));
        assert_eq!(restored.events.len(), dag.events.len());
    }

    #[test]
    fn persisted_orphan_resolves_after_reload() {
        let a = SigningKey::from_bytes(&[1u8; 32]);
        let b = SigningKey::from_bytes(&[2u8; 32]);

        let sign = |sk: &SigningKey, sp: Option<Hash>, refs: Vec<Hash>| {
            Event::sign(sk, 0, sp, refs, Vec::new())
        };

        let mut dag = Dag::new();
        let ga = sign(&a, None, Vec::new());
        let gah = ga.event_hash();
        dag.ingest(ga).unwrap();

        let gb = sign(&b, None, Vec::new());
        let gbh = gb.event_hash();
        // child refs gb, which we don't have yet → buffered, not admitted.
        let child = sign(&a, Some(gah), alloc::vec![gbh]);
        let child_h = child.event_hash();
        assert_eq!(dag.ingest(child).unwrap(), false);

        // Persist (admitted DAG + pending orphan buffer) and reload.
        let dag_json = dag_to_json(&dag);
        let pending_json = events_to_json(&dag.pending_events());
        let mut reloaded = dag_from_json(&dag_json).unwrap();
        for ev in events_from_json(&pending_json) {
            let _ = reloaded.ingest(ev);
        }
        assert!(!reloaded.has(&child_h), "still orphaned across reload");

        // The dependency arrives → the buffered child resolves.
        reloaded.ingest(gb).unwrap();
        assert!(reloaded.has(&child_h));
    }
}
