//! Message-passing — the first reducer on the substrate (v3).
//!
//! A message payload is `recipient[32] || body`. The reducer folds finalized
//! messages into per-recipient inboxes. Recipients are *application* identities
//! (any pubkey) — distinct from the substrate's member set, which only governs
//! consensus. Empty payloads (heartbeats) and malformed payloads are ignored
//! deterministically (reducer-validity), so every node derives the same inboxes.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::event::{Hash, PubKey};
use crate::reducer::Reducer;

/// Encode a message payload: `recipient[32] || body`. Client-facing helper.
#[allow(dead_code)]
pub fn encode_message(recipient: &PubKey, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + body.len());
    out.extend_from_slice(recipient);
    out.extend_from_slice(body);
    out
}

/// Decode a message payload. `None` if it's too short to carry a recipient.
pub fn decode_message(payload: &[u8]) -> Option<(PubKey, &[u8])> {
    if payload.len() < 32 {
        return None;
    }
    let mut recipient = [0u8; 32];
    recipient.copy_from_slice(&payload[..32]);
    Some((recipient, &payload[32..]))
}

#[derive(Clone, Debug)]
pub struct Message {
    pub from: PubKey,
    pub event: Hash,
    pub body: Vec<u8>,
}

#[derive(Default)]
pub struct Inboxes {
    pub by_recipient: BTreeMap<PubKey, Vec<Message>>,
}

impl Inboxes {
    /// Query accessor for committed messages to a recipient.
    #[allow(dead_code)]
    pub fn inbox(&self, recipient: &PubKey) -> &[Message] {
        self.by_recipient.get(recipient).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// The message-passing state machine.
pub struct Mailboxes;

impl Reducer for Mailboxes {
    type State = Inboxes;

    fn apply(state: &mut Inboxes, author: &PubKey, event_hash: &Hash, payload: &[u8]) {
        if payload.is_empty() {
            return; // heartbeat / pure graft
        }
        if let Some((recipient, body)) = decode_message(payload) {
            state.by_recipient.entry(recipient).or_default().push(Message {
                from: *author,
                event: *event_hash,
                body: body.to_vec(),
            });
        }
        // malformed payloads (too short) are deterministically ignored
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::Dag;
    use crate::event::Event;
    use crate::reducer::fold;
    use alloc::collections::BTreeSet;
    use ed25519_dalek::{Signer, SigningKey};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pk(sk: &SigningKey) -> PubKey {
        sk.verifying_key().to_bytes()
    }

    fn signed(sk: &SigningKey, sp: Option<Hash>, refs: Vec<Hash>, payload: Vec<u8>) -> Event {
        let author = pk(sk);
        let sh = Event::signing_hash(&author, &sp, &refs, &payload);
        Event { author, self_parent: sp, refs, payload, signature: sk.sign(&sh).to_bytes() }
    }

    /// A 2-member network where A authors one event with `payload` and B
    /// witnesses it, so that event finalizes. Returns the dag + the event hash.
    fn finalized_with_payload(payload: Vec<u8>) -> (Dag, SigningKey, SigningKey, Hash) {
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(BTreeSet::from([pk(&a), pk(&b)]));

        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gb = signed(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();

        let m = signed(&a, Some(gah), alloc::vec![gbh], payload);
        let mh = m.event_hash();
        dag.ingest(m).unwrap();

        // B grafts m, so every member has now witnessed it.
        let b1 = signed(&b, Some(gbh), alloc::vec![mh], Vec::new());
        dag.ingest(b1).unwrap();
        (dag, a, b, mh)
    }

    #[test]
    fn delivers_finalized_message_to_recipient() {
        let a = key(1);
        let b = key(2);
        let body = b"hello bob".to_vec();
        let (dag, _, _, mh) =
            finalized_with_payload(encode_message(&pk(&b), &body));
        assert!(dag.is_finalized(&mh));

        let inboxes = fold::<Mailboxes>(&dag);
        let inbox = inboxes.inbox(&pk(&b));
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].from, pk(&a));
        assert_eq!(inbox[0].body, body);
        assert_eq!(inbox[0].event, mh);
        // The sender's own inbox stays empty.
        assert!(inboxes.inbox(&pk(&a)).is_empty());
    }

    #[test]
    fn ignores_heartbeats_and_malformed_payloads() {
        // A finalized event carrying a too-short (non-empty) payload.
        let (dag, _, _, mh) = finalized_with_payload(b"x".to_vec());
        assert!(dag.is_finalized(&mh));
        let inboxes = fold::<Mailboxes>(&dag);
        // Genesis events (empty payload) and the malformed event produce nothing.
        assert!(inboxes.by_recipient.is_empty());
    }

    #[test]
    fn unfinalized_message_is_not_committed() {
        // Same as the delivery case but B never witnesses, so nothing finalizes.
        let a = key(1);
        let b = key(2);
        let mut dag = Dag::new(BTreeSet::from([pk(&a), pk(&b)]));
        let ga = signed(&a, None, Vec::new(), Vec::new());
        let gb = signed(&b, None, Vec::new(), Vec::new());
        let (gah, gbh) = (ga.event_hash(), gb.event_hash());
        dag.ingest(ga).unwrap();
        dag.ingest(gb).unwrap();
        let m = signed(&a, Some(gah), alloc::vec![gbh], encode_message(&pk(&b), b"hi"));
        let mh = m.event_hash();
        dag.ingest(m).unwrap();

        assert!(!dag.is_finalized(&mh));
        let inboxes = fold::<Mailboxes>(&dag);
        assert!(inboxes.inbox(&pk(&b)).is_empty());
    }
}
