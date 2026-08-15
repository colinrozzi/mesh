// The chat network's data schema — the payload kinds for OR-Set membership + a text log.
// ONE source of truth, sourced by the SM (and any consumer) via `wit!(from "../chat.wit")`
// — no shared Rust crate. Drift is caught structurally at compose. Single-field kinds, so
// each variant case carries its payload type directly.

variant msg {
    genesis(list<list<u8>>),
    text(string),
    member-add(list<u8>),
    member-remove(list<u8>),
}

world chat {}
