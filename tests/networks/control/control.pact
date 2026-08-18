// The control-plane data schema — the 5 payload kinds. ONE source of truth, sourced by the
// SM (and any consumer) via `wit!(from "../control.wit")` — no shared Rust crate. The
// multi-field kinds (genesis / command / response) carry a record; the bare kinds
// (join-request / depart) carry nothing. This is exactly what `#[derive(GraphValue)]`
// produced for the old struct-variant enum (a case wrapping a record), now declared.

record genesis-cfg {
    members: list<list<u8>>,
    join-allow: list<list<u8>>,
    command-allow: list<list<u8>>,
}

record command-body {
    corr-id: u64,
    verb: string,
    args: list<u8>,
}

record response-body {
    corr-id: u64,
    cmd-author: list<u8>,
    result: list<u8>,
}

variant msg {
    genesis(genesis-cfg),
    join-request,
    depart,
    command(command-body),
    response(response-body),
}

world control {}
