// The `mesh-control` interface — the app-level CONTROL envelope carried INSIDE a
// mesh Submit payload. The node never parses it; it just broadcasts the opaque
// payload. Two members (e.g. an orchestrator and a sentinel) use this to run
// request/response + lifecycle over the broadcast log.
//
// Exported by the SAME package as `mesh` (mesh_client_pkg.wasm) — a separate,
// opt-in interface. Transport-only consumers ignore it; control consumers link
// it too. `encode-*` build a payload to submit; `control-kind` + `decode-*` parse
// a delivered payload. target is a 32-byte pubkey as list<u8>; `encode-*` return
// empty bytes on a bad-length target.
//
// Validate:  pact check mesh-client-pkg/mesh-control.pact
interface mesh-control {
    exports {
        encode-command: func(corr-id: u64, target: list<u8>, cmd: string, args: list<u8>) -> list<u8>
        encode-response: func(corr-id: u64, target: list<u8>, result: list<u8>) -> list<u8>
        encode-lifecycle: func(event: u8, actor-id: string, ts: u64, data: list<u8>) -> list<u8>

        // control-kind returns 1=command / 2=response / 3=lifecycle; Some(k)
        // guarantees the matching decode-* succeeds.
        control-kind: func(bytes: list<u8>) -> option<u8>
        decode-command: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, string, list<u8>>>
        decode-response: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, list<u8>>>
        decode-lifecycle: func(bytes: list<u8>) -> option<tuple<u8, string, u64, list<u8>>>
    }
}
