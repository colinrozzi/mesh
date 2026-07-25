// The `mesh` interface — the canonical signature of the mesh client component.
//
// This is the contract between the mesh-client package (which EXPORTS these) and
// any consumer (which IMPORTS them and is composed with the package via
// `packr compose`). The interface hash is computed over ALL of these bindings, so
// a consumer must declare the COMPLETE interface to satisfy a hash-checked link —
// even functions it never calls. See CONSUMER.md for the paste-ready consumer
// `pack_types!` block and the `#[import_from]` bindings.
//
// hash / pubkey are `list<u8>` (32 bytes) across the boundary.
//
// Validate:  pact check mesh-client-pkg/mesh.pact
interface mesh {
    exports {
        // Submit a payload to `node`; returns the committed event hash.
        submit: func(node: string, payload: list<u8>) -> result<list<u8>, string>

        // Ask `node` to admit `member` (a 32-byte pubkey); returns the hash.
        introduce: func(node: string, member: list<u8>) -> result<list<u8>, string>

        // Ask `node` to leave the network; returns the hash.
        depart: func(node: string) -> result<list<u8>, string>

        // Subscribe `app-id` for committed-payload delivery from `node`.
        // Returns true on success (the ack carries no data).
        register: func(node: string, app-id: string) -> result<bool, string>

        // Decode a delivery received in handle-send into (from, body). Pure.
        delivery: func(msg: list<u8>) -> option<tuple<list<u8>, list<u8>>>

        // Build a node InitConfig JSON for supervisor.spawn. Pure.
        node-config: func(seed: string, listen: string, members: list<string>, dial: list<tuple<string, string>>) -> string
    }
}
