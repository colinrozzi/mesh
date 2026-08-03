//! Runtime proof for the composition pilot.
//!
//! Composes the sentinel-shaped entry stub with the `mesh-client` package, loads
//! the composite under the async runtime supplying ONLY
//! `theater:simple/message-server-host.request` (the residual host import), and
//! calls `run` — which forwards to the composed `mesh.submit`, which calls the
//! residual host import, which suspends. We assert:
//!
//!   1. the host import receives a real `Submit` command carrying our payload
//!      (proving the payload marshalled entry -> provider -> host across the
//!      memory gap), and
//!   2. `run` returns the hash from the ack (proving the reply marshalled back
//!      THROUGH the async suspend), and
//!   3. the host import was invoked exactly once (proving the fiber genuinely
//!      suspended on it, not short-circuited).
//!
//! This is the mesh-shaped analogue of pack's own `compose_async` test — a
//! non-entry provider making an async residual host call that RETURNS a payload.
//!
//! Prereqs (this test reads prebuilt wasm; it does not invoke cargo):
//!   cargo build --release --target wasm32-unknown-unknown \
//!       --manifest-path mesh-client-pkg/Cargo.toml
//!   cargo build --release --target wasm32-unknown-unknown \
//!       --manifest-path compose-smoke/sentinel-stub/Cargo.toml

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use packr::abi::{FromValue, Value};
use packr::compose::{compose, Component, GraphLink};
use packr::AsyncRuntime;

const SENTINEL_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/sentinel-stub/target/wasm32-unknown-unknown/release/sentinel_stub.wasm"
);
const MESH_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../mesh-client-pkg/target/wasm32-unknown-unknown/release/mesh_client_pkg.wasm"
);

struct HostState {
    calls: AtomicUsize,
    hash: [u8; 32],
    expected_payload: Vec<u8>,
}

/// A sentinel→mesh-client compose link (all our links share consumer/provider).
fn link(module: &str, name: &str, export: &str) -> GraphLink {
    GraphLink {
        consumer: "sentinel".into(),
        import_module: module.into(),
        import_name: name.into(),
        provider: "mesh-client".into(),
        export_name: export.into(),
    }
}

#[tokio::test]
async fn submit_round_trips_through_composition_and_async_host() {
    let sentinel = match std::fs::read(SENTINEL_WASM) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("SKIP: sentinel-stub wasm missing ({e}). Build it first (see module docs).");
            return;
        }
    };
    let mesh = match std::fs::read(MESH_WASM) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("SKIP: mesh-client-pkg wasm missing ({e}). Build it first (see module docs).");
            return;
        }
    };

    let components = vec![
        Component { name: "sentinel".into(), wasm: sentinel, entry: true },
        Component { name: "mesh-client".into(), wasm: mesh, entry: false },
    ];
    // Link every interface import the stub declares (submit + the two
    // mesh-control fns it binds); only message-server-host is left residual for
    // the host to supply.
    let links = vec![
        link("mesh", "submit", "submit"),
        link("mesh-control", "encode-lifecycle", "encode-lifecycle"),
        link("mesh-control", "decode-lifecycle", "decode-lifecycle"),
    ];
    let composite = compose(components, &links).expect("compose sentinel + mesh-client");

    let runtime = AsyncRuntime::new();
    let module = runtime.load_module(&composite).expect("load composite");

    let payload = b"hello-mesh".to_vec();
    let state = Arc::new(HostState {
        calls: AtomicUsize::new(0),
        hash: [7u8; 32],
        expected_payload: payload.clone(),
    });

    let mut instance = module
        .instantiate_with_host_async(state.clone(), |builder| {
            builder
                .interface("theater:simple/message-server-host")?
                .func_async(
                    "request",
                    |ctx: packr::AsyncCtx<Arc<HostState>>, input: Value| async move {
                        // Genuinely async: force the guest fiber to suspend here.
                        tokio::task::yield_now().await;
                        let st = ctx.data();
                        st.calls.fetch_add(1, Ordering::SeqCst);

                        // input = (actor-id: string, msg: list<u8>). The msg must
                        // be the Submit command mesh-client built from our payload.
                        let (_actor_id, msg): (String, Vec<u8>) =
                            input.try_into().expect("request args (string, list<u8>)");
                        match mesh_api::decode_command(&msg) {
                            Some(mesh_api::Command::Submit(p)) => {
                                assert_eq!(p, st.expected_payload, "Submit payload marshalled intact");
                            }
                            _ => panic!("expected a Submit command from mesh-client"),
                        }

                        // Reply with an ok-ack for a known hash.
                        let ack = mesh_api::encode_ack(true, &st.hash, "");
                        let reply: Value = Ok::<Vec<u8>, String>(ack).into();
                        reply
                    },
                )?;
            Ok(())
        })
        .await
        .expect("instantiate composite with residual message-server-host.request");

    let input: Value = ("node-1".to_string(), payload.clone()).into();
    let result = instance
        .call_with_value_async("run", &input)
        .await
        .expect("call run on composite");

    let decoded: Result<Vec<u8>, String> =
        FromValue::from_value(result).expect("decode run -> result<list<u8>, string>");

    assert_eq!(
        decoded,
        Ok(state.hash.to_vec()),
        "run -> mesh.submit must return the acked hash, marshalled back through the async suspend"
    );
    assert_eq!(
        state.calls.load(Ordering::SeqCst),
        1,
        "the residual message-server-host.request must have been invoked exactly once"
    );
}

/// The mesh-control envelope (encode → decode) round-trips across composition.
/// The stub's `roundtrip-lifecycle` calls the composed `mesh-control` encode then
/// decode and reports whether the fields survived — exercising the new
/// `option<tuple<...>>` marshalling across the memory gap. Pure (no host call),
/// but the composite still carries the residual message-server-host.request from
/// the mesh interface, so we satisfy it with an unused stub.
#[tokio::test]
async fn control_envelope_round_trips_through_composition() {
    let (sentinel, mesh) = match (std::fs::read(SENTINEL_WASM), std::fs::read(MESH_WASM)) {
        (Ok(s), Ok(m)) => (s, m),
        _ => {
            eprintln!("SKIP: component wasm missing. Build both first (see module docs).");
            return;
        }
    };

    let components = vec![
        Component { name: "sentinel".into(), wasm: sentinel, entry: true },
        Component { name: "mesh-client".into(), wasm: mesh, entry: false },
    ];
    // The stub binds submit + the two mesh-control fns, so link all three; only
    // message-server-host stays residual.
    let links = vec![
        link("mesh", "submit", "submit"),
        link("mesh-control", "encode-lifecycle", "encode-lifecycle"),
        link("mesh-control", "decode-lifecycle", "decode-lifecycle"),
    ];
    let composite = compose(components, &links).expect("compose with mesh-control links");

    let runtime = AsyncRuntime::new();
    let module = runtime.load_module(&composite).expect("load composite");

    let mut instance = module
        .instantiate_with_host_async((), |builder| {
            // Unused here, but the mesh interface's residual import must be satisfied.
            builder
                .interface("theater:simple/message-server-host")?
                .func_async("request", |_ctx: packr::AsyncCtx<()>, _input: Value| async move {
                    let empty: Value = Ok::<Vec<u8>, String>(Vec::new()).into();
                    empty
                })?;
            Ok(())
        })
        .await
        .expect("instantiate");

    let input: Value = (5u8, "actor-x".to_string(), 12_345u64, b"payload".to_vec()).into();
    let result = instance
        .call_with_value_async("roundtrip-lifecycle", &input)
        .await
        .expect("call roundtrip-lifecycle");

    let ok: bool = result.try_into().expect("roundtrip-lifecycle returns bool");
    assert!(ok, "the lifecycle envelope must survive encode→decode across composition");
}
