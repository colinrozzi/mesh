# Releasing mesh

Releases are cut **manually** from **locally-built, tested artifacts** — the exact
bytes that were verified end-to-end (and, for the node, deployed) go to the
GitHub release. There is intentionally **no** tag-triggered CI build+upload.

## Why no CI release build

An earlier `release.yml` rebuilt the wasm in CI on tag push and clobbered the
manually-uploaded assets. The CI toolchain (`dtolnay/rust-toolchain@stable`)
produces **different bytes** than the local build (nix `rust 1.96.0`) — same
source, different profile — so the published asset stopped matching the tested +
deployed binary (v0.3.1: published `27ee3e54` vs tested/live `da32d291`). wasm
builds are not byte-reproducible across toolchains, so a CI rebuild cannot be
trusted to equal what was tested.

Compounding it: theater's `static_package = true` is an operator **assertion**
that a package URL's bytes won't change — it does **not** verify a content hash.
So a consumer pinning the release URL would silently fetch a clobbered binary on
the next restart. The release URL must therefore be **immutable in practice** —
which means nothing may re-upload over a tested asset.

## Cut a release

1. Build both wasm from the release commit, in the nix devshell:
   ```sh
   cargo build --release --target wasm32-unknown-unknown
   cargo build --release --target wasm32-unknown-unknown --manifest-path mesh-client-pkg/Cargo.toml
   ```
2. Run the tests and (for a node change) the integration + composite round-trips.
   The binaries you tested are the binaries you ship.
3. Record the node sha so downstreams can verify:
   ```sh
   sha256sum target/wasm32-unknown-unknown/release/mesh.wasm
   ```
4. Tag and publish with the **exact tested artifacts** (5 assets):
   ```sh
   git tag vX.Y.Z <commit> && git push origin vX.Y.Z
   gh release create vX.Y.Z --title "..." --notes "..." \
     target/wasm32-unknown-unknown/release/mesh.wasm \
     mesh-client-pkg/target/wasm32-unknown-unknown/release/mesh_client_pkg.wasm \
     mesh-client-pkg/mesh.pact mesh-client-pkg/mesh-control.pact mesh-client-pkg/CONSUMER.md
   ```
5. **Verify the published artifact** (do not trust the upload — verify the real
   asset): download `.../releases/download/vX.Y.Z/mesh.wasm` and confirm its
   sha256 equals step 3. State that sha in the release notes.

## Future improvement

A reproducible build pipeline (build the release artifact via the nix flake with
pinned toolchain + `--remap-path-prefix`, then test *that* artifact and publish
it) would restore automation without the clobber risk. Until then, releases are
manual, and consumers should content-pin (e.g. a nix `fetchurl` SRI, as the
sentinel flake does for the client) rather than trust a bare URL.
