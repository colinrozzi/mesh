#!/usr/bin/env bash
# Build the mesh node + example-app into theater-loadable self-contained
# composites. `packr build` injects the fixed-base recipe and links the bundled
# allocator in one step — no .cargo/config, no link manifest.
#
# Run inside a shell with cargo + the wasm32 target + wasm-merge (binaryen):
#   nix develop /home/colin/work/theater -c bash pack/build.sh
# Override the packr CLI location with PACKR=... if it is not on PATH.
set -euo pipefail
MESH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKR="${PACKR:-$(command -v packr || echo /home/colin/work/pack/target/release/packr)}"

"$PACKR" build "$MESH"              # -> target/.../release/mesh.composite.wasm
"$PACKR" build "$MESH/example-app"  # -> example-app/target/.../release/mesh_example_app.composite.wasm
