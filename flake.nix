{
  description = "mesh: RSM substrate — pure node + generic system entry, plus turnkey external composition (lib.mkComposite) so an impl repo composes its SM into the node with one `nix build`.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";

    theater = {
      url = "git+file:///home/colin/work/theater";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-overlay.follows = "rust-overlay";
      inputs.crane.follows = "crane";
    };

    # packr — the composition CLI. Its flake wraps the binary with binaryen (wasm-merge),
    # so `packr compose` runs correctly inside the nix sandbox.
    pack = {
      url = "git+file:///home/colin/work/pack";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, theater, pack }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          targets = [ "wasm32-unknown-unknown" ];
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # Filter a source tree down to what a wasm crate build needs. NOTE: `.wit`/`.pact`
        # MUST be included — an SM that uses `wit!(from "x.wit")` reads the file at compile
        # time, so it has to be present in the sandbox.
        filterSrc = s: pkgs.lib.cleanSourceWith {
          src = s;
          filter = path: type:
            (pkgs.lib.hasSuffix ".rs" path) ||
            (pkgs.lib.hasSuffix ".toml" path) ||
            (pkgs.lib.hasSuffix ".lock" path) ||
            (pkgs.lib.hasSuffix ".wit" path) ||
            (pkgs.lib.hasSuffix ".pact" path) ||
            (type == "directory");
        };

        packrBin = pack.packages.${system}.packr;

        # Build one standalone wasm crate (its own Cargo.lock, possibly with path deps
        # elsewhere in `src`). crane's buildPackage is workspace-opinionated (its install
        # hook resolves the ROOT package's deps against the sub-crate's vendor and fails), so
        # we vendor the crate's own lock and run a plain offline `cargo build`. Exported so an
        # impl repo can build ITS OWN SM the same way — pass your repo as `src`.
        #   buildWasm { pname = "chat-sm"; src = ./.; crate = "chat-sm"; wasmName = "chat_sm.wasm"; }
        buildWasm = { pname, src, crate ? ".", wasmName }:
          let
            manifest = if crate == "." then "Cargo.toml" else "${crate}/Cargo.toml";
            lockPath = if crate == "." then src + "/Cargo.lock" else src + "/${crate}/Cargo.lock";
            vendorDir = craneLib.vendorCargoDeps { cargoLock = lockPath; };
          in
          pkgs.stdenv.mkDerivation {
            name = "${pname}-wasm";
            src = filterSrc src;
            nativeBuildInputs = [ rustToolchain ];
            buildPhase = ''
              export CARGO_HOME=$(mktemp -d)
              export CARGO_TARGET_DIR=$PWD/_target
              cp ${vendorDir}/config.toml $CARGO_HOME/config.toml
              cargo build --release --offline --target wasm32-unknown-unknown --manifest-path ${manifest}
            '';
            installPhase = ''
              mkdir -p $out
              cp _target/wasm32-unknown-unknown/release/${wasmName} $out/
            '';
          };

        # The two mesh-owned artifacts every composite is built from.
        node = buildWasm { pname = "mesh"; src = ./.; crate = "."; wasmName = "mesh.wasm"; };
        meshSystem = buildWasm { pname = "mesh-system"; src = ./.; crate = "mesh-system"; wasmName = "mesh_system.wasm"; };

        # The FIXED link graph — system→node (node.pact) + node→SM (state-machine). Identical
        # for every network; only the SM component name/wasm and the node→SM provider vary.
        composeManifest = { name, smWasm }: pkgs.writeText "${name}-compose.toml" ''
          [[component]]
          name  = "mesh-system"
          wasm  = "${meshSystem}/mesh_system.wasm"
          entry = true

          [[component]]
          name = "mesh"
          wasm = "${node}/mesh.wasm"

          [[component]]
          name = "${name}-sm"
          wasm = "${smWasm}"

          [[link]]
          consumer = "mesh-system"
          import   = "node.init"
          provider = "mesh"
          export   = "init"

          [[link]]
          consumer = "mesh-system"
          import   = "node.on-connect"
          provider = "mesh"
          export   = "on-connect"

          [[link]]
          consumer = "mesh-system"
          import   = "node.on-bytes"
          provider = "mesh"
          export   = "on-bytes"

          [[link]]
          consumer = "mesh-system"
          import   = "node.on-close"
          provider = "mesh"
          export   = "on-close"

          [[link]]
          consumer = "mesh-system"
          import   = "node.tick"
          provider = "mesh"
          export   = "tick"

          [[link]]
          consumer = "mesh-system"
          import   = "node.author"
          provider = "mesh"
          export   = "author"

          [[link]]
          consumer = "mesh-system"
          import   = "node.subscribe"
          provider = "mesh"
          export   = "subscribe"

          [[link]]
          consumer = "mesh-system"
          import   = "node.current-state"
          provider = "mesh"
          export   = "current-state"

          [[link]]
          consumer = "mesh-system"
          import   = "node.current-members"
          provider = "mesh"
          export   = "current-members"

          [[link]]
          consumer = "mesh-system"
          import   = "node.event-status"
          provider = "mesh"
          export   = "event-status"

          [[link]]
          consumer = "mesh"
          import   = "state-machine.initial-state"
          provider = "${name}-sm"
          export   = "initial-state"

          [[link]]
          consumer = "mesh"
          import   = "state-machine.validate"
          provider = "${name}-sm"
          export   = "validate"

          [[link]]
          consumer = "mesh"
          import   = "state-machine.apply"
          provider = "${name}-sm"
          export   = "apply"

          [[link]]
          consumer = "mesh"
          import   = "state-machine.members"
          provider = "${name}-sm"
          export   = "members"
        '';

        # THE external-composition entry point. Give it a network `name` and its SM `sm`
        # (a store path to the `<name>_sm.wasm`); get back `mesh_<name>.wasm` = the runnable
        # composite (system ⊕ node ⊕ SM). An impl repo calls this from its own flake.
        mkComposite = { name, sm }:
          pkgs.runCommand "mesh_${name}.wasm" { nativeBuildInputs = [ packrBin ]; } ''
            mkdir -p $out
            packr compose ${composeManifest { inherit name; smWasm = sm; }} --output $out/mesh_${name}.wasm
          '';
      in
      {
        packages = {
          default = node;
          node = node;
          mesh-system = meshSystem;

          # Demo: an in-tree SM (bank) composed via mkComposite — proves the path end to end
          # under `nix build .#demo-composite`.
          bank-sm = buildWasm { pname = "bank-sm"; src = ./.; crate = "tests/networks/bank/bank-sm"; wasmName = "bank_sm.wasm"; };
          demo-composite = mkComposite {
            name = "bank";
            sm = "${self.packages.${system}.bank-sm}/bank_sm.wasm";
          };
        };

        # Reusable across repos: `inputs.mesh.lib.${system}.mkComposite { name; sm; }`.
        lib = { inherit mkComposite buildWasm node meshSystem; };

        devShells.default = craneLib.devShell {
          packages = [
            rustToolchain
            theater.packages.${system}.default
            packrBin
            pkgs.gh
          ];
        };
      });
}
