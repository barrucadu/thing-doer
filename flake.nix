{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, gitignore, rust-overlay }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ rust-overlay.overlays.default ];
      };
      rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      rustPlatform = pkgs.makeRustPlatform {
        cargo = rustToolchain;
        rustc = rustToolchain;
      };
      buildDeps = with pkgs; [
        coreutils
        clang
        protobuf
        rustToolchain
      ];
    in
    {
      apps.${system} =
        let
          mkApp = script: extraDeps: {
            type = "app";
            program = toString (pkgs.writeShellScript "script.sh" ''
              PATH=${pkgs.lib.makeBinPath (buildDeps ++ extraDeps)}
              ${pkgs.lib.fileContents script}
            '');
          };
        in
        {
          lint = mkApp ./scripts/lint.sh (with pkgs; [ findutils shellcheck ]);
          documentation = mkApp ./scripts/documentation.sh (with pkgs; [ gnused mdbook mdbook-admonish ]);
          fmt = mkApp ./scripts/fmt.sh (with pkgs; [ git nix ]);
          test = mkApp ./scripts/test.sh [ ];
        };

      devShells.${system}.default = pkgs.mkShell {
        packages = buildDeps;
      };

      formatter.${system} = pkgs.nixpkgs-fmt;

      packages.${system}.default = rustPlatform.buildRustPackage rec {
        pname = "thing-doer";
        version = "0.0.0";

        src = gitignore.lib.gitignoreSource ./.;

        cargoLock = {
          lockFile = ./Cargo.lock;
          outputHashes = {
            "dns-resolver-0.1.0" = "sha256-Fg8fGWdE8VylpszzCXGcQ/HBjtAl9cSwfn5t9btlWpc=";
          };
        };

        nativeBuildInputs = buildDeps;
        doCheck = false;

        meta = {
          description = "A simple container orchestrator.";
          homepage = "https://github.com/barrucadu/thing-doer";
        };
      };
    };
}
