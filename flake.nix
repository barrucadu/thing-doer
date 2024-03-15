{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ rust-overlay.overlays.default ];
      };
      buildDeps = with pkgs; [
        coreutils
        clang
        protobuf
        (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
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
    };
}
