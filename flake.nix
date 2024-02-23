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
      rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
    in
    {
      apps.${system} =
        let
          mkApp = script: path: {
            type = "app";
            program = toString (pkgs.writeShellScript "script.sh" ''
              PATH=${pkgs.lib.makeBinPath ([ rust-toolchain ] ++ path)}
              ${pkgs.lib.fileContents script}
            '');
          };
        in
        {
          clippy = mkApp ./scripts/clippy.sh [ ];
          documentation = mkApp ./scripts/documentation.sh (with pkgs; [ coreutils gnused mdbook mdbook-admonish ]);
          fmt = mkApp ./scripts/fmt.sh (with pkgs; [ git nix ]);
          test = mkApp ./scripts/test.sh (with pkgs; [ coreutils clang ]);
        };

      devShells.${system}.default = pkgs.mkShell {
        packages = [ rust-toolchain ];
      };

      formatter.${system} = pkgs.nixpkgs-fmt;
    };
}
