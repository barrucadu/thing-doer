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
      vm-base = import ./integration-tests/vm.nix self.packages.${system}.default;
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

      checks.${system} =
        let runTest = f: (pkgs.testers.runNixOSTest (import f { inherit pkgs; defaults = vm-base; })).config.result; in
        {
          container-networking = runTest ./integration-tests/test-container-networking.nix;
          vm-works = runTest ./integration-tests/test-vm-works.nix;
        };

      devShells.${system}.default = pkgs.mkShell {
        packages = buildDeps;
      };

      formatter.${system} = pkgs.nixpkgs-fmt;

      packages.${system} = {
        default = rustPlatform.buildRustPackage rec {
          pname = "thing-doer";
          version = "0.0.0";

          src = gitignore.lib.gitignoreSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = {
              "dns-resolver-0.1.0" = "sha256-xiKkgzH3My+8XIIpdLwcHdD1gxDj8/gY+ILZujOdq34=";
            };
          };

          nativeBuildInputs = buildDeps;
          doCheck = false;

          meta = {
            description = "A simple container orchestrator.";
            homepage = "https://github.com/barrucadu/thing-doer";
          };
        };

        vm =
          let
            vm-config = { config, lib, pkgs, ... }: {
              networking.hostName = "thing-doer";
              services.openssh.enable = true;
              services.openssh.settings.PermitRootLogin = "yes";
              users.users.root.password = "root";
              thingDoer.nodeName = null; # force randomly-generated names

              # qemu options
              virtualisation.diskImage = null;
              virtualisation.graphics = false;
              virtualisation.sharedDirectories.host = { source = "$VM_SHARED_DIR"; target = "/mnt/host"; };
              virtualisation.qemu.networkingOptions = [
                "-net nic,macaddr=52:54:00:$(head -c 1 /dev/urandom | xxd -p):$(head -c 1 /dev/urandom | xxd -p):$(head -c 1 /dev/urandom | xxd -p),model=virtio -net bridge,br=br0"
              ];
            };
            vm-system = nixpkgs.lib.nixosSystem {
              inherit system;
              modules = [
                vm-config
                vm-base
                "${nixpkgs}/nixos/modules/virtualisation/qemu-vm.nix"
              ];
            };
          in
          vm-system.config.system.build.vm;
      };
    };
}
