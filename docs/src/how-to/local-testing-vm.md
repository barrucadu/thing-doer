# How To: Local Testing VM

```admonish info title="Prerequisites"
- You are running NixOS.
- You have a bridge network interface called `br0`, which is connected to a DHCP server.
```

```admonish danger title="Idiosyncratic"
This works for me, and might not work for you.
```

The `flake.nix` defines a virtual machine with etcd, flannel, and thing-doer
pre-installed.

Build and run it with:

```bash
nix build .#vm
sudo env VM_SHARED_DIR=$(pwd) ./result/bin/run-thing-doer-vm
```

This boots the VM and gives you a console.  Log in as the `root` user, with the
password `root`.  SSH is also available.

The current directory is mounted to `/mnt/host` inside the VM.

The VM will attach to the bridge network interface `br0` with a random MAC
address, so you can run multiple instances of the VM and they will all be
assigned unique IP addresses

## Bring up the etcd cluster

If you're using a single VM for testing, just run etcd:

```bash
etcd
```

Otherwise, follow [the etcd clustering guide][].

## Bring up the thing-doer cluster

Follow [How To: Container Networking][].  The automated script is available in
the PATH as `configure-podman-network`.

Then start the servers.  The binaries are in your PATH.  There are also systemd
units if the default behaviour is good enough:

```bash
systemctl start thing-doer-apid
systemctl start thing-doer-reaperd
systemctl start thing-doer-schedulerd
systemctl start thing-doer-workerd
```

[the etcd clustering guide]: https://etcd.io/docs/v3.5/op-guide/clustering/
[How To: Container Networking]: ./container-networking.html
