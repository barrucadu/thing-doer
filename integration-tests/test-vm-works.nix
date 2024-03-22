{ defaults, ... }: {
  inherit defaults;

  name = "VM can start all services";

  nodes = {
    machine = {
      services.etcd.enable = true;
      services.flannel.enable = true;
    };
  };

  testScript = ''
    NODE_TYPES = ["api", "reaper", "scheduler", "worker"]
    NODE_NAME = "machine"

    machine.wait_for_file("/run/flannel/subnet.env")
    machine.succeed("configure-podman-network")

    for ty in NODE_TYPES:
        machine.systemctl(f"start thing-doer-{ty}d")

    for ty in NODE_TYPES:
        machine.wait_for_unit(f"thing-doer-{ty}d")

    for ty in NODE_TYPES:
        machine.succeed(f"curl --fail-with-body http://127.0.0.1/resources/node.{ty}/{NODE_NAME}")

    machine.succeed("curl --fail-with-body http://127.0.0.1/dns_aliases/special/api/api.node/machine")
    machine.succeed("curl --fail-with-body http://127.0.0.1/dns_aliases/special/dns/worker.node/machine")
  '';
}
