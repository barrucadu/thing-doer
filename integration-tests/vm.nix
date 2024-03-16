thingDoerPackage: { config, lib, pkgs, ... }:

with lib;

{
  options.thingDoer = {
    nodeName = mkOption {
      type = types.str;
      default = config.networking.hostName;
    };

    etcdHosts = mkOption {
      type = types.str;
      default = "http://127.0.0.1:2379";
    };

    clusterAddressFile = mkOption {
      type = types.str;
      default = "/tmp/cluster-address";
    };

    apid = {
      externalAddress = mkOption {
        type = types.nullOr types.str;
        default = "127.0.0.1";
      };
    };

    workerd = {
      cpuLimit = mkOption {
        type = types.int;
        default = 1;
      };
      memoryLimit = mkOption {
        type = types.int;
        default = 512;
      };
      podmanBridgeNetwork = mkOption {
        type = types.str;
        default = "flannel";
      };
    };
  };

  config = {
    # see tools/configure-podman-network.sh
    environment.sessionVariables.IN_THING_DOER_VM = "1";

    environment.systemPackages = with pkgs; [
      etcd
      flannel
      jq
      tmux
      thingDoerPackage
      (pkgs.writeShellScriptBin "configure-podman-network" ../tools/configure-podman-network.sh)
    ];

    networking.firewall.allowedTCPPorts = [ 2379 ];
    networking.firewall.allowedUDPPorts = [ 8472 ];

    virtualisation.podman.enable = true;

    services.flannel.network = "10.5.0.0/16";

    systemd.services.thing-doer-apid = {
      serviceConfig.ExecStart = "${thingDoerPackage}/bin/apid";
      environment = {
        NODE_NAME = config.thingDoer.nodeName;
        ETCD_HOSTS = config.thingDoer.etcdHosts;
        CLUSTER_ADDRESS_FILE = config.thingDoer.clusterAddressFile;
        EXTERNAL_ADDRESS = config.thingDoer.apid.externalAddress;
      };
    };

    systemd.services.thing-doer-reaperd = {
      serviceConfig.ExecStart = "${thingDoerPackage}/bin/reaperd";
      environment = {
        NODE_NAME = config.thingDoer.nodeName;
        ETCD_HOSTS = config.thingDoer.etcdHosts;
      };
    };

    systemd.services.thing-doer-schedulerd = {
      serviceConfig.ExecStart = "${thingDoerPackage}/bin/schedulerd";
      environment = {
        NODE_NAME = config.thingDoer.nodeName;
        ETCD_HOSTS = config.thingDoer.etcdHosts;
      };
    };

    systemd.services.thing-doer-workerd = {
      serviceConfig.ExecStart = "${thingDoerPackage}/bin/workerd";
      environment = {
        NODE_NAME = config.thingDoer.nodeName;
        ETCD_HOSTS = config.thingDoer.etcdHosts;
        CLUSTER_ADDRESS_FILE = config.thingDoer.clusterAddressFile;
        CPU = toString config.thingDoer.workerd.cpuLimit;
        MEMORY = toString config.thingDoer.workerd.memoryLimit;
        PODMAN_BRIDGE_NETWORK = config.thingDoer.workerd.podmanBridgeNetwork;
      };
    };
  };
}
