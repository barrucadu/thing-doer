# Start four pods across two VMs
#
# - node1: running nginx on port 80 and a curl pod
# - node2: running nginx on port 8080 and a curl pod
#
# Then check that the curl pods can reach both nginx instances as well as the
# API server via their hostnames.
{ defaults, pkgs }:
let
  nginxContainerTemplate = variant: {
    name = "infra:5000/thing-doer-nginx-${variant}";
    tag = "latest";

    copyToRoot = pkgs.buildEnv {
      name = "root";
      paths = [ pkgs.nginx ];
      pathsToLink = [ "/bin" ];
    };

    extraCommands = ''
      mkdir -p var/log/nginx
      mkdir -p var/cache/nginx
    '';

    runAsRoot = ''
      #!${pkgs.stdenv.shell}
      ${pkgs.dockerTools.shadowSetup}
      groupadd --system nginx
      useradd --system --gid nginx nginx
    '';

    config =
      let
        root = pkgs.writeTextDir "index.html" "I am the '${variant}' nginx container!";
        config = pkgs.writeText "nginx.conf" ''
          user nginx nginx;
          daemon off;
          error_log /dev/stdout info;
          pid /dev/null;
          events {}
          http {
            access_log /dev/stdout;
            server {
              listen 80;
              index index.html;
              location / {
                root ${root};
              }
            }
          }
        '';
      in
      {
        Cmd = [ "/bin/nginx" "-c" config ];
        ExposedPorts = { "80/tcp" = { }; };
      };
  };

  nginxContainerFoo = nginxContainerTemplate "foo";
  nginxContainerBar = nginxContainerTemplate "bar";

  curlContainer = {
    name = "infra:5000/thing-doer-curl";
    tag = "latest";

    copyToRoot = pkgs.buildEnv {
      name = "root";
      paths = [ pkgs.bash pkgs.curl ];
      pathsToLink = [ "/bin" "/etc" ];
    };

    config = { Cmd = [ "/bin/bash" ]; };
  };
in
{
  name = "Pods on different nodes can communicate";

  defaults = pkgs.lib.mkMerge [
    defaults
    {
      services.flannel.enable = true;
      services.flannel.iface = "eth1";
      services.flannel.etcd.endpoints = [ "http://infra:2379" ];

      thingDoer.etcdEndpoints = "http://infra:2379";

      virtualisation.containers.registries.insecure = [ "infra:5000" ];
    }
  ];

  nodes = {
    infra = {
      services.etcd.enable = true;
      services.etcd.listenClientUrls = [ "http://0.0.0.0:2379" ];
      services.etcd.advertiseClientUrls = [ "http://infra:2379" ];

      services.dockerRegistry.enable = true;
      services.dockerRegistry.listenAddress = "0.0.0.0";

      virtualisation.podman.enable = true;
      virtualisation.containers.registries.insecure = [ "infra:5000" ];

      networking.firewall.allowedTCPPorts = [ 2379 5000 ];

      # podman will nondetermistically fail to load the images with the
      # default disk size of 1024.
      virtualisation.diskSize = 2048;
    };
    node1 = { };
    node2 = { };
  };

  testScript = ''
    import json

    start_all()

    # push docker containers to registry
    infra.wait_for_unit("docker-registry")
    infra.succeed("podman load < ${pkgs.dockerTools.buildImage nginxContainerFoo}")
    infra.succeed("podman load < ${pkgs.dockerTools.buildImage nginxContainerBar}")
    infra.succeed("podman load < ${pkgs.dockerTools.buildImage curlContainer}")
    infra.succeed("podman push ${nginxContainerFoo.name}")
    infra.succeed("podman push ${nginxContainerBar.name}")
    infra.succeed("podman push ${curlContainer.name}")

    # wait for the network
    for node in [infra, node1, node2]:
      node.wait_for_file("/run/flannel/subnet.env")
      node.succeed("configure-podman-network")

    # start apid and schedulerd on `infra`, workerd on `node1` and `node2`
    infra.systemctl("start thing-doer-apid")
    infra.systemctl("start thing-doer-schedulerd")
    node1.systemctl("start thing-doer-workerd")
    node2.systemctl("start thing-doer-workerd")

    infra.wait_for_unit("thing-doer-apid")
    infra.wait_for_unit("thing-doer-schedulerd")
    node1.wait_for_unit("thing-doer-workerd")
    node2.wait_for_unit("thing-doer-workerd")

    # create pods - nginx:80 on `node1`, nginx:8080 on `node2`, curl on both
    nginx_pod_on_port_80 = json.dumps({
        "name": "nginx-80",
        "type": "pod",
        "state": "created",
        "metadata": {},
        "spec": {
            "containers": [
                {
                    "name": "web",
                    "image": "${nginxContainerFoo.name}",
                }
            ],
            "ports": [80],
            "schedulingConstraints": {
                "mayBeScheduledOn": ["node1"],
            }
        }
    })
    nginx_pod_on_port_8080 = json.dumps({
        "name": "nginx-8080",
        "type": "pod",
        "state": "created",
        "metadata": {},
        "spec": {
            "containers": [
                {
                    "name": "web",
                    "image": "${nginxContainerBar.name}",
                }
            ],
            "ports": [{ "container": 80, "cluster": 8080 }],
            "schedulingConstraints": {
                "mayBeScheduledOn": ["node2"],
            }
        }
    })
    test_pod_on_node1 = json.dumps({
        "name": "test1",
        "type": "pod",
        "state": "created",
        "metadata": {},
        "spec": {
            "containers": [
                {
                    "name": "curl",
                    "image": "${curlContainer.name}",
                }
            ],
            "schedulingConstraints": {
                "mayBeScheduledOn": ["node1"],
            }
        }
    })
    test_pod_on_node2 = json.dumps({
        "name": "test2",
        "type": "pod",
        "state": "created",
        "metadata": {},
        "spec": {
            "containers": [
                {
                    "name": "curl",
                    "image": "${curlContainer.name}",
                }
            ],
            "schedulingConstraints": {
                "mayBeScheduledOn": ["node2"],
            }
        }
    })

    infra.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{nginx_pod_on_port_80}' http://127.0.0.1/resources")
    infra.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{nginx_pod_on_port_8080}' http://127.0.0.1/resources")
    infra.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{test_pod_on_node1}' http://127.0.0.1/resources")
    infra.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{test_pod_on_node2}' http://127.0.0.1/resources")

    # wait for pods to start
    node1.wait_until_succeeds("podman ps | grep nginx-80-web")
    node2.wait_until_succeeds("podman ps | grep nginx-8080-web")

    node1.wait_until_succeeds("podman ps | grep test1-curl")
    node2.wait_until_succeeds("podman ps | grep test2-curl")

    # curl on `node1` can communicate with everything
    node1.succeed("podman exec node1-test1-curl /bin/curl --fail-with-body http://api.special.cluster.local/resources/pod")
    node1.succeed("podman exec node1-test1-curl /bin/curl --fail-with-body http://nginx-80.pod.cluster.local | grep foo")
    node1.succeed("podman exec node1-test1-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local:8080 | grep bar")

    # curl on `node2` can communicate with everything
    node2.succeed("podman exec node2-test2-curl /bin/curl --fail-with-body http://api.special.cluster.local/resources/pod")
    node2.succeed("podman exec node2-test2-curl /bin/curl --fail-with-body http://nginx-80.pod.cluster.local | grep foo")
    node2.succeed("podman exec node2-test2-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local:8080 | grep bar")

    # TODO - these do not work yet, change `succeed` to `fail` once filter rules are in place
    # nginx-8080 is not accessible over port 80
    node1.succeed("podman exec node1-test1-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local")
    node2.succeed("podman exec node2-test2-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local")
  '';
}
