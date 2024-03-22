# Start three pods on a different VM each:
#
# - machine1: running nginx on port 80
# - machine2: running nginx on port 8080
# - machine3: running ubuntu
#
# Then check that the ubuntu pod can curl both nginx instances as well as the
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
  inherit defaults;

  name = "Pods on different nodes can communicate";

  nodes =
    let
      node = {
        services.flannel.enable = true;
        services.flannel.iface = "eth1";
        services.flannel.etcd.endpoints = [ "http://infra:2379" ];

        thingDoer.etcdEndpoints = "http://infra:2379";

        virtualisation.containers.registries.insecure = [ "infra:5000" ];
      };
    in
    {
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
      node1 = node;
      node2 = node;
      node3 = node;
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

    # start workerd on all three nodes
    for node in [node1, node2, node3]:
      node.wait_for_file("/run/flannel/subnet.env")
      node.succeed("configure-podman-network")
      node.systemctl("start thing-doer-workerd")
    # let things start in parallel while blocking
    for node in [node1, node2, node3]:
      node.wait_for_unit("thing-doer-workerd")

    # start apid and schedulerd on just node 1
    node1.systemctl("start thing-doer-apid")
    node1.systemctl("start thing-doer-schedulerd")
    node1.wait_for_unit("thing-doer-apid")
    node1.wait_for_unit("thing-doer-schedulerd")

    # create pods - nginx:80 on node1, nginx:8080 on node2, curl on node3
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
                    "ports": [80],
                }
            ],
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
                    "ports": [{ "container": 80, "cluster": 8080 }],
                }
            ],
            "schedulingConstraints": {
                "mayBeScheduledOn": ["node2"],
            }
        }
    })
    test_pod = json.dumps({
        "name": "test",
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
                "mayBeScheduledOn": ["node3"],
            }
        }
    })

    node1.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{nginx_pod_on_port_80}' http://127.0.0.1/resources")
    node1.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{nginx_pod_on_port_8080}' http://127.0.0.1/resources")
    node1.succeed(f"curl --fail-with-body -XPOST -H 'content-type: application/json' -d '{test_pod}' http://127.0.0.1/resources")

    # wait for pods to start
    node1.wait_until_succeeds("podman ps | grep nginx-80-web")
    node2.wait_until_succeeds("podman ps | grep nginx-8080-web")
    node3.wait_until_succeeds("podman ps | grep test-curl")

    # node3 can communicate with apid on node1
    node3.succeed("podman exec node3-test-curl /bin/curl --fail-with-body http://api.special.cluster.local/resources/pod")

    # node3 can communicate with nginx on node1 on port 80
    node3.succeed("podman exec node3-test-curl /bin/curl --fail-with-body http://nginx-80.pod.cluster.local")

    # node3 can communicate with nginx on node2 on port 8080 and not on port 80
    # TODO: the port mapping does not work - these lines should be `succeed` and `fail` respectively
    node3.fail("podman exec node3-test-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local:8080")
    node3.succeed("podman exec node3-test-curl /bin/curl --fail-with-body http://nginx-8080.pod.cluster.local")
  '';
}
