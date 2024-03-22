# How To: Container Networking

```admonish info title="Terminology"
In this guide, "host" refers to a machine that runs `apid` or `workerd` (or both).
```

```admonish info title="Prerequisites"
- etcd is up and running.
- Every host can reach at least one etcd host.
- Every host is reachable from every other host.
- UDP port 8472 is open in the firewall of every host.
```

```admonish danger title="Needs Improvement"
Binding `apid` and `workerd` to the gateway address of the podman bridge
network is kind of a hack, and means you can't run `apid` on hosts that aren't
also running podman.
```

There are thre requirements for cluster container networking:

1. Every container in the cluster has an IP address that is reachable from each
   other container in the cluster, regardless of which physical hosts those
   containers are running on.

2. Every `apid` instance is able to listen on an address that is reachable from
   every pod.

3. Every `workerd` instance is able to listen on an address that is reachable
   from every pod.

We're going to achieve this with [flannel][], a software-defined networking tool
that maps a larger private address space onto a smaller number of physical
machines, and special podman bridge networks.

At the end of this guide, pods should be able to reach nodes and pods through
the hostnames `<name>.api.node.cluster.local`,
`<name>.worker.node.cluster.local`, and `<name>.pod.cluster.local`.

Additionally, `api.special.cluster.local` should resolve to all API nodes and
`dns.special.cluster.local` to all worker nodes.


## Configure flannel

Pick a subnet which doesn't overlap with your out-of-cluster network and write
the flannel network configuration to etcd.

For example, this assigns the network `10.5.0.0/16` to flannel:

```bash
ETCDCTL_API=3 etcdctl put /coreos.com/network/config '{ "Network": "10.5.0.0/16", "Backend": {"Type": "vxlan"}}'
```

Then, on each host, install and run flannel:

```bash
sudo flannel
```

The flannel process will automatically claim a subnet of the overall network
space, and add the appropriate routes to the host's network configuration.


## Configure podman (automated)

Install `jq` and run the provided script:

```bash
sudo ./tools/configure-podman-network.sh
```

It will print out the network configuration and the commands to start `apid` and
`workerd`, for example:

```
flannel network: 10.5.0.0/16
flannel subnet:  10.5.38.1/24
flannel gateway: 10.5.38.1
flannel mtu:     1450

sudo apid --cluster-address=10.5.38.1
sudo workerd --cluster-address=10.5.38.1 --podman-bridge-network=flannel
```

Run those commands in separate terminals.

`apid` will start an HTTP server on port 80 TCP to serve the cluster.  If you
want it to also be available on a different, out-of-cluster, address, see the
`--external-address` argument.

`workerd` will start a DNS server on ports 53 UDP and TCP to serve DNS to its
pods, and to the cluster more generally if needed.

Now you have a working host!


## Configure podman (manual)

You can also manually configure the network.  This section explains all the
steps involved, and may be a useful guide to understanding the script.

### Create the podman network

Check the generated configuration for the flannel network

```bash
cat /run/flannel/subnet.env
```

Which gives something like:

```
FLANNEL_NETWORK=10.5.0.0/16
FLANNEL_SUBNET=10.5.72.1/24
FLANNEL_MTU=1450
FLANNEL_IPMASQ=false
```

Each host will have the same `FLANNEL_NETWORK` but a different `FLANNEL_SUBNET`.

Create a podman network which we will later associate with the flannel subnet:

```bash
sudo podman network create --disable-dns --driver=bridge --interface-name=podman_flannel -o mtu=$FLANNEL_MTU flannel
```

The subnet can't be given to the `podman network create` command as it doesn't
allow creating a network with a subnet that overlaps with one already in use.

Edit the podman network config file directly to use the flannel subnet and
gateway (the gateway is `10.5.72.1` in this example):

```bash
sudo nano /etc/containers/networks/flannel.json
```

### Bring up the network interface

The `podman_flannel` bridge network interface won't get created until the first
container using it starts, so create a pod:

```bash
sudo podman pod create --name=hold-flannel-interface --network=flannel
```

Then get the `InfraContainerID` of that pod:

```bash
sudo podman pod inspect hold-flannel-interface
```

Finally, start it:

```bash
sudo podman start $INFRA_CONTAINER_ID
```

### Start apid and workerd

Both `apid` and `workerd` will listen on the flannel gateway IP (`10.5.72.1` in
the running example).

#### apid

Pass the flannel gateway IP to the `apid` command:

```bash
sudo apid --cluster-address=$FLANNEL_GATEWAY
```

`apid` will start an HTTP server on port 80 TCP to serve the cluster.  If you
want it to also be available on a different, out-of-cluster, address, see the
`--external-address` argument.

#### workerd

Pass the flannel gateway IP and the podman network name to the `workerd`
command:

```bash
sudo workerd --cluster-address=$FLANNEL_GATEWAY --podman-bridge-network=flannel
```

`workerd` will start a DNS server on ports 53 UDP and TCP to serve DNS to its
pods, and to the cluster more generally if needed.


## Troubleshooting

You can sanity check the DNS configuration by launching a container on the
podman network and confirming that it can resolve both local and external domain
names:

```
$ podman run -it --rm --network=flannel --dns=$FLANNEL_GATEWAY ubuntu
# apt-get update -y
# apt-get install -y dnsutils
# dig $NAME.node.cluster.local
# dig $NAME.pod.cluster.local
# dig www.barrucadu.co.uk
```

### flannel cannot connect to etcd

Set the `-etcd-endpoints` argument if etcd is not running on the same host, and
ensure the etcd port is open in the firewall.

### apid or workerd fails to start with `AddrNotAvailable`

Check that the `--cluster-address` parameter is the flannel gateway address, and
that the podman interface exists in `ip link list`.

If the interface doesn't exist, bring it up again.

### apid fails to start with `AddrInUse`

Check you don't have anything else listening on port 80 (TCP) already.

### workerd fails to start with `AddrInUse`

Check you don't have anything else listening on port 53 (UDP or TCP) already.

### Cluster port mapping is not working

Ensure you have the `br_netfilter` module loaded:

```bash
sudo modprobe br_netfilter
```

### Containers on different hosts cannot reach each other

Open UDP port `8472` in the firewalls.

[flannel]: https://github.com/flannel-io/flannel/
