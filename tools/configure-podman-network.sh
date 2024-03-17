#!/usr/bin/env bash

# see https://thing-doer.docs.barrucadu.co.uk/how-to/container-networking.html
# assumes flannel is configured and running

set -e

FLANNEL_SUBNET_FILE="/run/flannel/subnet.env"
PODMAN_NETWORK_NAME="flannel"
PODMAN_INTERFACE_NAME="podman_flannel"

if ! [ "$(id -u)" -eq 0 ]; then
    echo "please run as root"
    exit 1
fi

if ! command -v jq &>/dev/null; then
    echo "jq not found in the PATH"
    exit 1
fi

if ! command -v podman &>/dev/null; then
    echo "podman not found in the PATH"
    exit 1
fi

if ! [ -f "$FLANNEL_SUBNET_FILE" ]; then
    echo "${FLANNEL_SUBNET_FILE} not found - is flannel running?"
    exit 1
fi

PODMAN_NETWORK_BACKEND="$(podman info --format='{{.Host.NetworkBackend}}')"
if [[ "$PODMAN_NETWORK_BACKEND" != "netavark" ]]; then
    echo "podman network backend must be netavark, not ${PODMAN_NETWORK_BACKEND}"
    exit 1
fi

if podman network inspect "$PODMAN_NETWORK_NAME" &>/dev/null; then
    echo "podman network ${PODMAN_NETWORK_NAME} already exists, will delete"
    podman network rm "$PODMAN_NETWORK_NAME" >/dev/null
fi

# shellcheck disable=SC1090
. "$FLANNEL_SUBNET_FILE"

FLANNEL_GATEWAY="${FLANNEL_SUBNET%/*}"

# see integration-tests/node.nix
if [[ "$IN_THING_DOER_VM" == "1" ]]; then
    echo -n "$FLANNEL_GATEWAY" > "/tmp/cluster-address"
fi

podman network create --disable-dns --driver=bridge --interface-name="$PODMAN_INTERFACE_NAME" -o mtu="$FLANNEL_MTU" "$PODMAN_NETWORK_NAME" >/dev/null

NETWORK_FILE="/etc/containers/networks/${PODMAN_NETWORK_NAME}.json"
< $NETWORK_FILE jq ".subnets[0].subnet = \"${FLANNEL_SUBNET}\"" | jq ".subnets[0].gateway = \"${FLANNEL_GATEWAY}\"" > "${NETWORK_FILE}.tmp"
mv "${NETWORK_FILE}.tmp" "$NETWORK_FILE"

podman pod create --name=hold-flannel-interface --network="${PODMAN_NETWORK_NAME}" >/dev/null
INFRA_CONTAINER_ID=$(podman pod inspect hold-flannel-interface --format='{{.InfraContainerID}}')
podman start "$INFRA_CONTAINER_ID" >/dev/null

echo "flannel network: ${FLANNEL_NETWORK}"
echo "flannel subnet:  ${FLANNEL_SUBNET}"
echo "flannel gateway: ${FLANNEL_GATEWAY}"
echo "flannel mtu:     ${FLANNEL_MTU}"
echo ""
echo "sudo apid --cluster-address=${FLANNEL_GATEWAY}"
echo "sudo workerd --cluster-address=${FLANNEL_GATEWAY} --podman-bridge-network=${PODMAN_NETWORK_NAME}"
