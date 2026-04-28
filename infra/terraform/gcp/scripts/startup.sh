#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

metadata() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" || true
}

NEXUS_CLUSTER_NAME="$(metadata nexus-cluster-name)"
NEXUS_NODE_ROLE="$(metadata nexus-node-role)"
NEXUS_NODE_INDEX="$(metadata nexus-node-index)"

apt-get update -y
apt-get install -y \
  ca-certificates \
  curl \
  git \
  gnupg \
  htop \
  jq \
  lsb-release \
  unzip

install -m 0755 -d /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
fi

. /etc/os-release
cat >/etc/apt/sources.list.d/docker.list <<EOF
deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable
EOF

apt-get update -y
apt-get install -y \
  containerd.io \
  docker-buildx-plugin \
  docker-ce \
  docker-ce-cli \
  docker-compose-plugin

systemctl enable --now docker
usermod -aG docker ubuntu || true

mkdir -p \
  /opt/nexus \
  /data/airflow \
  /data/kafka \
  /data/minio \
  /data/spark \
  /data/trino \
  /var/log/nexus

chown -R ubuntu:ubuntu /opt/nexus /data /var/log/nexus

cat >/etc/nexus-node.env <<EOF
NEXUS_CLUSTER_NAME=${NEXUS_CLUSTER_NAME}
NEXUS_NODE_ROLE=${NEXUS_NODE_ROLE}
NEXUS_NODE_INDEX=${NEXUS_NODE_INDEX}
NEXUS_HOME=/opt/nexus
NEXUS_DATA=/data
EOF

cat >/var/log/nexus/startup-complete.log <<EOF
NEXUS startup completed.
cluster=${NEXUS_CLUSTER_NAME}
role=${NEXUS_NODE_ROLE}
index=${NEXUS_NODE_INDEX}
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
