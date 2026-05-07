#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

metadata_attr() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" || true
}

metadata_instance() {
  local path="$1"
  curl -fsS -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/${path}" || true
}

sync_git_repo() {
  local repo_url="$1"
  local repo_ref="$2"
  local target_dir="$3"

  if [ -z "${repo_url}" ]; then
    return 0
  fi

  if [ ! -d "${target_dir}/.git" ]; then
    rm -rf "${target_dir}"
    git clone "${repo_url}" "${target_dir}"
  fi

  chown -R ubuntu:ubuntu "${target_dir}"
  git config --global --add safe.directory "${target_dir}"
  git -C "${target_dir}" remote set-url origin "${repo_url}"
  git -C "${target_dir}" fetch origin --tags --prune
  if git -C "${target_dir}" show-ref --verify --quiet "refs/remotes/origin/${repo_ref}"; then
    git -C "${target_dir}" checkout -B "${repo_ref}" "origin/${repo_ref}"
    git -C "${target_dir}" reset --hard "origin/${repo_ref}"
  else
    git -C "${target_dir}" checkout --detach "${repo_ref}"
  fi
  chown -R ubuntu:ubuntu "${target_dir}"
}

NEXUS_CLUSTER_NAME="$(metadata_attr nexus-cluster-name)"
NEXUS_NODE_ROLE="$(metadata_attr nexus-node-role)"
NEXUS_NODE_INDEX="$(metadata_attr nexus-node-index)"
NEXUS_NODE_NAME="$(metadata_instance name)"
NEXUS_NODE_IP="$(metadata_instance network-interfaces/0/ip)"
NEXUS_REPO_URL="$(metadata_attr nexus-repo-url)"
NEXUS_REPO_REF="$(metadata_attr nexus-repo-ref)"
DOCKER_ELK_REPO_URL="$(metadata_attr docker-elk-repo-url)"
DOCKER_ELK_REPO_REF="$(metadata_attr docker-elk-repo-ref)"
SSH_PASSWORD_LOGIN="$(metadata_attr ssh-password-login)"
SSH_PASSWORD="$(metadata_attr ssh-password)"
SSH_USER="$(metadata_attr ssh-user)"
NEXUS_APP_DIR="/opt/nexus/nexus"
DOCKER_ELK_APP_DIR="/opt/nexus/docker-elk"

if [ "${NEXUS_NODE_ROLE}" = "master" ]; then
  NEXUS_NODE_ROLES="master"
else
  NEXUS_NODE_ROLES="data,ingest"
fi

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

if [ "${SSH_PASSWORD_LOGIN}" = "TRUE" ] && [ -n "${SSH_PASSWORD}" ] && [ -n "${SSH_USER}" ]; then
  echo "${SSH_USER}:${SSH_PASSWORD}" | chpasswd
  install -m 0755 -d /etc/ssh/sshd_config.d
  sed -i 's/^[[:space:]]*PasswordAuthentication[[:space:]].*/# managed by nexus startup: &/' /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf 2>/dev/null || true
  sed -i 's/^[[:space:]]*KbdInteractiveAuthentication[[:space:]].*/# managed by nexus startup: &/' /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf 2>/dev/null || true
  sed -i 's/^[[:space:]]*ChallengeResponseAuthentication[[:space:]].*/# managed by nexus startup: &/' /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf 2>/dev/null || true
  cat >/etc/ssh/sshd_config.d/99-nexus-password-login.conf <<EOF
PasswordAuthentication yes
KbdInteractiveAuthentication yes
ChallengeResponseAuthentication yes
UsePAM yes
EOF
  systemctl restart ssh || systemctl restart sshd
fi

mkdir -p \
  /opt/nexus \
  /data/airflow \
  /data/elasticsearch \
  /data/kafka \
  /data/minio \
  /data/postgres \
  /data/spark \
  /data/trino \
  /var/log/nexus

chown -R ubuntu:ubuntu /opt/nexus /data /var/log/nexus
chown -R 1000:1000 /data/elasticsearch
chown -R 999:999 /data/postgres

cat >/etc/nexus-node.env <<EOF
NEXUS_CLUSTER_NAME=${NEXUS_CLUSTER_NAME}
NEXUS_NODE_ROLE=${NEXUS_NODE_ROLE}
NEXUS_NODE_INDEX=${NEXUS_NODE_INDEX}
NEXUS_NODE_NAME=${NEXUS_NODE_NAME}
NEXUS_NODE_IP=${NEXUS_NODE_IP}
NEXUS_HOME=/opt/nexus
NEXUS_DATA=/data
EOF

cat >/etc/nexus-elastic.env <<EOF
NEXUS_NODE_NAME=${NEXUS_NODE_NAME}
NEXUS_NODE_IP=${NEXUS_NODE_IP}
NEXUS_NODE_ROLES=${NEXUS_NODE_ROLES}
ES_JAVA_OPTS=-Xms4g -Xmx4g
EOF

chmod 0644 /etc/nexus-node.env /etc/nexus-elastic.env

sync_git_repo "${NEXUS_REPO_URL}" "${NEXUS_REPO_REF}" "${NEXUS_APP_DIR}"
sync_git_repo "${DOCKER_ELK_REPO_URL}" "${DOCKER_ELK_REPO_REF}" "${DOCKER_ELK_APP_DIR}"

cat >/var/log/nexus/startup-complete.log <<EOF
NEXUS startup completed.
cluster=${NEXUS_CLUSTER_NAME}
role=${NEXUS_NODE_ROLE}
index=${NEXUS_NODE_INDEX}
name=${NEXUS_NODE_NAME}
private_ip=${NEXUS_NODE_IP}
nexus_repo_url=${NEXUS_REPO_URL}
nexus_repo_ref=${NEXUS_REPO_REF}
docker_elk_repo_url=${DOCKER_ELK_REPO_URL}
docker_elk_repo_ref=${DOCKER_ELK_REPO_REF}
ssh_password_login=${SSH_PASSWORD_LOGIN}
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
