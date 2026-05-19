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

BOOTSTRAP_USER="ubuntu"

install_master_worker_ssh() {
  if [ "${NEXUS_NODE_ROLE}" != "master" ]; then
    return 0
  fi

  local private_key_b64
  private_key_b64="$(metadata_attr nexus-master-worker-private-key-b64)"
  if [ -z "${private_key_b64}" ]; then
    return 0
  fi

  install -m 0700 -d "/home/${BOOTSTRAP_USER}/.ssh"
  printf "%s" "${private_key_b64}" \
    | base64 -d >"/home/${BOOTSTRAP_USER}/.ssh/id_ed25519_nexus_cluster"
  chmod 0600 "/home/${BOOTSTRAP_USER}/.ssh/id_ed25519_nexus_cluster"

  cat >"/home/${BOOTSTRAP_USER}/.ssh/config" <<EOF
Host ${NEXUS_CLUSTER_NAME}-worker-* 10.*
  User ${BOOTSTRAP_USER}
  IdentityFile ~/.ssh/id_ed25519_nexus_cluster
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
  chmod 0600 "/home/${BOOTSTRAP_USER}/.ssh/config"
  chown -R "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" "/home/${BOOTSTRAP_USER}/.ssh"
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

  chown -R "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" "${target_dir}"
  git config --system --add safe.directory "${target_dir}" || true
  git -C "${target_dir}" remote set-url origin "${repo_url}"
  git -C "${target_dir}" fetch origin --tags --prune
  if git -C "${target_dir}" show-ref --verify --quiet "refs/remotes/origin/${repo_ref}"; then
    git -C "${target_dir}" checkout -B "${repo_ref}" "origin/${repo_ref}"
    git -C "${target_dir}" reset --hard "origin/${repo_ref}"
  else
    git -C "${target_dir}" checkout --detach "${repo_ref}"
  fi
  chown -R "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" "${target_dir}"
}

setup_docker_forwarding() {
  local sysctl_file="/etc/sysctl.d/98-nexus-docker-forward.conf"

  sysctl -w net.ipv4.ip_forward=1
  printf "net.ipv4.ip_forward=1\n" >"${sysctl_file}"
  sysctl --system >/dev/null
}

install_docker() {
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
  usermod -aG docker "${BOOTSTRAP_USER}" || true
}

configure_password_login() {
  if [ "${SSH_PASSWORD_LOGIN}" != "TRUE" ] || [ -z "${SSH_PASSWORD}" ] || [ -z "${SSH_USER}" ]; then
    return 0
  fi

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
}

write_nexus_helpers() {
  if [ "${NEXUS_NODE_ROLE}" != "master" ]; then
    return 0
  fi

  cat >/usr/local/bin/start-nexus-compose <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

if [ "${NEXUS_NODE_ROLE}" != "master" ]; then
  echo "Run this command on the master VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

cd "${NEXUS_APP_DIR}"
if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
fi

${DOCKER} compose --env-file .env -f infra/docker/docker-compose.yml up -d --build
${DOCKER} compose --env-file .env -f infra/docker/docker-compose.yml ps
EOF
  chmod 0755 /usr/local/bin/start-nexus-compose

  cat >/usr/local/bin/stop-nexus-compose <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

cd "${NEXUS_APP_DIR}"
${DOCKER} compose --env-file .env -f infra/docker/docker-compose.yml down
EOF
  chmod 0755 /usr/local/bin/stop-nexus-compose
}

NEXUS_CLUSTER_NAME="$(metadata_attr nexus-cluster-name)"
NEXUS_WORKER_COUNT="$(metadata_attr nexus-worker-count)"
NEXUS_NODE_ROLE="$(metadata_attr nexus-node-role)"
NEXUS_NODE_INDEX="$(metadata_attr nexus-node-index)"
NEXUS_NODE_NAME="$(metadata_instance name)"
NEXUS_NODE_IP="$(metadata_instance network-interfaces/0/ip)"
NEXUS_REPO_URL="$(metadata_attr nexus-repo-url)"
NEXUS_REPO_REF="$(metadata_attr nexus-repo-ref)"
SSH_PASSWORD_LOGIN="$(metadata_attr ssh-password-login)"
SSH_PASSWORD="$(metadata_attr ssh-password)"
SSH_USER="$(metadata_attr ssh-user)"

if [ -n "${SSH_USER}" ]; then
  BOOTSTRAP_USER="${SSH_USER}"
fi
if [ -z "${NEXUS_WORKER_COUNT}" ]; then
  NEXUS_WORKER_COUNT="4"
fi

NEXUS_HOME="/opt/nexus"
NEXUS_APP_DIR="${NEXUS_HOME}/nexus"
NEXUS_DATA="/data"

setup_docker_forwarding
install_master_worker_ssh
install_docker
configure_password_login

mkdir -p \
  "${NEXUS_HOME}" \
  "${NEXUS_DATA}/airflow" \
  "${NEXUS_DATA}/kafka" \
  "${NEXUS_DATA}/minio" \
  "${NEXUS_DATA}/postgres" \
  "${NEXUS_DATA}/spark" \
  "${NEXUS_DATA}/trino" \
  /var/log/nexus

chown -R "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" "${NEXUS_HOME}" "${NEXUS_DATA}" /var/log/nexus
chown -R 999:999 "${NEXUS_DATA}/postgres"

cat >/etc/nexus-node.env <<EOF
NEXUS_CLUSTER_NAME=${NEXUS_CLUSTER_NAME}
NEXUS_NODE_ROLE=${NEXUS_NODE_ROLE}
NEXUS_NODE_INDEX=${NEXUS_NODE_INDEX}
NEXUS_NODE_NAME=${NEXUS_NODE_NAME}
NEXUS_NODE_IP=${NEXUS_NODE_IP}
NEXUS_WORKER_COUNT=${NEXUS_WORKER_COUNT}
NEXUS_HOME=${NEXUS_HOME}
NEXUS_APP_DIR=${NEXUS_APP_DIR}
NEXUS_DATA=${NEXUS_DATA}
EOF
chmod 0644 /etc/nexus-node.env

sync_git_repo "${NEXUS_REPO_URL}" "${NEXUS_REPO_REF}" "${NEXUS_APP_DIR}"
write_nexus_helpers

cat >/var/log/nexus/startup-complete.log <<EOF
NEXUS startup completed.
cluster=${NEXUS_CLUSTER_NAME}
role=${NEXUS_NODE_ROLE}
index=${NEXUS_NODE_INDEX}
name=${NEXUS_NODE_NAME}
private_ip=${NEXUS_NODE_IP}
nexus_repo_url=${NEXUS_REPO_URL}
nexus_repo_ref=${NEXUS_REPO_REF}
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
