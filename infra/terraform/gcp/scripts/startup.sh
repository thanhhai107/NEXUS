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
  git config --system --add safe.directory "${target_dir}"
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

write_search_demo_config() {
  if [ ! -d "${DOCKER_ELK_APP_DIR}" ]; then
    return 0
  fi

  cat >"${DOCKER_ELK_APP_DIR}/.env" <<EOF
ELASTIC_VERSION=8.17.0
ES_JAVA_OPTS=-Xms1g -Xmx1g
ES_CLUSTER_NAME=amazon-search

POSTGRES_DB=amazon_search
POSTGRES_USER=search
POSTGRES_PASSWORD=search_demo

MEILI_MASTER_KEY=masterKey

ELASTIC_SEMANTIC_INFERENCE_ID=my-elser-endpoint
EOF
  chown "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" "${DOCKER_ELK_APP_DIR}/.env"
  chmod 0600 "${DOCKER_ELK_APP_DIR}/.env"

  cat >/etc/amazon-search-demo.env <<EOF
AMAZON_SEARCH_DEMO_DIR=${DOCKER_ELK_APP_DIR}
AMAZON_SEARCH_STREAMLIT_URL=http://${NEXUS_NODE_IP}:8501
AMAZON_SEARCH_FASTAPI_URL=http://${NEXUS_NODE_IP}:8000
AMAZON_SEARCH_KIBANA_URL=http://${NEXUS_NODE_IP}:5601
EOF
  chmod 0644 /etc/amazon-search-demo.env

  cat >/usr/local/bin/start-amazon-search-elasticsearch <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/amazon-search-demo.env

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

cd "${AMAZON_SEARCH_DEMO_DIR}"
${DOCKER} compose --env-file .env --env-file /etc/nexus-elastic.env up -d elasticsearch
${DOCKER} compose --env-file .env --env-file /etc/nexus-elastic.env ps elasticsearch
EOF
  chmod 0755 /usr/local/bin/start-amazon-search-elasticsearch

  if [ "${NEXUS_NODE_ROLE}" = "master" ]; then
    cat >/usr/local/bin/start-amazon-search-elasticsearch-cluster <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env
. /etc/amazon-search-demo.env

if [ "${NEXUS_NODE_ROLE}" != "master" ]; then
  echo "Run this command on the master VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

for i in $(seq 1 "${NEXUS_WORKER_COUNT:-4}"); do
  worker="${NEXUS_CLUSTER_NAME}-worker-${i}"
  echo "Starting Elasticsearch on ${worker}..."
  ssh -o BatchMode=yes "${worker}" "start-amazon-search-elasticsearch" &
done
wait

echo "Starting Elasticsearch on ${NEXUS_NODE_NAME}..."
cd "${AMAZON_SEARCH_DEMO_DIR}"
${DOCKER} compose --env-file .env --env-file /etc/nexus-elastic.env up -d elasticsearch
${DOCKER} compose --env-file .env --env-file /etc/nexus-elastic.env ps elasticsearch
EOF
    chmod 0755 /usr/local/bin/start-amazon-search-elasticsearch-cluster

    cat >/usr/local/bin/start-demo <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env
. /etc/amazon-search-demo.env

if [ "${NEXUS_NODE_ROLE}" != "master" ]; then
  echo "Run this command on the master VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

start-amazon-search-elasticsearch-cluster

cd "${AMAZON_SEARCH_DEMO_DIR}"
${DOCKER} compose --env-file .env --env-file /etc/nexus-elastic.env up -d --build postgres meilisearch elasticsearch kibana backend frontend

cat <<URLS

Amazon Search demo is starting:
  Streamlit: http://$(curl -fsS -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip):8501
  FastAPI:   http://$(curl -fsS -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip):8000/docs
  Kibana:    http://$(curl -fsS -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip):5601

Ingest is not run automatically. To ingest data, run:
  cd ${AMAZON_SEARCH_DEMO_DIR}
  docker compose exec -T backend python scripts/ingest_all.py --reset
URLS
EOF
    chmod 0755 /usr/local/bin/start-demo
  fi
}

setup_elasticsearch_host() {
  local required_max_map_count="262144"
  local sysctl_file="/etc/sysctl.d/99-elasticsearch.conf"
  local current_value

  current_value="$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)"
  if [ "${current_value}" -lt "${required_max_map_count}" ]; then
    sysctl -w "vm.max_map_count=${required_max_map_count}"
  fi

  if [ ! -f "${sysctl_file}" ] || ! grep -q "^vm.max_map_count=${required_max_map_count}$" "${sysctl_file}"; then
    printf "vm.max_map_count=%s\n" "${required_max_map_count}" >"${sysctl_file}"
  fi

  sysctl --system >/dev/null
}

setup_docker_forwarding() {
  local sysctl_file="/etc/sysctl.d/98-nexus-docker-forward.conf"

  sysctl -w net.ipv4.ip_forward=1
  printf "net.ipv4.ip_forward=1\n" >"${sysctl_file}"
  sysctl --system >/dev/null
}

NEXUS_CLUSTER_NAME="$(metadata_attr nexus-cluster-name)"
NEXUS_WORKER_COUNT="$(metadata_attr nexus-worker-count)"
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
if [ -n "${SSH_USER}" ]; then
  BOOTSTRAP_USER="${SSH_USER}"
fi
NEXUS_APP_DIR="/opt/nexus/nexus"
DOCKER_ELK_APP_DIR="/opt/nexus/docker-elk"

if [ -z "${NEXUS_WORKER_COUNT}" ]; then
  NEXUS_WORKER_COUNT="4"
fi

if [ "${NEXUS_NODE_ROLE}" = "master" ]; then
  NEXUS_NODE_ROLES="master"
elif [ "${NEXUS_NODE_INDEX}" = "1" ]; then
  NEXUS_NODE_ROLES="data,ingest,ml"
else
  NEXUS_NODE_ROLES="data,ingest"
fi

ES_SEED_HOSTS="${NEXUS_CLUSTER_NAME}-master-1"
for i in $(seq 1 "${NEXUS_WORKER_COUNT}"); do
  ES_SEED_HOSTS="${ES_SEED_HOSTS},${NEXUS_CLUSTER_NAME}-worker-${i}"
done

setup_elasticsearch_host
setup_docker_forwarding
install_master_worker_ssh

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

chown -R "${BOOTSTRAP_USER}:${BOOTSTRAP_USER}" /opt/nexus /data /var/log/nexus
chown -R 1000:1000 /data/elasticsearch
chown -R 999:999 /data/postgres

cat >/etc/nexus-node.env <<EOF
NEXUS_CLUSTER_NAME=${NEXUS_CLUSTER_NAME}
NEXUS_NODE_ROLE=${NEXUS_NODE_ROLE}
NEXUS_NODE_INDEX=${NEXUS_NODE_INDEX}
NEXUS_NODE_NAME=${NEXUS_NODE_NAME}
NEXUS_NODE_IP=${NEXUS_NODE_IP}
NEXUS_WORKER_COUNT=${NEXUS_WORKER_COUNT}
NEXUS_HOME=/opt/nexus
NEXUS_DATA=/data
EOF

cat >/etc/nexus-elastic.env <<EOF
NEXUS_NODE_NAME=${NEXUS_NODE_NAME}
NEXUS_NODE_IP=${NEXUS_NODE_IP}
NEXUS_NODE_ROLES=${NEXUS_NODE_ROLES}
ES_CLUSTER_NAME=amazon-search
ES_SEED_HOSTS=${ES_SEED_HOSTS}
ES_INITIAL_MASTER_NODES=${NEXUS_CLUSTER_NAME}-master-1
ELASTICSEARCH_DATA=/data/elasticsearch
ES_JAVA_OPTS=-Xms4g -Xmx4g
EOF

chmod 0644 /etc/nexus-node.env /etc/nexus-elastic.env

sync_git_repo "${NEXUS_REPO_URL}" "${NEXUS_REPO_REF}" "${NEXUS_APP_DIR}"
sync_git_repo "${DOCKER_ELK_REPO_URL}" "${DOCKER_ELK_REPO_REF}" "${DOCKER_ELK_APP_DIR}"
write_search_demo_config

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
