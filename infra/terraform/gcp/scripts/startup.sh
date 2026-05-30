#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

BOOTSTRAP_USER="${ssh_user}"
NEXUS_CLUSTER_NAME="${cluster_name}"
NEXUS_WORKER_COUNT="${worker_count}"
NEXUS_NODE_ROLE="${nexus_node_role}"
NEXUS_NODE_INDEX="${nexus_node_index}"
NEXUS_REPO_URL="${nexus_repo_url}"
NEXUS_REPO_REF="${nexus_repo_ref}"
SSH_PASSWORD_LOGIN="${enable_ssh_password_login}"
SSH_PASSWORD="${ssh_password}"
SSH_USER="${ssh_user}"
ENABLE_MASTER_WORKER_SSH="${enable_master_worker_ssh}"
MASTER_WORKER_PRIVATE_KEY_B64="${master_worker_private_key_b64}"
NEXUS_MASTER_PRIVATE_IP="${master_private_ip}"

NEXUS_HOME="/opt/nexus"
NEXUS_APP_DIR="$${NEXUS_HOME}/NEXUS"
NEXUS_DATA="/data"

if [ -z "$${BOOTSTRAP_USER}" ]; then
  BOOTSTRAP_USER="ubuntu"
fi
if [ -z "$${NEXUS_WORKER_COUNT}" ]; then
  NEXUS_WORKER_COUNT="4"
fi

NEXUS_NODE_NAME="$(curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name" || echo "unknown")"
NEXUS_NODE_IP="$(curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" || echo "unknown")"
if [ -z "$${NEXUS_MASTER_PRIVATE_IP}" ]; then
  NEXUS_MASTER_PRIVATE_IP="$${NEXUS_NODE_IP}"
fi

# MinIO peer IPs from Terraform
NEXUS_MINIO_PEER_IPS="${minio_peer_ips}"

# ZK peer IPs (first 3 nodes: master + worker-1 + worker-2)
NEXUS_ZK_PEER_IPS="${zk_peer_ips}"

# ---------------------------------------------------------------------------
# Master-worker SSH key
# ---------------------------------------------------------------------------
install_master_worker_ssh() {
  if [ "$${NEXUS_NODE_ROLE}" != "master" ]; then
    return 0
  fi
  if [ "$${ENABLE_MASTER_WORKER_SSH}" != "true" ]; then
    return 0
  fi
  if [ -z "$${MASTER_WORKER_PRIVATE_KEY_B64}" ]; then
    return 0
  fi

  install -m 0700 -d "/home/$${BOOTSTRAP_USER}/.ssh"
  printf "%s" "$${MASTER_WORKER_PRIVATE_KEY_B64}" \
    | base64 -d >"/home/$${BOOTSTRAP_USER}/.ssh/id_ed25519_nexus_cluster"
  chmod 0600 "/home/$${BOOTSTRAP_USER}/.ssh/id_ed25519_nexus_cluster"

  cat >"/home/$${BOOTSTRAP_USER}/.ssh/config" <<EOF
Host $${NEXUS_CLUSTER_NAME}-worker-* 10.*
  User $${BOOTSTRAP_USER}
  IdentityFile ~/.ssh/id_ed25519_nexus_cluster
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
  chmod 0600 "/home/$${BOOTSTRAP_USER}/.ssh/config"
  chown -R "$${BOOTSTRAP_USER}:$${BOOTSTRAP_USER}" "/home/$${BOOTSTRAP_USER}/.ssh"
}

# ---------------------------------------------------------------------------
# Git repo sync
# ---------------------------------------------------------------------------
sync_git_repo() {
  local repo_url="$1"
  local repo_ref="$2"
  local target_dir="$3"

  if [ -z "$${repo_url}" ]; then
    return 0
  fi

  if [ ! -d "$${target_dir}/.git" ]; then
    rm -rf "$${target_dir}"
    git clone "$${repo_url}" "$${target_dir}"
  fi

  chown -R "$${BOOTSTRAP_USER}:$${BOOTSTRAP_USER}" "$${target_dir}"
  git config --system --add safe.directory "$${target_dir}" || true
  git -C "$${target_dir}" remote set-url origin "$${repo_url}"
  git -C "$${target_dir}" fetch origin --tags --prune
  if git -C "$${target_dir}" show-ref --verify --quiet "refs/remotes/origin/$${repo_ref}"; then
    git -C "$${target_dir}" checkout -B "$${repo_ref}" "origin/$${repo_ref}"
    git -C "$${target_dir}" reset --hard "origin/$${repo_ref}"
  else
    git -C "$${target_dir}" checkout --detach "$${repo_ref}"
  fi
  chown -R "$${BOOTSTRAP_USER}:$${BOOTSTRAP_USER}" "$${target_dir}"
}

# ---------------------------------------------------------------------------
# Docker forwarding
# ---------------------------------------------------------------------------
setup_docker_forwarding() {
  local sysctl_file="/etc/sysctl.d/98-nexus-docker-forward.conf"

  sysctl -w net.ipv4.ip_forward=1
  printf "net.ipv4.ip_forward=1\n" >"$${sysctl_file}"
  sysctl --system >/dev/null
}

# ---------------------------------------------------------------------------
# Docker install
# ---------------------------------------------------------------------------
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
deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $${VERSION_CODENAME} stable
EOF

  apt-get update -y
  apt-get install -y \
    containerd.io \
    docker-buildx-plugin \
    docker-ce \
    docker-ce-cli \
    docker-compose-plugin

  systemctl enable --now docker
  usermod -aG docker "$${BOOTSTRAP_USER}" || true
}

# ---------------------------------------------------------------------------
# Password login
# ---------------------------------------------------------------------------
configure_password_login() {
  if [ "$${SSH_PASSWORD_LOGIN}" != "true" ] || [ -z "$${SSH_PASSWORD}" ] || [ -z "$${SSH_USER}" ]; then
    return 0
  fi

  echo "$${SSH_USER}:$${SSH_PASSWORD}" | chpasswd
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

# ---------------------------------------------------------------------------
# Write helper scripts
# ---------------------------------------------------------------------------
write_nexus_helpers() {
  # Install node-services.sh as a library
  cp "$${NEXUS_APP_DIR}/infra/docker/node-services.sh" /usr/local/bin/nexus-node-services
  chmod 0644 /usr/local/bin/nexus-node-services

  cat >/usr/local/bin/nexus-configure-env <<'NCEOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

set_env_value() {
  local env_file="$1"
  local key="$2"
  local value="$3"
  local escaped_value

  escaped_value="$(printf '%s' "$value" | sed 's/[&|]/\\&/g')"
  if grep -q "^$${key}=" "$env_file"; then
    sed -i "s|^$${key}=.*|$${key}=$${escaped_value}|" "$env_file"
  else
    printf "%s=%s\n" "$key" "$value" >>"$env_file"
  fi
}

cd "$NEXUS_APP_DIR"
if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
fi

master_ip="$${NEXUS_MASTER_PRIVATE_IP:-$NEXUS_NODE_IP}"
zk_peers="$${NEXUS_ZK_PEER_IPS:-$master_ip}"
minio_peers="$${NEXUS_MINIO_PEER_IPS:-$master_ip}"

default_worker_cores() {
  local cpus
  cpus="$(nproc --all 2>/dev/null || echo 2)"
  if [ "$cpus" -gt 2 ]; then
    echo $((cpus - 1))
  else
    echo "$cpus"
  fi
}

default_worker_memory() {
  local mem_mb usable_gb
  mem_mb="$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)"
  usable_gb=$(((mem_mb - 2048) / 1024))
  if [ "$usable_gb" -lt 1 ]; then
    usable_gb=1
  fi
  echo "$${usable_gb}G"
}

worker_cores="$${SPARK_WORKER_CORES:-$(default_worker_cores)}"
worker_memory="$${SPARK_WORKER_MEMORY:-$(default_worker_memory)}"
parallelism="$worker_cores"
if [[ "$NEXUS_WORKER_COUNT" =~ ^[0-9]+$ ]] && [[ "$worker_cores" =~ ^[0-9]+$ ]]; then
  parallelism=$((NEXUS_WORKER_COUNT * worker_cores))
fi

# Build ZK config
IFS=' ' read -r -a zk_ips <<< "$zk_peers"
zk_servers=""
for i in "$${!zk_ips[@]}"; do
  if [ -n "$zk_servers" ]; then zk_servers+=";"; fi
  zk_servers+="$${zk_ips[$i]}:2888:3888"
done
zk_connect=""
for ip in "$${zk_ips[@]}"; do
  if [ -n "$zk_connect" ]; then zk_connect+=","; fi
  zk_connect+="$${ip}:2181"
done

# Build MinIO peer command
IFS=' ' read -r -a minio_ips <<< "$minio_peers"
if [ "$${#minio_ips[@]}" -le 1 ]; then
  minio_cmd="/data"
else
  minio_cmd=""
  for ip in "$${minio_ips[@]}"; do
    if [ -n "$minio_cmd" ]; then minio_cmd+=" "; fi
    minio_cmd+="http://$${ip}:9000/data"
  done
fi

# Build Kafka bootstrap servers
kafka_servers=""
for ip in "$${zk_ips[@]}"; do
  if [ -n "$kafka_servers" ]; then kafka_servers+=","; fi
  kafka_servers+="$${ip}:9092"
done

set_env_value .env NEXUS_RUNTIME_MODE vm
set_env_value .env NEXUS_RUNTIME_DIR "$NEXUS_DATA"
set_env_value .env NEXUS_ACTOR vm
set_env_value .env NEXUS_GOVERNANCE_STORAGE postgres
set_env_value .env NEXUS_GX_ENABLED true
set_env_value .env MINIO_ENDPOINT "http://$${master_ip}:9000"
set_env_value .env MINIO_ROOT_USER "$${MINIO_ROOT_USER:-minioadmin}"
set_env_value .env MINIO_ROOT_PASSWORD "$${MINIO_ROOT_PASSWORD:-minioadmin}"
set_env_value .env NEXUS_BUCKET "$${NEXUS_BUCKET:-nexus-lakehouse}"
set_env_value .env NEXUS_MINIO_VOLUME "$${NEXUS_DATA}/minio"
set_env_value .env ZK_SERVERS "$zk_servers"
set_env_value .env ZK_CONNECT "$zk_connect"
set_env_value .env MINIO_PEERS "$minio_cmd"
set_env_value .env KAFKA_BOOTSTRAP_SERVERS "$kafka_servers"
set_env_value .env HIVE_METASTORE_URI "thrift://hive-metastore:9083"
set_env_value .env TRINO_HOST trino-coordinator
set_env_value .env TRINO_PORT 8080
set_env_value .env TRINO_USER "$${TRINO_USER:-nexus}"
set_env_value .env SPARK_MASTER_URL "spark://$${master_ip}:7077"
set_env_value .env SPARK_WORKER_CORES "$worker_cores"
set_env_value .env SPARK_WORKER_MEMORY "$worker_memory"
set_env_value .env SPARK_EXECUTOR_CORES "$worker_cores"
set_env_value .env SPARK_EXECUTOR_MEMORY "$worker_memory"
set_env_value .env SPARK_DEFAULT_PARALLELISM "$parallelism"
set_env_value .env SPARK_SQL_SHUFFLE_PARTITIONS "$parallelism"
set_env_value .env SPARK_LOCAL_DIRS "$NEXUS_DATA/spark"
set_env_value .env SUPERSET_SECRET_KEY "$${SUPERSET_SECRET_KEY:-change-me-for-production}"
set_env_value .env SUPERSET_ADMIN_USERNAME "$${SUPERSET_ADMIN_USERNAME:-admin}"
set_env_value .env SUPERSET_ADMIN_PASSWORD "$${SUPERSET_ADMIN_PASSWORD:-admin}"
set_env_value .env AIRFLOW_ADMIN_USERNAME "$${AIRFLOW_ADMIN_USERNAME:-admin}"
set_env_value .env AIRFLOW_ADMIN_PASSWORD "$${AIRFLOW_ADMIN_PASSWORD:-admin}"
set_env_value .env AIRFLOW_DB_URL "postgresql+psycopg2://airflow:airflow@$${master_ip}:5433/airflow"
set_env_value .env AIRFLOW_CELERY_BROKER_URL "redis://:@$${master_ip}:6379/0"
set_env_value .env NEXUS_GOVERNANCE_STORAGE "local"
set_env_value .env NEXUS_MASTER_PRIVATE_IP "$master_ip"
set_env_value .env NEXUS_NODE_IP "$NEXUS_NODE_IP"

mkdir -p \
  "$NEXUS_DATA/minio" \
  "$NEXUS_DATA/postgres/hive-metastore" \
  "$NEXUS_DATA/postgres/airflow" \
  "$NEXUS_DATA/spark" \
  "$NEXUS_DATA/superset"

chown -R 1001:0 "$NEXUS_DATA/spark" || true
chown -R 999:999 "$NEXUS_DATA/postgres" || true

# Generate nginx API LB config with remote worker IPs
mkdir -p "${NEXUS_APP_DIR}/runtime"
cat > "${NEXUS_APP_DIR}/runtime/nginx-api-lb.conf" <<NGINXEOF
events {
    worker_connections 1024;
}

http {
    upstream api_backend {
        server api-1:8000;
NGINXEOF

worker_count="${NEXUS_WORKER_COUNT:-4}"
if [ "$worker_count" -ge 1 ]; then
  for peer_ip in $minio_peers; do
    if [ "$peer_ip" != "$master_ip" ]; then
      echo "        server ${peer_ip}:8000;" >> "${NEXUS_APP_DIR}/runtime/nginx-api-lb.conf"
      break
    fi
  done
fi
if [ "$worker_count" -ge 2 ]; then
  count=0
  for peer_ip in $minio_peers; do
    if [ "$peer_ip" != "$master_ip" ]; then
      count=$((count + 1))
      if [ "$count" -eq 2 ]; then
        echo "        server ${peer_ip}:8000;" >> "${NEXUS_APP_DIR}/runtime/nginx-api-lb.conf"
        break
      fi
    fi
  done
fi

cat >> "${NEXUS_APP_DIR}/runtime/nginx-api-lb.conf" <<NGINXEOF
    }

    server {
        listen 8000;

        location / {
            proxy_pass http://api_backend;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
        }
    }
}
NGINXEOF
NCEOF
  chmod 0755 /usr/local/bin/nexus-configure-env

  cat >/usr/local/bin/start-nexus-compose <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

if [ "$NEXUS_NODE_ROLE" != "master" ]; then
  echo "Run this command on the master VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

cd "${NEXUS_APP_DIR}"
nexus-configure-env

. /usr/local/bin/nexus-node-services
SERVICES=$(node_services)

echo "Starting MASTER services (role=${NEXUS_NODE_ROLE}): ${SERVICES}"
${DOCKER} compose --env-file .env -f infra/docker/docker-compose.yml up -d --build ${SERVICES}
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

  cat >/usr/local/bin/start-nexus-worker <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

if [ "$NEXUS_NODE_ROLE" != "worker" ]; then
  echo "Run this command on a worker VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

nexus-configure-env
cd "$NEXUS_APP_DIR"

. /usr/local/bin/nexus-node-services
SERVICES=$(node_services)

echo "Starting WORKER services (index=${NEXUS_NODE_INDEX}, ip=${NEXUS_NODE_IP}): ${SERVICES}"
$DOCKER compose --env-file .env -f infra/docker/docker-compose.worker.yml up -d --build ${SERVICES}
$DOCKER compose --env-file .env -f infra/docker/docker-compose.worker.yml ps
EOF
  chmod 0755 /usr/local/bin/start-nexus-worker

  cat >/usr/local/bin/stop-nexus-worker <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

cd "$NEXUS_APP_DIR"
$DOCKER compose --env-file .env -f infra/docker/docker-compose.worker.yml down
EOF
  chmod 0755 /usr/local/bin/stop-nexus-worker

  if [ "$${NEXUS_NODE_ROLE}" != "master" ]; then
    return 0
  fi

  cat >/usr/local/bin/nexus-spark-submit <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

. /etc/nexus-node.env

if [ "$NEXUS_NODE_ROLE" != "master" ]; then
  echo "Run this command on the master VM."
  exit 1
fi

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

nexus-configure-env
cd "$NEXUS_APP_DIR"

env_value() {
  local key="$1"
  local default="$2"
  local line
  line="$(grep -E "^$key=" .env | tail -n 1 || true)"
  if [ -n "$line" ]; then
    printf '%s' "$line" | cut -d= -f2-
  else
    printf '%s' "$default"
  fi
}

spark_executor_cores="$(env_value SPARK_EXECUTOR_CORES "")"
spark_executor_memory="$(env_value SPARK_EXECUTOR_MEMORY "")"
spark_default_parallelism="$(env_value SPARK_DEFAULT_PARALLELISM "")"
spark_shuffle_partitions="$(env_value SPARK_SQL_SHUFFLE_PARTITIONS "")"

if ! $DOCKER image inspect nexus-spark:3.5 >/dev/null 2>&1; then
  $DOCKER build -t nexus-spark:3.5 infra/docker/spark
fi

$DOCKER run --rm \
  --network host \
  -e SPARK_MASTER="spark://$NEXUS_MASTER_PRIVATE_IP:7077" \
  -e SPARK_PROPERTIES_FILE=/opt/nexus/config/spark-defaults.conf \
  -e SPARK_DRIVER_HOST="$NEXUS_MASTER_PRIVATE_IP" \
  -e SPARK_DRIVER_BIND_ADDRESS=0.0.0.0 \
  -e SPARK_DRIVER_PORT=7078 \
  -e SPARK_BLOCKMANAGER_PORT=7079 \
  -e SPARK_EXECUTOR_CORES="$spark_executor_cores" \
  -e SPARK_EXECUTOR_MEMORY="$spark_executor_memory" \
  -e SPARK_DEFAULT_PARALLELISM="$spark_default_parallelism" \
  -e SPARK_SQL_SHUFFLE_PARTITIONS="$spark_shuffle_partitions" \
  -e SPARK_LOCAL_DIRS="$NEXUS_DATA/spark" \
  -e HIVE_METASTORE_URI="thrift://$NEXUS_MASTER_PRIVATE_IP:9083" \
  -e MINIO_ENDPOINT="http://$NEXUS_MASTER_PRIVATE_IP:9000" \
  -e MINIO_ROOT_USER="minioadmin" \
  -e MINIO_ROOT_PASSWORD="minioadmin" \
  -e NEXUS_RUNTIME_MODE=vm \
  -e NEXUS_RUNTIME_DIR="$NEXUS_DATA" \
  -v "$NEXUS_APP_DIR:/opt/nexus" \
  -v "$NEXUS_DATA:$NEXUS_DATA" \
  -w /opt/nexus \
  nexus-spark:3.5 \
  /opt/nexus/infra/spark/spark-submit-wrapper.sh "$@"
EOF
  chmod 0755 /usr/local/bin/nexus-spark-submit

  # Deploy nexus-run-workers helper
  if [ -f "$${NEXUS_APP_DIR}/infra/terraform/aws/scripts/nexus-run-workers.sh" ]; then
    cp "$${NEXUS_APP_DIR}/infra/terraform/aws/scripts/nexus-run-workers.sh" /usr/local/bin/nexus-run-workers
    chmod 0755 /usr/local/bin/nexus-run-workers
  fi
  if [ ! -f /etc/nexus-workers ] && [ -n "$${NEXUS_MINIO_PEER_IPS}" ]; then
    for ip in $${NEXUS_MINIO_PEER_IPS}; do
      if [ "$${ip}" != "$${NEXUS_MASTER_PRIVATE_IP}" ] && [ "$${ip}" != "$${NEXUS_NODE_IP}" ]; then
        echo "$${ip}" >> /etc/nexus-workers
      fi
    done
  fi
}

# ===========================================================================
# MAIN
# ===========================================================================

setup_docker_forwarding
install_master_worker_ssh
install_docker
configure_password_login

mkdir -p \
  "$${NEXUS_HOME}" \
  "$${NEXUS_DATA}/airflow" \
  "$${NEXUS_DATA}/kafka" \
  "$${NEXUS_DATA}/minio" \
  "$${NEXUS_DATA}/postgres" \
  "$${NEXUS_DATA}/spark" \
  "$${NEXUS_DATA}/trino" \
  /var/log/nexus

chown -R "$${BOOTSTRAP_USER}:$${BOOTSTRAP_USER}" "$${NEXUS_HOME}" "$${NEXUS_DATA}" /var/log/nexus
chown -R 999:999 "$${NEXUS_DATA}/postgres"

cat >/etc/nexus-node.env <<EOF
NEXUS_CLUSTER_NAME=$${NEXUS_CLUSTER_NAME}
NEXUS_NODE_ROLE=$${NEXUS_NODE_ROLE}
NEXUS_NODE_INDEX=$${NEXUS_NODE_INDEX}
NEXUS_NODE_NAME=$${NEXUS_NODE_NAME}
NEXUS_NODE_IP=$${NEXUS_NODE_IP}
NEXUS_MASTER_PRIVATE_IP=$${NEXUS_MASTER_PRIVATE_IP}
NEXUS_WORKER_COUNT=$${NEXUS_WORKER_COUNT}
NEXUS_MINIO_PEER_IPS=$${NEXUS_MINIO_PEER_IPS}
NEXUS_ZK_PEER_IPS=$${NEXUS_ZK_PEER_IPS}
NEXUS_HOME=$${NEXUS_HOME}
NEXUS_APP_DIR=$${NEXUS_APP_DIR}
NEXUS_DATA=$${NEXUS_DATA}
EOF
chmod 0644 /etc/nexus-node.env

sync_git_repo "$${NEXUS_REPO_URL}" "$${NEXUS_REPO_REF}" "$${NEXUS_APP_DIR}"
write_nexus_helpers
nexus-configure-env
if [ "$${NEXUS_NODE_ROLE}" = "worker" ]; then
  start-nexus-worker || true
fi

cat >/var/log/nexus/startup-complete.log <<EOF
NEXUS startup completed.
cluster=$${NEXUS_CLUSTER_NAME}
role=$${NEXUS_NODE_ROLE}
index=$${NEXUS_NODE_INDEX}
name=$${NEXUS_NODE_NAME}
private_ip=$${NEXUS_NODE_IP}
master_private_ip=$${NEXUS_MASTER_PRIVATE_IP}
nexus_repo_url=$${NEXUS_REPO_URL}
nexus_repo_ref=$${NEXUS_REPO_REF}
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF