#!/usr/bin/env bash
# Run one command on all worker VMs via SSH.
# Worker IPs are read from /etc/nexus-workers (one IP per line).
set -euo pipefail

CONFIG_FILE="/etc/nexus-workers"
SSH_USER_DEFAULT="${SSH_USER:-ubuntu}"
CONNECT_TIMEOUT="10"
PARALLEL="true"
STRICT_HOST_KEY_CHECKING="accept-new"

usage() {
  cat <<'EOF'
Usage:
  nexus-run-workers [options] -- <command>

Options:
  -f, --config <file>      Worker list file (default: /etc/nexus-workers)
  -u, --user <user>        SSH user (default: $SSH_USER or ubuntu)
  -k, --key <path>         SSH private key path (default: ~/.ssh/id_ed25519_nexus_cluster)
  -t, --timeout <seconds>  SSH connect timeout (default: 10)
      --serial             Run workers one by one (default: parallel)
      --strict-host-key    Use strict host key checking (default: accept-new)
  -h, --help               Show this help

Examples:
  nexus-run-workers -- "sudo nexus-configure-env && sudo start-nexus-worker"
  nexus-run-workers -u ubuntu --serial -- "hostname && uptime"
EOF
}

real_home() {
  if [ -n "${SUDO_USER:-}" ]; then
    getent passwd "${SUDO_USER}" | cut -d: -f6
  else
    printf "%s" "${HOME}"
  fi
}

SSH_KEY_DEFAULT="$(real_home)/.ssh/id_ed25519_nexus_cluster"
SSH_KEY="${SSH_KEY_DEFAULT}"
SSH_USER="${SSH_USER_DEFAULT}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    -f|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -u|--user)
      SSH_USER="$2"
      shift 2
      ;;
    -k|--key)
      SSH_KEY="$2"
      shift 2
      ;;
    -t|--timeout)
      CONNECT_TIMEOUT="$2"
      shift 2
      ;;
    --serial)
      PARALLEL="false"
      shift
      ;;
    --strict-host-key)
      STRICT_HOST_KEY_CHECKING="yes"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

REMOTE_CMD="$*"
if [ -z "${REMOTE_CMD}" ]; then
  usage
  exit 1
fi

if [ ! -f "${CONFIG_FILE}" ]; then
  echo "ERROR: ${CONFIG_FILE} not found."
  exit 1
fi

if [ ! -f "${SSH_KEY}" ]; then
  echo "ERROR: SSH key ${SSH_KEY} not found."
  exit 1
fi

if ! [[ "${CONNECT_TIMEOUT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: timeout must be an integer."
  exit 1
fi

WORKER_IPS=()
while IFS= read -r raw_line; do
  line="$(printf '%s' "${raw_line}" | sed 's/#.*$//' | xargs)"
  [ -z "${line}" ] && continue
  WORKER_IPS+=("${line}")
done < "${CONFIG_FILE}"

if [ "${#WORKER_IPS[@]}" -eq 0 ]; then
  echo "ERROR: No worker IPs found in ${CONFIG_FILE}."
  exit 1
fi

SSH_OPTS=(
  -i "${SSH_KEY}"
  -o "StrictHostKeyChecking=${STRICT_HOST_KEY_CHECKING}"
  -o "BatchMode=yes"
  -o "ConnectTimeout=${CONNECT_TIMEOUT}"
)

run_one() {
  local ip="$1"
  echo "[${ip}] starting"
  if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${ip}" "${REMOTE_CMD}" 2>&1 | sed "s/^/[${ip}] /"; then
    echo "[${ip}] success"
    return 0
  fi
  echo "[${ip}] failed"
  return 1
}

echo "Running command on ${#WORKER_IPS[@]} worker(s)..."
failed=0

if [ "${PARALLEL}" = "false" ]; then
  for ip in "${WORKER_IPS[@]}"; do
    run_one "${ip}" || failed=$((failed + 1))
  done
else
  pids=()
  pid_to_ip=()
  for ip in "${WORKER_IPS[@]}"; do
    ( run_one "${ip}" ) &
    pids+=("$!")
    pid_to_ip+=("${ip}")
  done

  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      failed=$((failed + 1))
    fi
  done
fi

echo "---"
if [ "${failed}" -eq 0 ]; then
  echo "All workers completed successfully."
else
  echo "${failed}/${#WORKER_IPS[@]} workers failed."
  exit 1
fi
