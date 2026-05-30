#!/usr/bin/env bash
# Run a command on all worker VMs via SSH.
# Worker IPs are read from /etc/nexus-workers (one IP per line).
# The cluster SSH key is used for passwordless authentication.
set -euo pipefail

CONFIG_FILE="/etc/nexus-workers"

# When run with sudo, HOME points to /root; resolve the actual user's home
if [ -n "${SUDO_USER:-}" ]; then
  REAL_HOME="$(getent passwd "${SUDO_USER}" | cut -d: -f6)"
else
  REAL_HOME="${HOME}"
fi
SSH_KEY="${REAL_HOME}/.ssh/id_ed25519_nexus_cluster"
SSH_USER="${SSH_USER:-ubuntu}"
REMOTE_CMD="${1:-}"

if [ -z "${REMOTE_CMD}" ]; then
  echo "Usage: ${0} '<command>'"
  echo ""
  echo "Cài đặt worker IPs trước:"
  echo "  echo '<worker-ip>' | sudo tee -a /etc/nexus-workers"
  echo ""
  echo "Ví dụ:"
  echo "  ${0} 'sudo nexus-configure-env && sudo start-nexus-worker'"
  exit 1
fi

if [ ! -f "${CONFIG_FILE}" ]; then
  echo "ERROR: ${CONFIG_FILE} not found."
  echo "Thêm worker IPs vào ${CONFIG_FILE}, mỗi dòng một IP."
  exit 1
fi

if [ ! -f "${SSH_KEY}" ]; then
  echo "ERROR: SSH key ${SSH_KEY} not found."
  exit 1
fi

WORKER_IPS=()
while IFS= read -r line; do
  line="$(echo "${line}" | xargs)"
  [ -z "${line}" ] && continue
  WORKER_IPS+=("${line}")
done < "${CONFIG_FILE}"

if [ "${#WORKER_IPS[@]}" -eq 0 ]; then
  echo "ERROR: No worker IPs found in ${CONFIG_FILE}."
  exit 1
fi

echo "Running on ${#WORKER_IPS[@]} workers..."
pids=()
for ip in "${WORKER_IPS[@]}"; do
  (
    echo "[${ip}] Starting..."
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i "${SSH_KEY}" "${SSH_USER}@${ip}" "${REMOTE_CMD}" 2>&1 | sed "s/^/[${ip}] /"
    echo "[${ip}] Done (exit code: $?)"
  ) &
  pids+=($!)
done

failed=0
for pid in "${pids[@]}"; do
  wait "${pid}" || failed=$((failed + 1))
done

echo "---"
if [ "${failed}" -eq 0 ]; then
  echo "All workers completed successfully."
else
  echo "${failed}/${#WORKER_IPS[@]} workers failed."
fi
