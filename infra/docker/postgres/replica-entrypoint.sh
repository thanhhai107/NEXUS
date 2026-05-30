#!/usr/bin/env bash
set -e

if [ -z "$(ls -A /var/lib/postgresql/data)" ]; then
    echo "Bootstrapping replica from primary..."
    PGPASSWORD="${REPLICATION_PASSWORD}" pg_basebackup \
        -h "${PRIMARY_HOST}" \
        -D /var/lib/postgresql/data \
        -U "${REPLICATION_USER}" \
        -P \
        -R \
        -X stream \
        -C -S replica_slot
fi

exec docker-entrypoint.sh postgres
