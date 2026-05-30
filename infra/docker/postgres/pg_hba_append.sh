#!/usr/bin/env bash
set -e

cat >> /var/lib/postgresql/data/pg_hba.conf <<EOF
host replication replicator all md5
host all all all md5
EOF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT pg_reload_conf();"
