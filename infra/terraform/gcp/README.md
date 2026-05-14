# GCP Terraform

Terraform nay tao cum VM GCP cho demo Amazon Electronics Search:

- 1 master VM co public IP
- 4 worker VM private-only theo mac dinh
- Ubuntu 22.04 LTS
- Docker va Docker Compose plugin
- Cloud NAT de worker private pull Docker images / git repos
- Firewall cho SSH, Streamlit UI va FastAPI tren master
- Repo demo duoc clone vao `/opt/nexus/docker-elk`
- Helper `start-amazon-search-demo` tren master de start stack va ingest sample data

Demo search moi chay bang Docker Compose:

- Streamlit frontend: TCP `8501`
- FastAPI backend: TCP `8000`
- PostgreSQL va Meilisearch: tren master VM
- Elasticsearch: phan tan tren master + worker VMs
- PostgreSQL, Elasticsearch, Meilisearch khong mo public; truy cap bang SSH tunnel khi can

## Files

```text
main.tf                   Terraform resources and outputs
terraform.tfvars.example  Example variables
scripts/startup.sh        VM bootstrap script
```

## Usage

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>

cd infra/terraform/gcp
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
terraform output
```

Set at least:

```hcl
project_id = "your-gcp-project-id"
master_zone = "asia-southeast1-b"
worker_zones = [
  "asia-southeast1-b",
  "asia-southeast1-b",
  "asia-southeast1-c",
  "asia-southeast1-c"
]
allowed_admin_cidrs = ["YOUR_PUBLIC_IP/32"]
ssh_public_key = "ssh-ed25519 YOUR_PUBLIC_KEY nexus"
enable_oslogin = false
enable_master_worker_ssh = true
```

Default zone placement:

```text
nexus-master-1   asia-southeast1-b
nexus-worker-1   asia-southeast1-b
nexus-worker-2   asia-southeast1-b
nexus-worker-3   asia-southeast1-c
nexus-worker-4   asia-southeast1-c
```

For a short classroom demo you can temporarily use:

```hcl
allowed_admin_cidrs = ["0.0.0.0/0"]
```

Prefer a narrow `/32` CIDR when possible. Only `22`, `8000`, `8501`, and the
optional Nexus UI ports are public through this Terraform config.

## Repo Provisioning

By default, startup clones:

```hcl
nexus_repo_url = "https://github.com/thanhhai107/NEXUS.git"
nexus_repo_ref = "master"

docker_elk_repo_url = "https://github.com/thanhhai107/docker-elk.git"
docker_elk_repo_ref = "main"
```

The Amazon Search demo repo is placed at:

```text
/opt/nexus/docker-elk
```

Startup writes `/opt/nexus/docker-elk/.env` with demo defaults on every VM:

```text
POSTGRES_DB=amazon_search
POSTGRES_USER=search
POSTGRES_PASSWORD=search_demo
MEILI_MASTER_KEY=masterKey
```

Startup also writes `/etc/nexus-elastic.env` on every VM. The master gets
`NEXUS_NODE_ROLES=master`; workers get `NEXUS_NODE_ROLES=data,ingest`.

Local changes inside `/opt/nexus/docker-elk` can be overwritten on VM boot
because the startup script fast-forwards/resets the configured branch.

Startup also prepares every Elasticsearch host by setting
`vm.max_map_count=262144` immediately and persisting it in
`/etc/sysctl.d/99-elasticsearch.conf`. This avoids Elasticsearch exit code `78`
from the bootstrap check when the default Linux value is too low.

## Start Distributed Elasticsearch

On the master VM, start Elasticsearch across the master and all workers with one
helper:

```bash
start-amazon-search-elasticsearch-cluster
```

The helper SSHes from the master to each worker and runs
`start-amazon-search-elasticsearch` there, then starts the master Elasticsearch
container. The startup script installs the internal master-to-worker SSH key on
the master automatically when `enable_master_worker_ssh = true`.

## Start Demo On Master

After `terraform apply`, SSH to the master:

```bash
ssh ubuntu@<MASTER_PUBLIC_IP>
```

Run:

```bash
start-amazon-search-demo
```

The helper runs:

```bash
start-amazon-search-elasticsearch-cluster
cd /opt/nexus/docker-elk
docker compose --env-file .env --env-file /etc/nexus-elastic.env up -d --build postgres meilisearch elasticsearch backend frontend
docker compose exec -T backend python scripts/ingest_all.py --reset --product-limit 2000000 --review-limit 200000
```

It uses `data/sample` if you have not downloaded the full Amazon dataset yet.

Open:

```text
http://<MASTER_PUBLIC_IP>:8501
http://<MASTER_PUBLIC_IP>:8000/docs
```

The same URLs are available from:

```bash
terraform output service_urls
```

Verify Elasticsearch is distributed:

```bash
curl "http://localhost:9200/_cat/nodes?v&h=name,node.role,master,ip"
curl "http://localhost:9200/_cluster/health?pretty"
```

Expected nodes:

```text
nexus-master-1    master/coordinating
nexus-worker-1    data,ingest
nexus-worker-2    data,ingest
nexus-worker-3    data,ingest
nexus-worker-4    data,ingest
```

## Full Dataset

On the master VM:

```bash
cd /opt/nexus/docker-elk
python3 data/download_datasets.py --reviews
docker compose exec -T backend python scripts/ingest_all.py --reset --product-limit 2000000 --review-limit 200000
```

Lower the limits if the VM does not have enough disk, RAM, or time.

## Engine Access From Local Machine

PostgreSQL, Elasticsearch and Meilisearch are intentionally not exposed
publicly. Use the Terraform output:

```bash
terraform output search_engine_tunnel_command
```

Example:

```bash
ssh -L 5432:127.0.0.1:5432 \
    -L 9200:127.0.0.1:9200 \
    -L 7700:127.0.0.1:7700 \
    ubuntu@<MASTER_PUBLIC_IP>
```

Then local URLs are:

```text
PostgreSQL:     127.0.0.1:5432
Elasticsearch:  http://127.0.0.1:9200
Meilisearch:    http://127.0.0.1:7700
```

## Worker Access

Workers do not have public IPs. Connect through the master VM:

```bash
ssh -J ubuntu@<MASTER_PUBLIC_IP> ubuntu@<WORKER_PRIVATE_IP>
```

When `enable_master_worker_ssh = true`, Terraform creates an internal cluster
SSH key, stores it in Terraform state, and the startup script installs it on the
master for commands such as `start-amazon-search-elasticsearch-cluster`.

If you ever need to reinstall it manually, run this on the master:

```bash
install -m 700 -d ~/.ssh
curl -fsS -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/nexus-master-worker-private-key-b64 \
  | base64 -d > ~/.ssh/id_ed25519_nexus_cluster
chmod 600 ~/.ssh/id_ed25519_nexus_cluster
cat > ~/.ssh/config <<'EOF'
Host nexus-worker-* 10.*
  User ubuntu
  IdentityFile ~/.ssh/id_ed25519_nexus_cluster
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
chmod 600 ~/.ssh/config
```

## Clean Up

```bash
terraform destroy -var-file="terraform.tfvars"
```
