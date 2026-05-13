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

Demo search moi chay tren master VM bang Docker Compose:

- Streamlit frontend: TCP `8501`
- FastAPI backend: TCP `8000`
- PostgreSQL, Elasticsearch, Meilisearch: khong mo public; truy cap bang SSH tunnel khi can

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
allowed_admin_cidrs = ["YOUR_PUBLIC_IP/32"]
ssh_public_key = "ssh-ed25519 YOUR_PUBLIC_KEY nexus"
enable_oslogin = false
enable_master_worker_ssh = true
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

Startup writes `/opt/nexus/docker-elk/.env` with demo defaults:

```text
POSTGRES_DB=amazon_search
POSTGRES_USER=search
POSTGRES_PASSWORD=search_demo
MEILI_MASTER_KEY=masterKey
```

Local changes inside `/opt/nexus/docker-elk` can be overwritten on VM boot
because the startup script fast-forwards/resets the configured branch.

## Start Demo

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
cd /opt/nexus/docker-elk
docker compose up -d --build
docker compose exec -T backend python scripts/ingest_all.py --reset
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

## Full Dataset

On the master VM:

```bash
cd /opt/nexus/docker-elk
python3 data/download_datasets.py --reviews
docker compose exec -T backend python scripts/ingest_all.py --reset --product-limit 5000 --review-limit 20000
```

Raise the limits only if the VM has enough disk, RAM and time.

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
SSH key and stores it in Terraform state. Install it on the master if you need
direct master-to-worker SSH:

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
