# GCP Terraform For NEXUS

This Terraform profile creates a small Ubuntu VM cluster for NEXUS.

## What It Provisions

- 1 master VM with a public IP.
- Private worker VMs.
- Docker and Docker Compose plugin on every VM.
- Optional Nexus repo checkout at `/opt/nexus/nexus`.
- Internal master-to-worker SSH when enabled.
- Firewall rules for SSH and Nexus user-facing services.
- Cloud NAT so private workers can pull packages and images.

The startup script writes `/etc/nexus-node.env` on every VM and installs these
helpers on the master:

```bash
start-nexus-compose
stop-nexus-compose
```

## Files

```text
main.tf                   Terraform resources, variables, outputs
terraform.tfvars.example  Example variables
scripts/startup.sh        VM bootstrap script
```

## Configure

```bash
cd infra/terraform/gcp
cp terraform.tfvars.example terraform.tfvars
```

Set at least:

```hcl
project_id = "nexus-496808"
allowed_admin_cidrs = ["YOUR_PUBLIC_IP/32"]
ssh_public_key = "ssh-ed25519 YOUR_PUBLIC_KEY nexus"
nexus_repo_url = "https://github.com/thanhhai107/NEXUS.git"
nexus_repo_ref = "master"
```

Use `allowed_admin_cidrs = ["0.0.0.0/0"]` only for a short demo.

## Apply

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>

terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
terraform output
```

SSH to the master:

```bash
terraform output master
ssh ubuntu@<MASTER_PUBLIC_IP>
```

Start the local Nexus Docker Compose stack on the master:

```bash
start-nexus-compose
```

Stop it:

```bash
stop-nexus-compose
```

## Public Services

Terraform exposes only Nexus-facing ports on the master, restricted by
`allowed_admin_cidrs`:

| Port | Service |
| ---: | --- |
| 22 | SSH |
| 8000 | FastAPI |
| 8080 | Airflow |
| 8085 | Trino |
| 8088 | Superset |
| 9001 | MinIO Console |

Use the output below if you prefer SSH tunnels:

```bash
terraform output nexus_tunnel_command
```

## Worker Access

Workers do not have public IPs. Connect through the master:

```bash
ssh -J ubuntu@<MASTER_PUBLIC_IP> ubuntu@<WORKER_PRIVATE_IP>
```

When `enable_master_worker_ssh = true`, Terraform creates an internal key,
stores it in Terraform state, and the startup script installs it on the master.

To reinstall that key manually on the master:

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

Keep `.terraform/`, state files, and real `terraform.tfvars` out of Git.
