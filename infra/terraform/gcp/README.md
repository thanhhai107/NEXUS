# GCP Terraform

Minimal Terraform for the NEXUS VM lab cluster.

It creates:

- 1 master VM
- 4 worker VMs by default
- Public IP only on the master VM
- Private-only worker VMs
- Ubuntu 22.04 LTS
- Docker and Docker Compose plugin via `scripts/startup.sh`
- SSH, master UI, MinIO console, and internal cluster firewall rules
- A small VM service account for logging and monitoring
- `/etc/nexus-node.env` and `/etc/nexus-elastic.env` on each VM
- Optional repo provisioning into `/opt/nexus/app` and `/opt/nexus/docker-elk`

It does not install Kubernetes or Ansible.

## Files

```text
main.tf                   All Terraform resources and outputs
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

terraform destroy -var-file="terraform.tfvars" # Clean up when done
```

Set at least:

```hcl
project_id = "your-gcp-project-id"
allowed_admin_cidrs = ["YOUR_PUBLIC_IP/32"]
ssh_public_key = "ssh-ed25519 YOUR_PUBLIC_KEY nexus"
enable_oslogin = false
```

If you intentionally want SSH open to the whole Internet, use:

```hcl
allowed_admin_cidrs = ["0.0.0.0/0"]
```

Workers are private-only in this Terraform config, so public SSH exposure is for
the master VM. If old worker VMs still have external IPs from a previous apply,
apply the updated Terraform first so those public IPs are removed.

Password SSH login can be enabled for simple demos:

```hcl
enable_ssh_password_login = true
ssh_password              = "CHANGE_ME_TO_A_STRONG_PASSWORD"
```

When enabled, `scripts/startup.sh` sets the password for `ssh_user` and enables
SSH password authentication. Keep this for short-lived demos only when SSH is
open to `0.0.0.0/0`.

By default, the VM boots with both demo repos already present:

```hcl
nexus_repo_url = "https://github.com/thanhhai107/NEXUS.git"
nexus_repo_ref = "main"

docker_elk_repo_url = "https://github.com/thanhhai107/docker-elk.git"
docker_elk_repo_ref = "main"
```

When these URLs are set, the startup script clones or syncs to the latest
commit on the configured branch:

- Nexus repo: `/opt/nexus/app`
- ShopX `docker-elk` repo: `/opt/nexus/docker-elk`

It does not run Docker Compose automatically.
Local changes inside these two VM directories can be overwritten on boot.

Generate a key if you do not already have one:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/nexus_gcp -C "nexus"
cat ~/.ssh/nexus_gcp.pub
```

Connect after apply:

```bash
terraform output
ssh -i ~/.ssh/nexus_gcp ubuntu@<MASTER_PUBLIC_IP>
```

Workers do not have public IPs. Connect through the master VM as a jump host:

```bash
ssh -J ubuntu@<MASTER_PUBLIC_IP> ubuntu@<WORKER_PRIVATE_IP>
```

After the VM is running, SSH into the target VM and run the stack you want.

Run the Nexus stack on the master VM from the Nexus repo:

```bash
cd /opt/nexus/app
docker compose -f infra/docker/docker-compose.yml up -d
```

Run the ShopX `docker-elk` Elasticsearch worker on worker VMs:

```bash
cd /opt/nexus/docker-elk
cat /etc/nexus-elastic.env
docker compose --env-file .env --env-file /etc/nexus-elastic.env up -d elasticsearch
```

Run the ShopX `docker-elk` master services on the master VM:

```bash
cd /opt/nexus/docker-elk
docker compose --env-file .env --env-file /etc/nexus-elastic.env --profile master up -d
```

The `master_ui` firewall rule includes Kibana on TCP `5601`.
