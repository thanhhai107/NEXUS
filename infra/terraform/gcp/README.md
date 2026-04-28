# GCP Terraform

Minimal Terraform for the NEXUS VM lab cluster.

It creates:

- 1 master VM
- 4 worker VMs by default
- Ubuntu 22.04 LTS
- Docker and Docker Compose plugin via `scripts/startup.sh`
- SSH, master UI, MinIO console, and internal cluster firewall rules
- A small VM service account for logging and monitoring

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

After provisioning, clone or copy this repo to `/opt/nexus` on the target nodes, create `.env`, then run:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```
