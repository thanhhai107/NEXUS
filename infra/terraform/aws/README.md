# NEXUS — AWS Terraform

Triển khai cụm NEXUS (1 master + N worker) lên AWS EC2.

## Kiến trúc

```
                      ┌──────────────────────────────┐
                      │        Internet Gateway       │
                      └──────────────┬───────────────┘
                                     │
                      ┌──────────────▼───────────────┐
                      │       Public Subnet(s)        │
                      │  ┌───────────┐                │
                      │  │  Master   │  ← Elastic IP  │
                      │  │  (public) │                │
                      │  └───────────┘                │
                      │  ┌───────────┐                │
                      │  │ NAT GW    │                │
                      │  └─────┬─────┘                │
                      └────────┼──────────────────────┘
                               │
                      ┌────────▼──────────────────────┐
                      │       Private Subnet(s)        │
                      │  ┌──────┐ ┌──────┐    ┌──────┐│
                      │  │Worker│ │Worker│... │Worker││
                      │  │  1   │ │  2   │    │  N   ││
                      │  └──────┘ └──────┘    └──────┘│
                      └────────────────────────────────┘
```

## Tài nguyên chính

| Tài nguyên | Mô tả |
|---|---|
| `aws_vpc` | VPC mới (có thể dùng default VPC nếu cần) |
| `aws_subnet` | Public subnet (master + NAT) + Private subnets (workers) |
| `aws_internet_gateway` | Internet ra/vào cho public subnets |
| `aws_nat_gateway` | NAT cho private workers truy cập internet |
| `aws_instance` (master) | 1 EC2 instance public, chạy tất cả Docker services |
| `aws_instance` (workers) | N EC2 instances private, chạy Spark/Dask workers |
| `aws_key_pair` | SSH key pair cho truy cập |
| `aws_iam_role` + `aws_iam_instance_profile` | IAM role cho EC2 (SSM + CloudWatch) |
| `aws_security_group` | 3 SG: SSH, master UI ports, internal cluster |
| `aws_eip` | Elastic IP cho master |

## Cấu hình AWS CLI

```bash
# 1. Cài đặt AWS CLI v2
# Windows (PowerShell Admin):
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi /quiet

# macOS:
brew install awscli

# Linux:
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# 2. Lấy Access Key từ AWS Console
#    Vào IAM → Users → Security credentials → Create access key
#    (Chọn CLI use case)

# 3. Cấu hình credentials
aws configure
#   AWS Access Key ID:     AKIA... (paste từ bước 2)
#   AWS Secret Access Key: ...     (paste từ bước 2)
#   Default region name:   ap-southeast-1
#   Default output format: json

# 4. Kiểm tra kết nối
aws sts get-caller-identity
```

## Yêu cầu

- Terraform >= 1.6.0
- AWS CLI đã cấu hình (xem phía trên)
- SSH key pair: `ssh-keygen -t ed25519 -f ~/.ssh/nexus_aws -C "nexus-aws"`

## Capacity note

For a 32 vCPU EC2 quota, the recommended role-specific distributed shape is:

```hcl
master_instance_type = "m5zn.3xlarge" # 12 vCPU, 48 GiB
worker_instance_type = "m7i.xlarge"  # 4 vCPU, 16 GiB
worker_count         = 4             # 12 + 4*4 = 28 vCPU
```

This keeps the service-heavy master large enough for Airflow, Kafka, Trino,
Superset, MinIO, Hive Metastore, and optional metadata services while giving
Spark four balanced worker VMs. If you leave role-specific types empty,
Terraform falls back to `instance_type`.

Note: `m7i.3xlarge` is not a valid EC2 instance type. If you want to stay fully
on the `m7i` family instead, use `m7i.4xlarge` for the master; that gives a
16 vCPU / 64 GiB master and uses the full 32 vCPU quota with four
`m7i.xlarge` workers.

## Distributed Spark mode

The master runs the service stack and Spark master:

```bash
start-nexus-compose
```

Each worker VM writes VM-mode `.env` settings and starts a host-network Spark
worker automatically on boot. To restart a worker manually:

```bash
start-nexus-worker
stop-nexus-worker
```

Submit Spark jobs from the master with the host-network helper so executors on
private worker VMs can connect back to the driver:

```bash
nexus-spark-submit processing/bronze/raw_to_bronze.py \
  --raw-path s3a://nexus-lakehouse/raw/example/*.jsonl \
  --bronze-table nexus.bronze.example
```

Spark master UI is exposed on port `8081` for `allowed_admin_cidrs`.

## Triển khai

```bash
cd infra/terraform/aws

# 1. Tạo file biến
cp terraform.tfvars.example terraform.tfvars
# Sửa ssh_public_key và các giá trị khác trong terraform.tfvars

# 2. Khởi tạo
terraform init

# 3. Kiểm tra
terraform plan

# 4. Triển khai
terraform apply

# 5. SSH vào master
ssh ubuntu@<master_public_ip>
```

## Dọn dẹp

```bash
terraform destroy
```

## Khác biệt với GCP version

| Thành phần | GCP | AWS |
|---|---|---|
| Provider | `hashicorp/google` | `hashicorp/aws` |
| Instance | `google_compute_instance` | `aws_instance` |
| Network | `google_compute_network` | `aws_vpc` |
| Firewall | `google_compute_firewall` | `aws_security_group` |
| NAT | `google_compute_router_nat` (Cloud NAT) | `aws_nat_gateway` |
| Static IP | `google_compute_address` | `aws_eip` |
| Service account | `google_service_account` | `aws_iam_role` + `aws_iam_instance_profile` |
| SSH key | Embedded in VM metadata | `aws_key_pair` |
| Startup script | GCP metadata service | Template file injected via `user_data` |
| Runtime detection | `metadata.google.internal` | `169.254.169.254` (IMDS) |
| Image | `google_compute_image` (Ubuntu) | `aws_ami` data source |

## Biến môi trường (sau khi triển khai)

Đặt trong `.env` trên master VM:
```bash
NEXUS_RUNTIME_MODE=vm          # Sử dụng /data/ thay vì runtime/
```
