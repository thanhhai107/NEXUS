terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0, < 7.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0, < 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# =============================================================================
# VARIABLES
# =============================================================================

variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-southeast-1"
}

variable "master_zone" {
  description = "AWS availability zone for the master node."
  type        = string
  default     = "ap-southeast-1a"
}

variable "worker_zones" {
  description = "AWS availability zones for worker nodes, in worker index order."
  type        = list(string)
  default = [
    "ap-southeast-1a",
    "ap-southeast-1a",
    "ap-southeast-1a",
    "ap-southeast-1a"
  ]
}

variable "cluster_name" {
  description = "Resource name prefix."
  type        = string
  default     = "nexus"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC. Set to '' to use the default VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "use_default_vpc" {
  description = "Use the existing default VPC instead of creating a new one."
  type        = bool
  default     = false
}

variable "instance_type" {
  description = "Fallback EC2 instance type for all nodes when role-specific types are not set."
  type        = string
  default     = "m5.xlarge"
}

variable "master_instance_type" {
  description = "EC2 instance type for the master node. Leave empty to use instance_type."
  type        = string
  default     = ""
}

variable "worker_instance_type" {
  description = "EC2 instance type for worker nodes. Leave empty to use instance_type."
  type        = string
  default     = ""
}

variable "worker_count" {
  description = "Number of worker nodes."
  type        = number
  default     = 3
}

variable "ebs_root_size_gb" {
  description = "Root EBS volume size for each node (GB)."
  type        = number
  default     = 200
}

variable "ebs_root_type" {
  description = "Root EBS volume type for each node."
  type        = string
  default     = "gp3"
}

variable "allowed_admin_cidrs" {
  description = "CIDR ranges allowed to access SSH and public admin UIs."
  type        = list(string)
}

variable "ssh_user" {
  description = "Linux SSH user used in output helper commands."
  type        = string
  default     = "ubuntu"
}

variable "ssh_public_key" {
  description = "SSH public key, for example the content of ~/.ssh/nexus_aws.pub."
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_master_worker_ssh" {
  description = "Generate an internal SSH key so the master VM can SSH to private worker VMs without a password."
  type        = bool
  default     = true
}

variable "enable_ssh_password_login" {
  description = "Enable SSH password authentication for ssh_user."
  type        = bool
  default     = false
}

variable "ssh_password" {
  description = "Password for ssh_user when enable_ssh_password_login is true."
  type        = string
  default     = ""
  sensitive   = true
}

variable "nexus_repo_url" {
  description = "Git URL for the Nexus repo. Leave empty to skip provisioning this repo on VMs."
  type        = string
  default     = "https://github.com/thanhhai107/NEXUS.git"
}

variable "nexus_repo_ref" {
  description = "Git branch, tag, or commit to checkout for the Nexus repo."
  type        = string
  default     = "master"
}

variable "create_elastic_ip" {
  description = "Allocate and attach an Elastic IP to the master instance."
  type        = bool
  default     = true
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  cluster_tag = "nexus-cluster"
  master_tag  = "nexus-master"
  worker_tag  = "nexus-worker"

  tags = {
    Project = "nexus"
    Cluster = var.cluster_name
  }

  master_tags = merge(local.tags, { Role = "master" })
  worker_tags = merge(local.tags, { Role = "worker" })

  master_instance_type = var.master_instance_type != "" ? var.master_instance_type : var.instance_type
  worker_instance_type = var.worker_instance_type != "" ? var.worker_instance_type : var.instance_type

  worker_zones = [
    for index in range(var.worker_count) : var.worker_zones[index % length(var.worker_zones)]
  ]
}

# =============================================================================
# SSH KEY
# =============================================================================

resource "tls_private_key" "master_worker" {
  count     = var.enable_master_worker_ssh ? 1 : 0
  algorithm = "ED25519"
}

output "master_worker_ssh_private_key" {
  description = "Internal private key installed on the master when enable_master_worker_ssh is true. Stored in Terraform state."
  value       = var.enable_master_worker_ssh ? tls_private_key.master_worker[0].private_key_openssh : ""
  sensitive   = true
}

# =============================================================================
# KEY PAIR
# =============================================================================

resource "aws_key_pair" "nexus" {
  key_name   = "${var.cluster_name}-key"
  public_key = var.ssh_public_key
}

# =============================================================================
# NETWORKING
# =============================================================================

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_vpc" "default" {
  count   = var.use_default_vpc ? 1 : 0
  default = true
}

locals {
  vpc_id = var.use_default_vpc ? data.aws_vpc.default[0].id : aws_vpc.nexus[0].id
}

resource "aws_vpc" "nexus" {
  count = var.use_default_vpc ? 0 : 1

  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.tags, { Name = "${var.cluster_name}-vpc" })
}

resource "aws_subnet" "public" {
  count = var.use_default_vpc ? 0 : length(distinct(local.worker_zones))

  vpc_id                  = aws_vpc.nexus[0].id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone       = distinct(local.worker_zones)[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.tags, { Name = "${var.cluster_name}-public-${count.index + 1}" })
}

resource "aws_subnet" "private" {
  count = var.use_default_vpc ? 0 : length(distinct(local.worker_zones))

  vpc_id            = aws_vpc.nexus[0].id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + length(distinct(local.worker_zones)))
  availability_zone = distinct(local.worker_zones)[count.index]

  tags = merge(local.tags, { Name = "${var.cluster_name}-private-${count.index + 1}" })
}

resource "aws_internet_gateway" "nexus" {
  count  = var.use_default_vpc ? 0 : 1
  vpc_id = aws_vpc.nexus[0].id

  tags = merge(local.tags, { Name = "${var.cluster_name}-igw" })
}

resource "aws_route_table" "public" {
  count  = var.use_default_vpc ? 0 : 1
  vpc_id = aws_vpc.nexus[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.nexus[0].id
  }

  tags = merge(local.tags, { Name = "${var.cluster_name}-public-rt" })
}

resource "aws_eip" "nat" {
  count  = var.use_default_vpc ? 0 : 1
  domain = "vpc"

  tags = merge(local.tags, { Name = "${var.cluster_name}-nat-eip" })
}

resource "aws_nat_gateway" "nexus" {
  count         = var.use_default_vpc ? 0 : 1
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = merge(local.tags, { Name = "${var.cluster_name}-nat" })
}

resource "aws_route_table" "private" {
  count  = var.use_default_vpc ? 0 : 1
  vpc_id = aws_vpc.nexus[0].id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nexus[0].id
  }

  tags = merge(local.tags, { Name = "${var.cluster_name}-private-rt" })
}

resource "aws_route_table_association" "public" {
  count = var.use_default_vpc ? 0 : length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table_association" "private" {
  count = var.use_default_vpc ? 0 : length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

# =============================================================================
# SECURITY GROUPS
# =============================================================================

resource "aws_security_group" "ssh" {
  name        = "${var.cluster_name}-allow-ssh"
  description = "Allow SSH from admin CIDRs"
  vpc_id      = local.vpc_id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.cluster_name}-sg-ssh" })
}

resource "aws_security_group" "master_ui" {
  name        = "${var.cluster_name}-allow-master-ui"
  description = "Allow master UI ports from admin CIDRs"
  vpc_id      = local.vpc_id

  ingress {
    description = "Nexus API"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  ingress {
    description = "Airflow"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  ingress {
    description = "Spark Master UI"
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  ingress {
    description = "Trino"
    from_port   = 8085
    to_port     = 8085
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  ingress {
    description = "Superset"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  ingress {
    description = "MinIO Console"
    from_port   = 9001
    to_port     = 9001
    protocol    = "tcp"
    cidr_blocks = var.allowed_admin_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.cluster_name}-sg-master-ui" })
}

resource "aws_security_group" "internal" {
  name        = "${var.cluster_name}-allow-internal"
  description = "Allow all traffic within the cluster"
  vpc_id      = local.vpc_id

  ingress {
    description = "Internal all TCP"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Internal all UDP"
    from_port   = 0
    to_port     = 65535
    protocol    = "udp"
    self        = true
  }

  ingress {
    description = "Internal ICMP"
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.cluster_name}-sg-internal" })
}

# =============================================================================
# IAM ROLE & INSTANCE PROFILE
# =============================================================================

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "nexus" {
  name               = "${var.cluster_name}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.nexus.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_logs" {
  role       = aws_iam_role.nexus.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_instance_profile" "nexus" {
  name = "${var.cluster_name}-instance-profile"
  role = aws_iam_role.nexus.name
}

# =============================================================================
# AMI
# =============================================================================

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  owners = ["099720109477"] # Canonical
}

# =============================================================================
# ELASTIC IP
# =============================================================================

resource "aws_eip" "master" {
  count    = var.create_elastic_ip ? 1 : 0
  instance = aws_instance.master.id
  domain   = "vpc"

  tags = merge(local.tags, { Name = "${var.cluster_name}-master-eip" })
}

# =============================================================================
# STARTUP SCRIPT (template file)
# =============================================================================

locals {
  startup_vars = {
    cluster_name                  = var.cluster_name
    worker_count                  = var.worker_count
    nexus_repo_url                = var.nexus_repo_url
    nexus_repo_ref                = var.nexus_repo_ref
    ssh_user                      = var.ssh_user
    enable_ssh_password_login     = var.enable_ssh_password_login
    ssh_password                  = var.ssh_password
    enable_master_worker_ssh      = var.enable_master_worker_ssh
    master_worker_private_key_b64 = var.enable_master_worker_ssh ? base64encode(tls_private_key.master_worker[0].private_key_openssh) : ""
  }
}

# =============================================================================
# EC2 INSTANCES
# =============================================================================

resource "aws_instance" "master" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = local.master_instance_type
  key_name                    = aws_key_pair.nexus.key_name
  subnet_id                   = var.use_default_vpc ? null : aws_subnet.public[0].id
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.nexus.name

  vpc_security_group_ids = [
    aws_security_group.ssh.id,
    aws_security_group.master_ui.id,
    aws_security_group.internal.id,
  ]

  root_block_device {
    volume_size = var.ebs_root_size_gb
    volume_type = var.ebs_root_type
    encrypted   = true
  }

  tags = merge(local.master_tags, { Name = "${var.cluster_name}-master-1" })

  user_data_base64 = base64encode(templatefile("${path.module}/scripts/startup.sh", merge(local.startup_vars, {
    nexus_node_role   = "master"
    nexus_node_index  = ""
    master_private_ip = ""
  })))
}

resource "aws_instance" "workers" {
  count = var.worker_count

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = local.worker_instance_type
  key_name                    = aws_key_pair.nexus.key_name
  subnet_id                   = var.use_default_vpc ? null : aws_subnet.private[count.index % length(aws_subnet.private)].id
  associate_public_ip_address = false
  iam_instance_profile        = aws_iam_instance_profile.nexus.name

  vpc_security_group_ids = [
    aws_security_group.ssh.id,
    aws_security_group.internal.id,
  ]

  root_block_device {
    volume_size = var.ebs_root_size_gb
    volume_type = var.ebs_root_type
    encrypted   = true
  }

  tags = merge(local.worker_tags, { Name = "${var.cluster_name}-worker-${count.index + 1}" })

  user_data_base64 = base64encode(templatefile("${path.module}/scripts/startup.sh", merge(local.startup_vars, {
    nexus_node_role   = "worker"
    nexus_node_index  = tostring(count.index + 1)
    master_private_ip = aws_instance.master.private_ip
  })))
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "master_static_ip" {
  description = "Master node public IP address."
  value       = aws_instance.master.public_ip
}

output "master" {
  value = {
    name       = aws_instance.master.tags["Name"]
    zone       = aws_instance.master.availability_zone
    private_ip = aws_instance.master.private_ip
    public_ip  = aws_instance.master.public_ip
    ssh        = "ssh ${var.ssh_user}@${aws_instance.master.public_ip}"
  }
}

output "workers" {
  value = [
    for worker in aws_instance.workers : {
      name       = worker.tags["Name"]
      zone       = worker.availability_zone
      private_ip = worker.private_ip
      public_ip  = ""
      ssh        = "ssh -J ${var.ssh_user}@${aws_instance.master.public_ip} ${var.ssh_user}@${worker.private_ip}"
    }
  ]
}

output "service_urls" {
  value = {
    nexus_api           = "http://${aws_instance.master.public_ip}:8000/docs"
    nexus_airflow       = "http://${aws_instance.master.public_ip}:8080"
    nexus_spark_master  = "http://${aws_instance.master.public_ip}:8081"
    nexus_trino         = "http://${aws_instance.master.public_ip}:8085"
    nexus_superset      = "http://${aws_instance.master.public_ip}:8088"
    nexus_minio_console = "http://${aws_instance.master.public_ip}:9001"
  }
}

output "nexus_tunnel_command" {
  description = "Use this when you need local access to Nexus services without exposing them publicly."
  value       = "ssh -L 8000:127.0.0.1:8000 -L 8080:127.0.0.1:8080 -L 8081:127.0.0.1:8081 -L 8085:127.0.0.1:8085 -L 8088:127.0.0.1:8088 -L 9000:127.0.0.1:9000 -L 9001:127.0.0.1:9001 ${var.ssh_user}@${aws_instance.master.public_ip}"
}
