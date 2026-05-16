terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0, < 8.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0, < 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.master_zone
}

variable "project_id" {
  description = "Google Cloud project ID."
  type        = string
}

variable "enable_required_apis" {
  description = "Enable Compute and IAM APIs when permitted."
  type        = bool
  default     = true
}

variable "region" {
  description = "Google Cloud region."
  type        = string
  default     = "asia-southeast1"
}

variable "zone" {
  description = "Deprecated. Use master_zone and worker_zones."
  type        = string
  default     = null
}

variable "master_zone" {
  description = "Google Cloud zone for the master node."
  type        = string
  default     = "asia-southeast1-c"
}

variable "worker_zones" {
  description = "Google Cloud zones for worker nodes, in worker index order."
  type        = list(string)
  default = [
    "asia-southeast1-c",
    "asia-southeast1-c",
    "asia-southeast1-c",
    "asia-southeast1-c"
  ]
}

variable "cluster_name" {
  description = "Resource name prefix."
  type        = string
  default     = "nexus"
}

variable "network_name" {
  description = "Existing VPC network name."
  type        = string
  default     = "default"
}

variable "machine_type" {
  description = "Machine type for all nodes."
  type        = string
  default     = "e2-custom-12-16384"
}

variable "worker_count" {
  description = "Number of worker nodes."
  type        = number
  default     = 4
}

variable "boot_disk_size_gb" {
  description = "Boot disk size for each node."
  type        = number
  default     = 200
}

variable "boot_disk_type" {
  description = "Boot disk type for each node."
  type        = string
  default     = "pd-balanced"
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
  description = "SSH public key added to VM metadata, for example the content of ~/.ssh/nexus_gcp.pub."
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_oslogin" {
  description = "Enable Google OS Login on VM instances."
  type        = bool
  default     = false
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

variable "docker_elk_repo_url" {
  description = "Git URL for the Amazon Search demo repo. Leave empty to skip provisioning this repo on VMs."
  type        = string
  default     = "https://github.com/thanhhai107/docker-elk.git"
}

variable "docker_elk_repo_ref" {
  description = "Git branch, tag, or commit to checkout for the Amazon Search demo repo."
  type        = string
  default     = "main"
}


locals {
  cluster_tag = "${var.cluster_name}-cluster"
  master_tag  = "${var.cluster_name}-master"
  worker_tag  = "${var.cluster_name}-worker"

  common_metadata = {
    nexus-cluster-name  = var.cluster_name
    nexus-worker-count  = tostring(var.worker_count)
    nexus-repo-ref      = var.nexus_repo_ref
    nexus-repo-url      = var.nexus_repo_url
    docker-elk-repo-ref = var.docker_elk_repo_ref
    docker-elk-repo-url = var.docker_elk_repo_url
    ssh-password-login  = var.enable_ssh_password_login ? "TRUE" : "FALSE"
    ssh-password        = var.ssh_password
    ssh-user            = var.ssh_user
    enable-oslogin      = var.enable_oslogin ? "TRUE" : "FALSE"
  }

  admin_ssh_keys = var.ssh_public_key == "" ? [] : [
    "${var.ssh_user}:${var.ssh_public_key}"
  ]
  master_worker_ssh_keys = var.enable_master_worker_ssh ? [
    "${var.ssh_user}:${tls_private_key.master_worker[0].public_key_openssh}"
  ] : []
  ssh_keys = concat(local.admin_ssh_keys, local.master_worker_ssh_keys)

  optional_ssh_metadata = length(local.ssh_keys) == 0 ? {} : {
    ssh-keys = join("\n", local.ssh_keys)
  }
  master_worker_private_key_metadata = var.enable_master_worker_ssh ? {
    nexus-master-worker-private-key-b64 = base64encode(tls_private_key.master_worker[0].private_key_openssh)
  } : {}

  worker_zones = [
    for index in range(var.worker_count) : var.worker_zones[index % length(var.worker_zones)]
  ]
}

resource "tls_private_key" "master_worker" {
  count     = var.enable_master_worker_ssh ? 1 : 0
  algorithm = "ED25519"
}

output "master_worker_ssh_private_key" {
  description = "Internal private key installed on the master when enable_master_worker_ssh is true. Stored in Terraform state."
  value       = var.enable_master_worker_ssh ? tls_private_key.master_worker[0].private_key_openssh : ""
  sensitive   = true
}

resource "google_project_service" "required" {
  for_each = var.enable_required_apis ? toset([
    "compute.googleapis.com",
    "iam.googleapis.com"
  ]) : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

data "google_compute_network" "selected" {
  name = var.network_name

  depends_on = [
    google_project_service.required
  ]
}

data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2204-lts"
  project = "ubuntu-os-cloud"

  depends_on = [
    google_project_service.required
  ]
}

resource "google_service_account" "vm" {
  account_id   = "${var.cluster_name}-vm"
  display_name = "NEXUS VM service account"

  depends_on = [
    google_project_service.required
  ]
}

resource "google_compute_firewall" "ssh" {
  name          = "${var.cluster_name}-allow-ssh"
  network       = data.google_compute_network.selected.name
  source_ranges = var.allowed_admin_cidrs
  target_tags   = [local.cluster_tag]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "master_ui" {
  name          = "${var.cluster_name}-allow-master-ui"
  network       = data.google_compute_network.selected.name
  source_ranges = var.allowed_admin_cidrs
  target_tags   = [local.master_tag]

  allow {
    protocol = "tcp"
    ports    = ["5601", "8000", "8501", "8080", "8085", "8088"]
  }
}

resource "google_compute_firewall" "minio_console" {
  name          = "${var.cluster_name}-allow-minio-console"
  network       = data.google_compute_network.selected.name
  source_ranges = var.allowed_admin_cidrs
  target_tags   = [local.worker_tag]

  allow {
    protocol = "tcp"
    ports    = ["9001"]
  }
}

resource "google_compute_firewall" "internal" {
  name        = "${var.cluster_name}-allow-internal"
  network     = data.google_compute_network.selected.name
  source_tags = [local.cluster_tag]
  target_tags = [local.cluster_tag]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_compute_firewall" "internal_cidr" {
  name          = "${var.cluster_name}-allow-internal-cidr"
  network       = data.google_compute_network.selected.name
  priority      = 100
  source_ranges = ["10.0.0.0/8"]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_compute_router" "nat" {
  name    = "${var.cluster_name}-nat-router"
  network = data.google_compute_network.selected.self_link
  region  = var.region
}

resource "google_compute_router_nat" "workers" {
  name                               = "${var.cluster_name}-workers-nat"
  router                             = google_compute_router.nat.name
  region                             = google_compute_router.nat.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_compute_instance" "master" {
  name         = "${var.cluster_name}-master-1"
  machine_type = var.machine_type
  zone         = var.master_zone
  tags         = [local.cluster_tag, local.master_tag]

  labels = {
    project = "nexus"
    cluster = var.cluster_name
    role    = "master"
  }

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.boot_disk_size_gb
      type  = var.boot_disk_type
    }
  }

  network_interface {
    network = data.google_compute_network.selected.self_link
    access_config {
      nat_ip = google_compute_address.master_ip.address
    }
  }

  metadata = merge(local.common_metadata, local.optional_ssh_metadata, local.master_worker_private_key_metadata, {
    nexus-node-role = "master"
  })

  metadata_startup_script = file("${path.module}/scripts/startup.sh")

  service_account {
    email  = google_service_account.vm.email
    scopes = ["https://www.googleapis.com/auth/logging.write", "https://www.googleapis.com/auth/monitoring.write"]
  }
}

resource "google_compute_instance" "workers" {
  count        = var.worker_count
  name         = "${var.cluster_name}-worker-${count.index + 1}"
  machine_type = var.machine_type
  zone         = local.worker_zones[count.index]
  tags         = [local.cluster_tag, local.worker_tag]

  labels = {
    project = "nexus"
    cluster = var.cluster_name
    role    = "worker"
  }

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = var.boot_disk_size_gb
      type  = var.boot_disk_type
    }
  }

  network_interface {
    network = data.google_compute_network.selected.self_link
  }

  metadata = merge(local.common_metadata, local.optional_ssh_metadata, {
    nexus-node-role  = "worker"
    nexus-node-index = tostring(count.index + 1)
  })

  metadata_startup_script = file("${path.module}/scripts/startup.sh")

  service_account {
    email  = google_service_account.vm.email
    scopes = ["https://www.googleapis.com/auth/logging.write", "https://www.googleapis.com/auth/monitoring.write"]
  }
}

resource "google_compute_address" "master_ip" {
  name   = "nexus-master-ip"
  region = var.region
}

output "master_static_ip" {
  value = google_compute_address.master_ip.address
}

output "master" {
  value = {
    name       = google_compute_instance.master.name
    zone       = google_compute_instance.master.zone
    private_ip = google_compute_instance.master.network_interface[0].network_ip
    public_ip  = google_compute_instance.master.network_interface[0].access_config[0].nat_ip
    ssh        = "ssh ${var.ssh_user}@${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}"
  }
}

output "workers" {
  value = [
    for worker in google_compute_instance.workers : {
      name       = worker.name
      zone       = worker.zone
      private_ip = worker.network_interface[0].network_ip
      public_ip  = ""
      ssh        = "ssh -J ${var.ssh_user}@${google_compute_instance.master.network_interface[0].access_config[0].nat_ip} ${var.ssh_user}@${worker.network_interface[0].network_ip}"
    }
  ]
}

output "service_urls" {
  value = {
    amazon_search_streamlit = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8501"
    amazon_search_fastapi   = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8000"
    amazon_search_docs      = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8000/docs"
    amazon_search_kibana    = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:5601"
    nexus_airflow           = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8080"
    nexus_trino             = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8085"
    nexus_superset          = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8088"
  }
}

output "search_engine_tunnel_command" {
  description = "Use this when you need local access to PostgreSQL, Elasticsearch, Meilisearch, or Kibana without exposing them publicly."
  value       = "ssh -L 5432:127.0.0.1:5432 -L 9200:127.0.0.1:9200 -L 7700:127.0.0.1:7700 -L 5601:127.0.0.1:5601 ${var.ssh_user}@${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}"
}
