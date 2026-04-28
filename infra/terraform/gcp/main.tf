terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0, < 8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
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
  description = "Google Cloud zone."
  type        = string
  default     = "asia-southeast1-a"
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


locals {
  cluster_tag = "${var.cluster_name}-cluster"
  master_tag  = "${var.cluster_name}-master"
  worker_tag  = "${var.cluster_name}-worker"

  common_metadata = {
    nexus-cluster-name = var.cluster_name
    enable-oslogin     = var.enable_oslogin ? "TRUE" : "FALSE"
  }

  optional_ssh_metadata = var.ssh_public_key == "" ? {} : {
    ssh-keys = "${var.ssh_user}:${var.ssh_public_key}"
  }
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
    ports    = ["8000", "8080", "8085", "8088"]
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

resource "google_compute_instance" "master" {
  name         = "${var.cluster_name}-master-1"
  machine_type = var.machine_type
  zone         = var.zone
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

  metadata = merge(local.common_metadata, local.optional_ssh_metadata, {
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
  zone         = var.zone
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
    access_config {}
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
    private_ip = google_compute_instance.master.network_interface[0].network_ip
    public_ip  = google_compute_instance.master.network_interface[0].access_config[0].nat_ip
    ssh        = "ssh ${var.ssh_user}@${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}"
  }
}

output "workers" {
  value = [
    for worker in google_compute_instance.workers : {
      name       = worker.name
      private_ip = worker.network_interface[0].network_ip
      public_ip  = worker.network_interface[0].access_config[0].nat_ip
      ssh        = "ssh ${var.ssh_user}@${worker.network_interface[0].access_config[0].nat_ip}"
    }
  ]
}

output "service_urls" {
  value = {
    airflow  = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8080"
    trino    = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8085"
    superset = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8088"
    fastapi  = "http://${google_compute_instance.master.network_interface[0].access_config[0].nat_ip}:8000"
  }
}

