terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file(var.ssh_key_file)
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

# Enable Required APIs
resource "google_project_service" "cloudbuild" {
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "composer" {
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute_api" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage_api" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery_api" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "container_api" {
  service            = "container.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iap" {
  service            = "iap.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute_iap" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# Reserve a static external IP address
resource "google_compute_address" "static_ip" {
  name   = "${var.vm_name}-static-ip"
  region = var.region
}

# Compute Engine VM
resource "google_compute_instance" "de-fi-vm" {
  name         = var.vm_name
  machine_type = var.machine_type

  deletion_protection = false

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.static_ip.address
    }
  }

  tags = ["airflow-access"]
}

# Firewall rule to allow HTTP and HTTPS traffic (for reverse proxy)
resource "google_compute_firewall" "allow_http_https" {
  name    = "allow-http-https"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["airflow-access"]
}

# Firewall rule to allow access to Airflow UI on port 8080
resource "google_compute_firewall" "allow_airflow_ui" {
  name    = "allow-airflow-ui"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"] # For public access; consider restricting for security
  target_tags   = ["airflow-access"]
}

# Google Cloud Storage Bucket
resource "google_storage_bucket" "data_bucket" {
  name          = var.storage_bucket_name
  location      = var.region
  force_destroy = true
}

# BigQuery Dataset
resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = var.bigquery_dataset_id
  location   = var.region
}

# Cloud Build Trigger
resource "google_cloudbuild_trigger" "github_trigger" {
  name = "github-trigger"

  github {
    owner = var.github_owner
    name  = var.github_repo

    push {
      branch_regex = var.build_trigger_branch  # Use branch_regex for regex patterns
    }
  }

  build {
    steps {
      name = "gcr.io/cloud-builders/docker"
      args = [
        "build",
        "-t",
        "gcr.io/${var.project_id}/${var.docker_image_name}:$SHORT_SHA",
        "."
      ]
    }

    images = ["gcr.io/${var.project_id}/${var.docker_image_name}:$SHORT_SHA"]
  }

  substitutions = {
    SHORT_SHA = "$SHORT_SHA"
  }
}

# Google Cloud Composer Environment
resource "google_composer_environment" "my_composer_env" {
  name   = var.composer_env_name
  region = var.region

  config {
    #     node_count = var.composer_node_count

    software_config {
      image_version = var.composer_image_version

      env_variables = var.composer_env_variables

      pypi_packages = var.composer_pypi_packages
    }

    private_environment_config {
      enable_private_endpoint = true
      master_ipv4_cidr_block  = "10.0.0.0/24"

      #       network    = data.google_compute_network.composer_network.name
      #       subnetwork = data.google_compute_subnetwork.composer_subnetwork.name
    }
  }
}

# Grant Cloud Composer v2 API Service Agent Extension role to the Composer service agent
# resource "google_project_iam_member" "composer_service_agent_extension" {
#   project = var.project_id
#   role    = "roles/composer.serviceAgentExtension"
#   member  = "serviceAccount:service-781183121559@cloudcomposer-accounts.iam.gserviceaccount.com" # de-fi-244@sapient-hub-442421-b5.iam.gserviceaccount.com"
# }

