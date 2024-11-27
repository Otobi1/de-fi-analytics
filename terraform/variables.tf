# --------------------------------------------
# Existing Variables
# --------------------------------------------

# Define the GCP project ID
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

# Define the region
variable "region" {
  description = "The region where resources will be created"
  type        = string
}

# Define the zone for VM
variable "zone" {
  description = "The zone where the VM instance will be deployed"
  type        = string
}

# Define the VM name
variable "vm_name" {
  description = "The name of the VM instance"
  type        = string
}

# Define the machine type for the VM
variable "machine_type" {
  description = "Machine type for the compute instance"
  type        = string
  default     = "n1-standard-1" # Optional: Provide a default value
}

# Define the storage bucket name
variable "storage_bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

# Define the BigQuery dataset ID
variable "bigquery_dataset_id" {
  description = "The ID for the BigQuery dataset"
  type        = string
}

# Define the SSH key file for accessing the VM
variable "ssh_key_file" {
  description = "Path to the SSH private key file"
  type        = string
}

# --------------------------------------------
# New Variables for Google Cloud Build
# --------------------------------------------

# Define the GitHub repository owner (user or organization)
variable "github_owner" {
  description = "GitHub repository owner (user or organization)"
  type        = string
}

# Define the GitHub repository name
variable "github_repo" {
  description = "GitHub repository name"
  type        = string
}

# Optional: Define the branch to trigger builds (default is '^main$')
variable "build_trigger_branch" {
  description = "Branch name to trigger Cloud Build (regex supported)"
  type        = string
  default     = "^main$"
}

# Optional: Define the Docker image name for Cloud Build (default is 'my-app')
variable "docker_image_name" {
  description = "Docker image name to build and push"
  type        = string
  default     = "my-app"
}

# Define the GitHub repository URL (for output)
variable "git_repo_url" {
  description = "URL of the GitHub repository"
  type        = string
}

# --------------------------------------------
# New Variables for Google Cloud Composer
# --------------------------------------------

# Define the Cloud Composer environment name
variable "composer_env_name" {
  description = "Name of the Cloud Composer environment"
  type        = string
  default     = "my-composer-environment"
}

# Optional: Define the number of nodes for Composer
variable "composer_node_count" {
  description = "Number of nodes in the Composer environment"
  type        = number
  default     = 3
}

# Optional: Define the Composer image version
variable "composer_image_version" {
  description = "Composer image version (e.g., composer-2.0.0-airflow-2.3.0)"
  type        = string
  default     = "composer-2.0.0-airflow-2.3.0"
}

# Optional: Define environment variables for Composer
variable "composer_env_variables" {
  description = "Environment variables for the Composer environment"
  type        = map(string)
  default = {
    "DBT_PROFILES_DIR" = "/home/airflow/.dbt"
    # "ANOTHER_VAR"      = "value"  # Replace or add additional environment variables as needed
  }
}

# Optional: Define PyPI packages to install in Composer
variable "composer_pypi_packages" {
  description = "Python packages to install in the Composer environment"
  type        = map(string)
  default = {
    "dbt"      = ">=0.20.0"
    "requests" = ">=2.25.1" # Add other Python packages as needed
  }
}

# # Optional: Define network configurations for Composer
# variable "composer_network" {
#   description = "VPC network for the Composer environment"
#   type        = string
#   default     = "composer-network"  # Desired network name
# }
#
# variable "composer_subnetwork" {
#   description = "Subnetwork for the Composer environment"
#   type        = string
#   default     = "composer-subnetwork"  # Desired subnetwork name
# }
