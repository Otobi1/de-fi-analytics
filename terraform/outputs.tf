# --------------------------------------------
# Output the name of the VM
# --------------------------------------------
output "vm_name" {
  description = "The name of the VM instance"
  value       = google_compute_instance.de-fi-vm.name
}

# --------------------------------------------
# Output the public IP address of the VM
# --------------------------------------------
output "vm_public_ip" {
  description = "The public IP address of the VM"
  value       = google_compute_instance.de-fi-vm.network_interface[0].access_config[0].nat_ip
}

# --------------------------------------------
# Output the static IP address of the VM
# --------------------------------------------
output "vm_static_ip" {
  description = "The static IP address of the VM instance"
  value       = google_compute_address.static_ip.address
}

# --------------------------------------------
# Output the name of the storage bucket
# --------------------------------------------
output "storage_bucket_name" {
  description = "The name of the GCS storage bucket"
  value       = google_storage_bucket.data_bucket.name
}

# --------------------------------------------
# Output the BigQuery dataset ID
# --------------------------------------------
output "bigquery_dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.my_dataset.dataset_id
}

# --------------------------------------------
# Output the Git repository URL
# --------------------------------------------
output "git_repo_url" {
  description = "The Git repository URL where Airflow DAGs and DBT projects are stored"
  value       = var.git_repo_url
}

# --------------------------------------------
# Optional: Output Cloud Build Trigger Details
# --------------------------------------------
output "cloudbuild_trigger_id" {
  description = "The ID of the Cloud Build trigger"
  value       = google_cloudbuild_trigger.github_trigger.id
}

output "cloudbuild_trigger_name" {
  description = "The name of the Cloud Build trigger"
  value       = google_cloudbuild_trigger.github_trigger.name
}

# --------------------------------------------
# Optional: Output Cloud Composer Environment Details
# --------------------------------------------
# output "composer_env_name" {
#   description = "The name of the Cloud Composer environment"
#   value       = google_composer_environment.my_composer_env.name
# }

# output "composer_env_uri" {
#   description = "The URI of the Cloud Composer environment's Airflow web interface"
#   value       = google_composer_environment.my_composer_env.config.0.software_config.0.airflow_uri
# }
