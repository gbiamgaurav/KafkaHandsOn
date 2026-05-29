output "bootstrap_address" {
  description = "Kafka bootstrap address — use as KAFKA_BOOTSTRAP env var"
  value = join("", [
    "bootstrap.",
    var.cluster_name,
    ".",
    var.region,
    ".managedkafka.googleapis.com:9092"
  ])
}

output "vm_external_ip" {
  description = "External IP of the application VM"
  value       = google_compute_instance.kafka_app.network_interface[0].access_config[0].nat_ip
}

output "bucket_name" {
  description = "GCS bucket holding the uploaded application code"
  value       = google_storage_bucket.app_code.name
}

output "service_account_email" {
  description = "Service account used by the VM"
  value       = google_service_account.kafka_app.email
}

output "ssh_command" {
  description = "SSH into the application VM"
  value       = "gcloud compute ssh kafka-app-vm --zone=${var.zone} --project=${var.project_id}"
}

output "watch_startup_log" {
  description = "Stream the VM startup log (includes Kafka verify result)"
  value       = "gcloud compute ssh kafka-app-vm --zone=${var.zone} --project=${var.project_id} --command='sudo tail -f /var/log/kafka-app.log'"
}

output "check_consumer_lag" {
  description = "Check Kafka consumer group lag"
  value       = "gcloud managed-kafka consumer-groups list --cluster=${var.cluster_name} --location=${var.region} --project=${var.project_id}"
}

output "list_topics" {
  description = "List all Kafka topics"
  value       = "gcloud managed-kafka topics list --cluster=${var.cluster_name} --location=${var.region} --project=${var.project_id}"
}
