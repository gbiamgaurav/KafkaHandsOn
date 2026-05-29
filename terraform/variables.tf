variable "project_id" {
  type        = string
  description = "GCP project ID where all resources will be created"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region for Kafka cluster and VM"
}

variable "zone" {
  type        = string
  default     = "us-central1-a"
  description = "GCP zone for the compute instance"
}

variable "cluster_name" {
  type        = string
  default     = "kafka-handson"
  description = "Name for the Managed Kafka cluster"
}

variable "vm_machine_type" {
  type        = string
  default     = "e2-medium"
  description = "GCE machine type for the application VM"
}

variable "groq_api_key" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Groq API key for GenAI pipeline demo (leave empty to run in mock mode)"
}
