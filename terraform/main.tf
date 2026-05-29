# ── APIs ─────────────────────────────────────────────────────────────────────
resource "google_project_service" "managedkafka" {
  service            = "managedkafka.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# ── Service Account ───────────────────────────────────────────────────────────
resource "google_service_account" "kafka_app" {
  account_id   = "kafka-app-sa"
  display_name = "Kafka HandsOn App"
}

resource "google_project_iam_member" "kafka_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.kafka_app.email}"
}

resource "google_project_iam_member" "storage_reader" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.kafka_app.email}"
}

resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.kafka_app.email}"
}

# ── GCS Bucket (application code) ────────────────────────────────────────────
resource "google_storage_bucket" "app_code" {
  name                        = "${var.project_id}-kafka-handson"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [google_project_service.storage]
}

resource "google_storage_bucket_object" "producer" {
  name   = "app/producer.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../producer.py"
}

resource "google_storage_bucket_object" "consumer_ml" {
  name   = "app/consumer_ml.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../consumer_ml.py"
}

resource "google_storage_bucket_object" "simple_producer" {
  name   = "app/simple_producer.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../simple_producer.py"
}

resource "google_storage_bucket_object" "simple_processor" {
  name   = "app/simple_processor.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../simple_processor.py"
}

resource "google_storage_bucket_object" "genai_pipeline" {
  name   = "app/genai_pipeline.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../genai_pipeline.py"
}

resource "google_storage_bucket_object" "llm_multi_api" {
  name   = "app/llm_multi_api.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../llm_multi_api.py"
}

resource "google_storage_bucket_object" "requirements" {
  name   = "app/requirements.txt"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../requirements.txt"
}

resource "google_storage_bucket_object" "verify_kafka" {
  name   = "app/verify_kafka.py"
  bucket = google_storage_bucket.app_code.name
  source = "${path.module}/../verify_kafka.py"
}

# ── Managed Kafka Cluster ─────────────────────────────────────────────────────
resource "google_managed_kafka_cluster" "main" {
  cluster_id = var.cluster_name
  location   = var.region

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472 # 3 GiB
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"
      }
    }
  }

  labels = {
    env     = "dev"
    usecase = "handson"
  }

  depends_on = [google_project_service.managedkafka]
}

# ── Kafka Topics ──────────────────────────────────────────────────────────────
resource "google_managed_kafka_topic" "raw_transactions" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "raw-transactions"
  location           = var.region
  partition_count    = 3
  replication_factor = 3
}

resource "google_managed_kafka_topic" "flagged_transactions" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "flagged-transactions"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "approved_transactions" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "approved-transactions"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "orders" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "orders"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "large_orders" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "large-orders"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "normal_orders" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "normal-orders"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "llm_requests" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "llm-requests"
  location           = var.region
  partition_count    = 3
  replication_factor = 3
}

resource "google_managed_kafka_topic" "llm_responses" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "llm-responses"
  location           = var.region
  partition_count    = 3
  replication_factor = 3
}

resource "google_managed_kafka_topic" "feedback_events" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "feedback-events"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

resource "google_managed_kafka_topic" "verify_test" {
  cluster            = google_managed_kafka_cluster.main.cluster_id
  topic_id           = "kafka-verify-test"
  location           = var.region
  partition_count    = 1
  replication_factor = 3
}

# ── Firewall: SSH ─────────────────────────────────────────────────────────────
resource "google_compute_firewall" "allow_ssh" {
  name    = "kafka-handson-allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka-app"]

  depends_on = [google_project_service.compute]
}

# ── Application VM ────────────────────────────────────────────────────────────
resource "google_compute_instance" "kafka_app" {
  name         = "kafka-app-vm"
  machine_type = var.vm_machine_type
  zone         = var.zone

  tags = ["kafka-app"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  network_interface {
    network = "default"
    access_config {} # assigns an external IP
  }

  service_account {
    email  = google_service_account.kafka_app.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script = templatefile("${path.module}/scripts/startup.sh", {
      cluster_name  = var.cluster_name
      region        = var.region
      project_id    = var.project_id
      bucket_name   = google_storage_bucket.app_code.name
      groq_api_key  = var.groq_api_key
    })
  }

  depends_on = [
    google_managed_kafka_cluster.main,
    google_managed_kafka_topic.verify_test,
    google_storage_bucket_object.producer,
    google_storage_bucket_object.consumer_ml,
    google_storage_bucket_object.verify_kafka,
    google_project_iam_member.kafka_client,
    google_project_iam_member.storage_reader,
  ]
}
