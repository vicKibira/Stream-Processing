terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Enable required APIs ───────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ── GCS bucket for Dataflow staging & temp ────────────────────────
resource "google_storage_bucket" "dataflow_bucket" {
  name          = "${var.project_id}-pizza-dataflow"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 7 }
  }

  depends_on = [google_project_service.apis]
}

# upload CSV to GCS so Dataflow can read it
resource "google_storage_bucket_object" "pizza_csv" {
  name   = "data/pizza_sales.csv"
  bucket = google_storage_bucket.dataflow_bucket.name
  source = "${path.module}/../data/pizza_sales.csv"
}

# ── Pub/Sub topic ─────────────────────────────────────────────────
resource "google_pubsub_topic" "pizza_orders" {
  name = "pizza-orders"

  message_retention_duration = "86600s" # 24 hrs

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "pizza_orders_sub" {
  name  = "pizza-orders-sub"
  topic = google_pubsub_topic.pizza_orders.name

  ack_deadline_seconds       = 20
  retain_acked_messages      = false
  message_retention_duration = "86600s"

  expiration_policy { ttl = "" } # never expires

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# ── BigQuery dataset + table ──────────────────────────────────────
resource "google_bigquery_dataset" "pizza" {
  dataset_id = "pizza_analytics"
  location   = var.region

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_table" "pizza_orders" {
  dataset_id          = google_bigquery_dataset.pizza.dataset_id
  table_id            = "pizza_orders"
  deletion_protection = false

  schema = jsonencode([
    { name = "pizza_id",          type = "INTEGER", mode = "NULLABLE" },
    { name = "order_id",          type = "INTEGER", mode = "NULLABLE" },
    { name = "pizza_name_id",     type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_name",        type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_category",    type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_size",        type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_ingredients", type = "STRING",  mode = "NULLABLE" },
    { name = "quantity",          type = "INTEGER", mode = "NULLABLE" },
    { name = "unit_price",        type = "FLOAT",   mode = "NULLABLE" },
    { name = "total_price",       type = "FLOAT",   mode = "NULLABLE" },
    { name = "order_date",        type = "STRING",  mode = "NULLABLE" },
    { name = "order_time",        type = "STRING",  mode = "NULLABLE" },
    { name = "event_ts",          type = "FLOAT",   mode = "NULLABLE" },
  ])
}

# ── Service account for Dataflow ──────────────────────────────────
resource "google_service_account" "dataflow_sa" {
  account_id   = "pizza-dataflow-sa"
  display_name = "Pizza Dataflow Service Account"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# ── Dataflow job: Pub/Sub → BigQuery ─────────────────────────────
resource "google_dataflow_job" "pubsub_to_bq" {
  name              = "pizza-pubsub-to-bq"
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/PubSub_to_BigQuery"
  temp_gcs_location = "gs://${google_storage_bucket.dataflow_bucket.name}/tmp"

  parameters = {
    inputTopic      = google_pubsub_topic.pizza_orders.id
    outputTableSpec = "${var.project_id}:${google_bigquery_dataset.pizza.dataset_id}.${google_bigquery_table.pizza_orders.table_id}"
  }

  service_account_email = google_service_account.dataflow_sa.email
  region                = var.region
  machine_type          = "n1-standard-2"
  max_workers           = 3

  on_delete = "cancel"

  depends_on = [
    google_pubsub_topic.pizza_orders,
    google_bigquery_table.pizza_orders,
    google_storage_bucket.dataflow_bucket,
    google_project_iam_member.dataflow_worker,
    google_project_iam_member.pubsub_subscriber,
    google_project_iam_member.bigquery_editor,
    google_project_iam_member.storage_admin,
  ]
}
