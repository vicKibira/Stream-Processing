output "pubsub_topic" {
  value = google_pubsub_topic.pizza_orders.name
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.pizza_orders_sub.name
}

output "bigquery_table" {
  value = "${google_bigquery_dataset.pizza.dataset_id}.${google_bigquery_table.pizza_orders.table_id}"
}

output "gcs_bucket" {
  value = google_storage_bucket.dataflow_bucket.name
}

output "dataflow_sa_email" {
  value = google_service_account.dataflow_sa.email
}
