resource "google_bigquery_dataset" "bq_ds" {
  dataset_id                  = "bq_world_pop2"
  friendly_name               = "test"
}

resource "google_bigquery_table" "table_tf" {
    table_id = "world_pop_table2"
    dataset_id = google_bigquery_dataset.bq_ds.dataset_id
}