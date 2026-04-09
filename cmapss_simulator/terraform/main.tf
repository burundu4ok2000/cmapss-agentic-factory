terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure the Google Cloud Provider
provider "google" {
  project = var.project
  region  = var.region
}

# Create a Google Cloud Storage Bucket for the Data Lake
resource "google_storage_bucket" "datalake" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class

  # Ensure the bucket can be cleanly destroyed
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Create a Google BigQuery Dataset for the Data Warehouse
resource "google_bigquery_dataset" "telemetry_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# Create an External Table in BigQuery pointing to the GCS bucket
resource "google_bigquery_table" "raw_telemetry" {
  dataset_id = google_bigquery_dataset.telemetry_dataset.dataset_id
  table_id   = "raw_telemetry"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "time_cycles", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "op_setting_1", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "op_setting_2", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "op_setting_3", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "T2", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "T24", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "T30", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "T50", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "P2", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "P15", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "P30", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "Nf", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "Nc", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "epr", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "Ps30", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "phi", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "NRf", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "NRc", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "BPR", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "farB", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "htBleed", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "Nf_dmd", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "PCNfR_dmd", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "W31", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "W32", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "processing_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "is_corrupted", "type": "BOOLEAN", "mode": "NULLABLE"},
  {"name": "corruption_reason", "type": "STRING", "mode": "NULLABLE"}
]
EOF

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.datalake.name}/telemetry/data/*"]

    hive_partitioning_options {
      mode                   = "AUTO"
      source_uri_prefix      = "gs://${google_storage_bucket.datalake.name}/telemetry/data/"
      require_partition_filter = false
    }
  }
}
