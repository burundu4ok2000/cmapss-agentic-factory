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

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.datalake.name}/telemetry/*.parquet"]
  }
}
