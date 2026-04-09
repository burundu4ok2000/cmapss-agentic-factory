variable "project" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The region for the GCP resources"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The location for the storage bucket and BigQuery dataset"
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  type        = string
  default     = "cmapss_telemetry"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default     = "cmapss-datalake-bucket"
}

variable "gcs_storage_class" {
  description = "The storage class for the GCS bucket"
  type        = string
  default     = "STANDARD"
}
