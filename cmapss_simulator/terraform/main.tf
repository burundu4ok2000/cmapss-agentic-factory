terraform {
  required_version = ">= 1.0.0"
}
provider "google" {
  project = "demo-project"
  region  = "us-central1"
}
