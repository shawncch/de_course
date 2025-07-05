terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.41.0"
    }
  }
}

provider "google" {
  # Configuration options
  # not needed if we have declared env variable for credentials -> creds.json
  credentials = file(var.creds)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "demo-bucket" { #demo-bucket is a variable -> call it with google_storage_bucket.demo-bucket
  name          = var.gsb_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30 # age in days
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.dataset_id
  location   = var.location
}