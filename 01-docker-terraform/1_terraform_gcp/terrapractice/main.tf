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
  #   credentials = "./keys/my-creds.json"
  project = "dtc-de-course-464416"
  region  = "asia-southeast1"
}

resource "google_storage_bucket" "demo-bucket" {     #demo-bucket is a variable -> call it with google_storage_bucket.demo-bucket
  name          = "dtc-de-course-464416-demo-bucket" # should be unique
  location      = "ASIA"
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