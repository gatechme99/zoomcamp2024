terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.5.0"
    }
  }
}

provider "google" {
  ## credentials = ""
  project     = "dezoomcamp2024-437322"
  region      = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dezoomcamp2024-437322-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}