terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}


provider "google" {
  project = var.project
  region  = var.region
  credentials = file(var.credentials)
}

resource "google_storage_bucket" "arjun_nyc_taxi_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true
  storage_class = var.gcs_storage_class

  lifecycle_rule {
    condition {
      age = 1
    }

    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }

    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  
}


# resource "google_compute_instance" "arjun_compute_engine" {
#     name         = var.gcs_vm_name
#     project      = "de-zoomcamp-448516"
#     zone         = "asia-south1-c"
#     machine_type = "e2-standard-8"

#     boot_disk {
#         initialize_params {
#             image = "projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20250130"
#             size  = 30
#             type  = "pd-standard"  # Change this to "pd-standard" or "pd-ssd"
#         }
#         auto_delete = true
#     }

#     network_interface {
#         network       = "default"
#         access_config {
#             network_tier = "PREMIUM"
#         }
#     }

#     service_account {
#         email  = "terraform-runner@de-zoomcamp-448516.iam.gserviceaccount.com"
#         scopes = ["cloud-platform"]

#     }

#     scheduling {
#         automatic_restart   = true
#         on_host_maintenance = "MIGRATE"
#         preemptible         = false
#     }

#     shielded_instance_config {
#         enable_secure_boot          = false
#         enable_vtpm                 = true
#         enable_integrity_monitoring = true
#     }

#     labels = {
#         goog-ec-src = "vm_add-gcloud"
#     }

#     metadata_startup_script = file("gcp_vm_install_docker.sh")

    
# }

resource "google_bigquery_dataset" "nyc_taxi_dataset" {
    dataset_id = var.bq_dataset_name
    project = var.project
    location = var.region
    default_table_expiration_ms = 7200000
    default_partition_expiration_ms = 7200000
    labels = {
        env = "default"
    }
  
}

