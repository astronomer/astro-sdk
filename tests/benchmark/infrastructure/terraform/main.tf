provider "google" {
  project         = var.project
  region          = var.region
  request_timeout = "120s"
  batching {
    enable_batching = false
  }
}

resource "google_project_service" "kubernetes" {
  project = var.project
  service = "container.googleapis.com"
}

resource "google_project_service" "container_registry" {
  project = var.project
  service = "containerregistry.googleapis.com"
}

resource "google_service_account" "benchmark" {
  account_id   = "astro-sdk-gke-benchmark"
  display_name = "Service Account for GKE benchmark node pool"
}

resource "google_project_iam_member" "benchmark_gcs" {
  project = var.project
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_project_iam_member" "benchmark_bq_data_editor" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_project_iam_member" "benchmark_bq_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.benchmark.email}"
}

resource "google_project_iam_member" "benchmark_gcr" {
  project = var.project
  role    = "roles/containerregistry.ServiceAgent"
  member  = "serviceAccount:${google_service_account.benchmark.email}"
}

# Takes approximately 6m14 (2022/06)
resource "google_container_cluster" "astro_sdk" {
  name     = "astro-sdk"
  # Since we're using only for benchmarking, we can use the location as a zone
  # If we need high availability, this should be changed to a region.
  location = var.zone

  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1
}

# Takes approximately 1m15 (2022/06)
resource "google_container_node_pool" "benchmark" {
  name       = "benchmark"
  # Since we're using only for benchmarking, we can use the location as a zone
  # If we need high availability, this should be changed to a region.
  # When this is set to the region, we'll have (number of zones in region x node_count)
  # If we need high availability, this should be changed to a region.
  # When location is regional, we'll have a total of (zones in region) x (node_count) nodes.

  location   = var.zone
  cluster    = google_container_cluster.astro_sdk.name
  node_count = 1

  node_config {
    preemptible  = true
    machine_type = var.gke_node_pool_machine_type

    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    # https://developers.google.com/identity/protocols/oauth2/scopes
    service_account = google_service_account.benchmark.email
    oauth_scopes    = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
}


}
