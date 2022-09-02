terraform {

  backend "gcs" {
    bucket = "dag-authoring"
    prefix = "terraform-state/benchmark"
  }
}
