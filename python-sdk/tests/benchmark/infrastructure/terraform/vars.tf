variable "project" {
  type        = string
  description = "GCP Project ID"
  default     = "astronomer-dag-authoring"
}

variable "region" {
  type        = string
  description = "GCP Region"
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "GCP Region"
  default     = "us-central1-a"
}

variable "tfstate_bucket" {
  type        = string
  description = "Terraform state GCS bucket"
  default     = "astronomer-dag-authoring-tfstate"
}

variable "gke_node_pool_machine_type" {
  type        = string
  description = "GCP machine type used for the GKE Node Pool"
  default     = "n2-standard-8"  # 16 vCPU and 64GB RAM, more options at https://cloud.google.com/compute/vm-instance-pricing
}
