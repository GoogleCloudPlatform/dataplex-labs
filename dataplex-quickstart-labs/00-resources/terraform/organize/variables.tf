variable "project_id" {
  type        = string
  description = "project id required"
}

variable "project_number" {
  type        = string
  description = "project nbr required"
}

variable "gcp_region" {
 description = "GCP region"
}

variable "gcp_lake" {
 description = "GCP lake"
 default = "oda-lake"
}

