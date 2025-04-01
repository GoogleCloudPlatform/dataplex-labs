
variable "project_id" {
  type        = string
  description = "project id required"
}

variable "bucket" {
  type        = string
  description = "value of bucket to scan"
}

variable "scanname" {
  type        = string
  description = "value of name to use as the scan name"
}

variable "gcp_region" {
  type        = string
  description = "value of name to use for the region"
}
