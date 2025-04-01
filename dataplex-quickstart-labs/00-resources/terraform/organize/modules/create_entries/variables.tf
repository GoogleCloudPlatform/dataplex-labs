variable "project_id" {
  type        = string
  description = "project id required"
}

variable "region" {
  type        = string
  description = "region required"
}

variable "entry_group_id" {
    type = string
    description = "entry group name to use"
}

variable "entry_id" {
    type = string
    description = "entry id to use"
}

variable "bucket_name" {
  type        = string
  description = "input bucket name"
}

variable "folder_name" {
  type        = string
  description = "name of folder"
}

variable "project_nbr" {
  type        = string
  description = "project number"
}

variable "aspect_name" {
  type        = string
  description = "aspect_name"
}
