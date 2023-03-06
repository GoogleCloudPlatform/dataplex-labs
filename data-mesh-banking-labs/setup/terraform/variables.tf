variable "project_id" {
  type        = string
  description = "project id required"
}

variable "location" {
 description = "Location/region to be used"
 default = "us-central1"
}

variable "ip_range" {
 description = "IP Range used for the network for this demo"
 default = "10.6.0.0/24"
}


variable "hive_metastore_version" {
 description = "Version of hive to be used for the dataproc metastore"
 default = "3.1.2"
}

variable "lake_name" {
  description = "Default name of the Dataplex Lake"
  default = "dataplex_enablement_lake"
}

variable "date_partition" {
  description = "Date Partition to use for Data Generator Tool"
  default = "2022-12-01"
}

variable "tmpdir" {
  description = "Temporary folder to use for Data Generator Tool"
  default = "~/dataplex-labs/sample_data/generated"
}

#variable "ldap" {
# description = "Individual ldap"
#}

variable "user_ip_range" {
 description = "IP range for the user running the demo"
 default = "10.6.0.0/24"
}
