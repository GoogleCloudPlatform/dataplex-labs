/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*

must set these command line variables
export USER_PROJECT_OVERRIDE=true
export GOOGLE_BILLING_PROJECT=dataplex-demo062024

install jq

terraform apply \
-var="project_id=${PROJECT_ID}" \
-var="gcp_region=${YOUR_GCP_REGION}" \
-var="project_number=${PROJECT_NBR}" \
--auto-approve 
*/


/******************************************
Local variables declaration
 *****************************************/

locals {
project_id                  = "${var.project_id}"
location                    = "${var.gcp_region}"
project_nbr                 = "${var.project_number}"

lab_chicagocrimes_bucket    = "chicagocrimes_${local.project_nbr}"
lab_customers_bucket        = "customers_${local.project_nbr}"
lab_credit_card_bucket      = "credit_card_${local.project_nbr}"
lab_code_bucket             = "raw-code-${local.project_nbr}"

bucket_list = {
    bucket1 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "raw_code"
              }
    bucket2 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "airflow/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "airflow" 
              } 
    bucket3 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "chicago-crimes-analytics", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "crimes_bucket" 
              } 
    bucket4 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "pyspark/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "pyspark" 
              } 
    bucket5 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "nyc-taxi-trip-analytics/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "taxidata"
              } 
    bucket6 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "sparksql/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "sparksql" 
              } 
    bucket7 = { 
                bucket_name = "${local.lab_code_bucket}", bucket_foldername = "spark-sql/retail-transactions-anomaly-detection/*", 
                lake = "oda-lake", zone = "oda-misc-zone", entry_id = "retail_bucket"
              } 
   }

table_list = {
  table1 = { data_type = "crimes", source_folder = "${local.lab_chicagocrimes_bucket}", entry_id = "crimes_table", lake = "oda-lake", zone = "oda-raw-zone" }
  table2 = { data_type = "customers", source_folder = "${local.lab_customers_bucket}", entry_id = "customers_table", lake = "oda-lake", zone = "oda-raw-sensitive-zone" }
  table3 = { data_type = "creditcard", source_folder = "${local.lab_credit_card_bucket}", entry_id = "creditcard_table", lake = "oda-lake", zone = "oda-raw-sensitive-zone" }
}

aspect_name         = "mystorage"
entry_group_name    = "oda-custom-entry-group"
bq_entry_group_name = "@bigquery"

}

provider "google" {
  project = "${var.project_id}"
  region  = "${var.gcp_region}"
}

/*
roles: 
roles/dataplex.discoveryPublishingServiceAgent
roles/storage.objectViewer
roles/bigquery.dataEditor
*/

/*****************************************************
1. Set roles on dataplex service agent account
 *****************************************************/

module "dataplexagent_role_grants" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "service-${local.project_nbr}@gcp-sa-dataplex.iam.gserviceaccount.com"
  prefix                  = "serviceAccount"
  project_id              = local.project_id

  project_roles = [
    "roles/dataplex.discoveryPublishingServiceAgent",
    "roles/storage.objectViewer",
    "roles/bigquery.dataEditor"
  ]
}

/*****************************************************
2. Create Entry Group and Entries for each folder
 *****************************************************/

resource "google_dataplex_entry_group" "entry_group" {
  entry_group_id = local.entry_group_name
  location =local.location

  depends_on = [
    module.dataplexagent_role_grants
  ]
}

module "do_create_entries" {
  for_each = local.bucket_list

  source = "./modules/create_entries"

  bucket_name = each.value.bucket_name
  folder_name = each.value.bucket_foldername
  entry_group_id = local.entry_group_name
  entry_id = each.value.entry_id
  project_nbr = "${var.project_number}"
  region = var.gcp_region
  project_id = var.project_id
  aspect_name = local.aspect_name
  
  
  depends_on = [
    google_dataplex_entry_group.entry_group
  ]
}

resource "google_dataplex_aspect_type" "my_storage_aspect" {
  aspect_type_id = local.aspect_name
  project = local.project_id
  location = local.location

  metadata_template = <<EOF
{
  "name": "${local.aspect_name}",
  "type": "record",
  "recordFields": [
    {
      "name": "service",
      "type": "string",
      "annotations": {
        "displayName": "Service",
        "description": "Service Name for this resource."
      },
      "index": 1
    },
    {
      "name": "resourceName",
      "type": "string",
      "annotations": {
        "displayName": "Resource Name",
        "description": "Name for GCS Resource."
      },
      "index": 2
      },
    {
      "name": "sourceUris",
      "type": "array",
      "annotations": {
        "displayName": "Source URI's",
        "description": "Source URI's for this GCS Resource."
      },
      "index": 3,
      "arrayItems": {"name": "sourceUri", "type": "string"}
      },
    {
      "name": "type", 
      "type": "enum", "index": 4, 
      "enumValues": [{"name": "UNSTRUCTURED", "index": 1}, {"name": "STRUCTURED", "index": 2}]
    }
  ]
  }
EOF
}

/******************************************
 3. Create Aspects for lakes and zones
 *****************************************/

resource "google_dataplex_aspect_type" "lake_aspect" {
  aspect_type_id = "lake"
  project = var.project_id
  location = var.gcp_region

  metadata_template = <<EOF
{
  "name": "lake",
  "type": "record",
  "recordFields": [
    {
      "name": "lake",
      "type": "enum",
      "annotations": {
        "displayName": "Lake",
        "description": "Specifies the lake represented by the entry."
      },
      "index": 1,
      "constraints": {
        "required": true
      },
      "enumValues": [
        {
          "name": "oda-lake",
          "index": 1
        }
      ]
    }
  ]
}
EOF
}

resource "google_dataplex_aspect_type" "zone_aspect" {
  aspect_type_id = "zone"
  project = var.project_id
  location = var.gcp_region

  metadata_template = <<EOF
{
  "name": "zone",
  "type": "record",
  "recordFields": [
    {
      "name": "type",
      "type": "enum",
      "annotations": {
        "displayName": "Type",
        "description": "Specifies the zone represented by the entry."
      },
      "index": 1,
      "constraints": {
        "required": true
      },
      "enumValues": [
        {
          "name": "oda-misc-zone",
          "index": 1
        },
        {
          "name": "oda-raw-zone",
          "index": 2
        },
        {
          "name": "oda-dq-zone",
          "index": 3
        },
        {
          "name": "oda-curated-zone",
          "index": 4
        },
        {
          "name": "oda-product-zone",
          "index": 5
        },
        {
          "name": "oda-raw-sensitive-zone",
          "index": 6
        }
      ]
    }
  ]
}
EOF
}

/******************************************
 4. Update Aspects on entries for lakes and zones for buckets
 *****************************************/

module "do_update_aspects_zone" {
  for_each = local.bucket_list

  source = "./modules/update_aspect"

  project_id = var.project_id
  region = var.gcp_region
  entry_group_id = local.entry_group_name
  entry_id = each.value.entry_id
  aspect_id = "zone"
  aspect_name = "type"
  aspect_value = each.value.zone

  depends_on = [
    google_dataplex_entry_group.entry_group,
    module.do_create_entries
 #   google_dataplex_aspect_type.lake_aspect,
 #   google_dataplex_aspect_type.zone_aspect
  ]
}

module "do_update_aspects_lake" {
  for_each = local.bucket_list

  source = "./modules/update_aspect"

  project_id = var.project_id
  region = var.gcp_region
  entry_group_id = local.entry_group_name
  entry_id = each.value.entry_id
  aspect_id = "lake"
  aspect_name = "lake"
  aspect_value = each.value.lake

  depends_on = [
    google_dataplex_entry_group.entry_group,
    module.do_create_entries
  #  google_dataplex_aspect_type.lake_aspect,
  #  google_dataplex_aspect_type.zone_aspect
  ]
}

/************************************************
 5. Discover crimes table in GCS & create entry
 ***********************************************/

/********************************************************
 Note: Each discovery is done separately rather than in 
 a loop because we need access to the output variables 
 from the module.
 *******************************************************/

resource "random_string" "scanname" {
  length = 8  # Adjust the length of the suffix as needed
  special = false # Set to true if you want to include special characters
  upper   = false # Set to true if you want to include uppercase letters
  numeric  = true  # Set to true if you want to include numbers
}

module "do_discovery_crimes_table" {
  source = "./modules/discover_tables"

  project_id = var.project_id
  bucket = local.table_list.table1.source_folder
  scanname = "discoveryscan${random_string.scanname.result}crimes"
  gcp_region = var.gcp_region

  depends_on = [
    random_string.scanname
  ]
}

module "do_discovery_customers_table" {
  source = "./modules/discover_tables"

  project_id = var.project_id
  bucket = local.table_list.table2.source_folder
  scanname = "discoveryscan${random_string.scanname.result}-customers"
  gcp_region = var.gcp_region

  depends_on = [
    random_string.scanname
  ]
}

module "do_discovery_creditcard_table" {
  source = "./modules/discover_tables"

  gcp_region = var.gcp_region
  project_id = var.project_id
  bucket = local.table_list.table3.source_folder
  scanname = "discoveryscan${random_string.scanname.result}-creditcard"

  depends_on = [
    random_string.scanname
  ]
}

/*******************************************
Wait 2 Extra minutes for Discovery to Complete
********************************************/

resource "time_sleep" "sleep_after_discovery" {
  create_duration = "2m"

  depends_on = [
    module.do_discovery_crimes_table,
    module.do_discovery_customers_table,
    module.do_discovery_creditcard_table
  ]
}

/******************************************
 6. Update Aspects on entries for lakes and zones for tables
 *****************************************/

/*
table_name = element(split("/", module.do_discovery_crimes_table.table_name[0]), 1)
  dataset_name = element(split("/", module.do_discovery_crimes_table.table_name[0]), 0)

"bigquery.googleapis.com/projects/${var.project_id}/datasets/credit_card_795099425658/tables/credit_card_795099425658"

input_table_name = each.value == "crimes" ? module.do_discovery_crimes_table.table_name[0] : (
    each.value == "customers" ? module.do_discovery_customers_table.table_name[0] : 
    each.value == "creditcard" ? module.do_discovery_creditcard_table.table_name[0] : ""
  )

input_dataset_name = each.value == "crimes" ? module.do_discovery_crimes_table.table_name[0] : (
    each.value == "customers" ? module.do_discovery_customers_table.table_name[0] : 
    each.value == "creditcard" ? module.do_discovery_creditcard_table.table_name[0] : ""
  )

*/


module "do_update_table_zone_aspect" {
  for_each = local.table_list

  source = "./modules/update_aspect"

  entry_id = each.value.data_type == "crimes" ? module.do_discovery_crimes_table.table_name[0] : (
    each.value.data_type == "customers" ? module.do_discovery_customers_table.table_name[0] : 
    each.value.data_type == "creditcard" ? module.do_discovery_creditcard_table.table_name[0] : ""
  )
  project_id = var.project_id
  region = var.gcp_region
  entry_group_id = local.bq_entry_group_name
  aspect_id = "zone"
  aspect_name = "type"
  aspect_value = each.value.zone

  depends_on = [
    time_sleep.sleep_after_discovery
  ]
}

module "do_update_table_lake_aspect" {
  for_each = local.table_list


  source = "./modules/update_aspect"

  entry_id = each.value.data_type == "crimes" ? module.do_discovery_crimes_table.table_name[0] : (
    each.value.data_type == "customers" ? module.do_discovery_customers_table.table_name[0] : 
    each.value.data_type == "creditcard" ? module.do_discovery_creditcard_table.table_name[0] : ""
  )


  project_id = var.project_id
  region = var.gcp_region
  entry_group_id = local.bq_entry_group_name
  aspect_id = "lake"
  aspect_name = "lake"
  aspect_value = each.value.lake

  depends_on = [
    time_sleep.sleep_after_discovery
  ]
}
