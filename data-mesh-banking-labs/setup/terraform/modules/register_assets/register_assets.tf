/**
 * Copyright 2022 Google LLC
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

####################################################################################
# Variables
####################################################################################
variable "project_id" {}
variable "project_number" {}
variable "location" {}
variable "lake_name" {}
variable "customers_bucket_name" {}
variable "merchants_bucket_name" {}
variable "transactions_bucket_name" {}
variable "transactions_ref_bucket_name" {}
variable "customers_curated_bucket_name" {}
variable "merchants_curated_bucket_name" {}
variable "transactions_curated_bucket_name" {}
variable "datastore_project_id" {}

resource "google_dataplex_asset" "register_gcs_assets1" {
 for_each = {
    "authorizations-ref-raw-data/Authorization Ref Raw Data/authorizations-raw-zone/consumer-banking--creditcards--transaction--domain" : var.transactions_ref_bucket_name,
    "transactions-raw-data/Authorization Raw Data/authorizations-raw-zone/consumer-banking--creditcards--transaction--domain" : var.transactions_bucket_name,
    "merchant-raw-data/Merchant Raw Data/merchant-raw-zone/consumer-banking--merchant--domain" : var.merchants_bucket_name,
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
    csv_options {
      delimiter = "|"
      header_rows = 1
    }
  }

  resource_spec {
    name = "projects/${var.datastore_project_id}/buckets/${each.value}"
    type = "STORAGE_BUCKET"
  }

  project = var.project_id
}

#sometimes we get API rate limit errors for dataplex; add wait until this is resolved.
resource "time_sleep" "sleep_after_assets" {
  create_duration = "60s"

  depends_on = [google_dataplex_asset.register_gcs_assets1]
}

resource "google_dataplex_asset" "register_gcs_assets2" {
 for_each = {
    #"customer-raw-data/Customer Raw Data/customer-raw-zone/consumer-banking--customer--domain" : var.customers_bucket_name,
    "transactions-curated-data/Transactions Curated Data/authorizations-data-product-zone/consumer-banking--creditcards--transaction--domain" : var.transactions_curated_bucket_name,
    "merchant-curated-data/Merchant Curated Data/merchant-data-product-zone/consumer-banking--merchant--domain" : var.merchants_curated_bucket_name,
    #"customer-curated-data/Customer Curated Data/customer-curated-zone/consumer-banking--customer--domain" : var.customers_curated_bucket_name
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
    csv_options {
      delimiter = "|"
      header_rows = 1
    }
  }

  resource_spec {
    name = "projects/${var.datastore_project_id}/buckets/${each.value}"
    type = "STORAGE_BUCKET"
  }

  project = var.project_id
  depends_on  = [time_sleep.sleep_after_assets]
}


#sometimes we get API rate limit errors for dataplex; add wait until this is resolved.
resource "time_sleep" "sleep_after_gcs_assets2" {
  create_duration = "60s"

  depends_on = [google_dataplex_asset.register_gcs_assets2]
}

resource "google_dataplex_asset" "register_bq_assets1" {
 for_each = {
    #"customer-data-product/Customer Data Product/customer-data-product-zone/consumer-banking--customer--domain" : "customer_data_product",
    #"customer-data-product-reference/Customer Reference Data Product/customer-data-product-zone/consumer-banking--customer--domain" : "customer_ref_data" ,
    # "customer-refined-data/Customer Refined Data/customer-curated-zone/consumer-banking--customer--domain" : "customer_refined_data" ,
    "merchant-refined-data/Merchant Refined Data/merchant-data-product-zone/consumer-banking--merchant--domain" : "merchants_refined_data",
     "auth-refined-data/Authorization Refined Data/authorizations-data-product-zone/consumer-banking--creditcards--transaction--domain" : "pos_auth_refined_data"
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.datastore_project_id}/datasets/${each.value}"
    type = "BIGQUERY_DATASET"
  }

  project = var.project_id
  depends_on  = [time_sleep.sleep_after_gcs_assets2]
}

resource "google_dataplex_asset" "register_bq_assets2" {
 for_each = {
    "merchant-data-products/Merchant Data Product/merchant-data-product-zone/consumer-banking--merchant--domain" : "merchants_data_product",
    "merchant-ref-product/Merchant Data Product Reference/merchant-data-product-zone/consumer-banking--merchant--domain" : "merchants_ref_data",
    "authorizations-data-product/Auhorization Data Product/authorizations-data-product-zone/consumer-banking--creditcards--transaction--domain" : "auth_data_product",
    "authorizations-ref-product/Authorization Data Product Reference/authorizations-data-product-zone/consumer-banking--creditcards--transaction--domain" : "auth_ref_data",
     "cc-analytics-data-product/CCA Data Product/data-product-zone/consumer-banking--creditcards--analytics--domain" : "cc_analytics_data_product"
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.datastore_project_id}/datasets/${each.value}"
    type = "BIGQUERY_DATASET"

  }

  project = var.project_id
  depends_on  = [google_dataplex_asset.register_bq_assets1]
}


#sometimes we get API rate limit errors for dataplex; add wait until this is resolved.
resource "time_sleep" "sleep_after_bq_assets2" {
  create_duration = "60s"

  depends_on = [google_dataplex_asset.register_bq_assets2]
}

resource "google_dataplex_asset" "register_bq_assets3" {
 for_each = {
    "dlp-reports/DLP Reports/operations-data-product-zone/central-operations--domain" : "central_dlp_data" ,
    "dq-reports/DQ Reports/operations-data-product-zone/central-operations--domain" : "central_dq_results" ,
    "audit-data/Audit Data/operations-data-product-zone/central-operations--domain" : "central_audit_data" ,
     "enterprise-reference-data/Enterprise Reference Data/operations-data-product-zone/central-operations--domain" : "enterprise_reference_data"
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${each.value}"
    type = "BIGQUERY_DATASET"
  }

  project = var.project_id
  depends_on  = [time_sleep.sleep_after_bq_assets2]
}

resource "google_dataplex_asset" "register_gcs_assets3" {
 for_each = {
      "common-utilities/COMMON UTILITIES/common-utilities/central-operations--domain" : format("%s_dataplex_process" ,var.project_id)
  }
  name          = element(split("/", each.key), 0)
  display_name  = element(split("/", each.key), 1)
  location      = var.location

  lake = element(split("/", each.key), 3)
  dataplex_zone = element(split("/", each.key), 2)

  discovery_spec {
    enabled = true
    csv_options {
      delimiter = "|"
      header_rows = 1
    }
  }

  resource_spec {
    name = "projects/${var.project_id}/buckets/${each.value}"
    type = "STORAGE_BUCKET"
  }

  project = var.project_id
  depends_on  = [google_dataplex_asset.register_bq_assets3]
}
