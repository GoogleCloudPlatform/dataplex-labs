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
variable "location" {}
variable "lake_name" {}
#variable "metastore_service_name" {}
variable "project_number" {}
variable "datastore_project_id" {}

/* With Metastore
resource "null_resource" "create_lake" {
 for_each = {
    "consumer-banking--customer--domain/Customer - Source Domain" : "domain_type=source",
    "consumer-banking--merchant--domain/Merchant - Source Domain" : "domain_type=source",
    "consumer-banking--creditcards--transaction--domain/Transactions - Source Domain" : "domain_type=source",
    "consumer-banking--creditcards--analytics--domain/Credit Card Analytics - Consumer Domain" : "domain_type=consumer",
    "central-operations--domain/Central Operations Domain" : "domain_type=operations"
  }
  provisioner "local-exec" {
    command = format("gcloud dataplex lakes create --project=%s %s --display-name=\"%s\" --location=%s --labels=%s --metastore-service=%s ", 
                     var.project_id,
                     element(split("/", each.key), 0),
                     element(split("/", each.key), 1),
                     var.location,
                     each.value,
                     "projects/${var.project_id}/locations/${var.location}/services/${var.metastore_service_name}")
  }
}
*/



####################################################################################
# Create Domain1: Customer Lakes and Zones        
# This will done as part of lab#0                                          #
####################################################################################
/*
resource "google_dataplex_lake" "create_customer_lakes" {
  location     = var.location
  name         = "consumer-banking--customer--domain"
  description  = "Consumer Banking Customer Domain"
  display_name = "Consumer Banking - Customer Domain"

  labels       = {
    domain_type="source"
  }
  
  project = var.project_id
}

resource "google_dataplex_zone" "create_customer_zones" {
 for_each = {
    "customer-raw-zone/Customer Raw Zone/consumer-banking--customer--domain/RAW" : "data_product_category=raw_data",
    "customer-curated-zone/Customer Curated Zone/consumer-banking--customer--domain/CURATED" : "data_product_category=curated_data",
    "customer-data-product-zone/Customer Data Product Zone/consumer-banking--customer--domain/CURATED" : "data_product_category=master_data",
  }

  discovery_spec {
    enabled = true
    schedule = "0 * * * *"
  }

  lake     =  element(split("/", each.key), 2)
  location = var.location
  name     = element(split("/", each.key), 0)

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = element(split("/", each.key), 3)
  description  = element(split("/", each.key), 1)
  display_name = element(split("/", each.key), 1)
  labels       = {
    element(split("=", each.value), 0) = element(split("=", each.value), 1)
  }
  project      = var.project_id

  depends_on  = [google_dataplex_lake.create_customer_lakes]
}




*/


####################################################################################
# Create Domain2: Merchant Lakes and Zones                                         #
####################################################################################


resource "google_dataplex_lake" "create_merchant_lakes" {
  location     = var.location
  name         = "consumer-banking--merchant--domain"
  description  = "Consumer Banking Merchant Domain"
  display_name = "Merchant Domain"

  labels       = {
    domain_type="source"
  }
  
  project = var.project_id

 # depends_on  = [google_dataplex_zone.create_customer_zones]
}

resource "google_dataplex_zone" "create_merchant_zones" {
 for_each = {
    "merchant-raw-zone/Merchant Raw Zone/consumer-banking--merchant--domain/RAW" : "data_product_category=raw_data",
    #Commenting due to ZonePerRegion Limitations
    #"merchant-curated-zone/Merchant Curated Zone/consumer-banking--merchant--domain/CURATED" : "data_product_category=curated_data",
    "merchant-data-product-zone/Merchant Curated Zone/consumer-banking--merchant--domain/CURATED" : "data_product_category=master_data",
  }

  discovery_spec {
    enabled = true
    schedule = "0 * * * *"
  }

  lake     =  element(split("/", each.key), 2)
  location = var.location
  name     = element(split("/", each.key), 0)

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = element(split("/", each.key), 3)
  description  = element(split("/", each.key), 1)
  display_name = element(split("/", each.key), 1)
  labels       = {
    element(split("=", each.value), 0) = element(split("=", each.value), 1)
  }
  project      = var.project_id

  depends_on  = [google_dataplex_lake.create_merchant_lakes]
}


####################################################################################
# Create Domain3: CreditCards Transactions Lakes and Zones                         #
####################################################################################


resource "google_dataplex_lake" "create_cc_transaction_lakes" {
  location     = var.location
  name         = "consumer-banking--creditcards--transaction--domain"
  description  = "Consumer Banking CreditCards Transaction Domain"
  display_name = "CreditCards - Transaction Domain"

  labels       = {
    domain_type="source"
  }
  
  project = var.project_id

  depends_on  = [google_dataplex_zone.create_merchant_zones]
}

resource "google_dataplex_zone" "create_cc_transaction_zones" {
 for_each = {

    #"authorization-curated-zone/Authorizations Curated Zone/consumer-banking--creditcards--transaction--domain/CURATED" : "data_product_category=raw_data",
    "authorizations-raw-zone/Authorizations Raw Zone/consumer-banking--creditcards--transaction--domain/RAW" : "data_product_category=curated_data",
    "authorizations-data-product-zone/Authorizations Curated Zone/consumer-banking--creditcards--transaction--domain/CURATED" : "data_product_category=master_data"

  }

  discovery_spec {
    enabled = true
    schedule = "0 * * * *"
  }

  lake     =  element(split("/", each.key), 2)
  location = var.location
  name     = element(split("/", each.key), 0)

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = element(split("/", each.key), 3)
  description  = element(split("/", each.key), 1)
  display_name = element(split("/", each.key), 1)
  labels       = {
    element(split("=", each.value), 0) = element(split("=", each.value), 1)
  }
  project      = var.project_id

  depends_on  = [google_dataplex_lake.create_cc_transaction_lakes]
}

####################################################################################
# Create Domain4: CreditCards Analytics Lakes and Zones                            #
####################################################################################

resource "google_dataplex_lake" "create_cc_analytics_lakes" {
  location     = var.location
  name         = "consumer-banking--creditcards--analytics--domain"
  description  = "Consumer Banking CreditCards Analytics Domain"
  display_name = "CreditCards - Analytics Domain"

  labels       = {
    domain_type="consumer"
  }
  
  project = var.project_id

  depends_on  = [google_dataplex_zone.create_cc_transaction_zones]

}

resource "google_dataplex_zone" "create_cc_analytics_zones" {
 for_each = {
       "data-product-zone/Data Product Zone/consumer-banking--creditcards--analytics--domain/CURATED" : "data_product_category=master_data",

  }

  discovery_spec {
    enabled = true
    schedule = "0 * * * *"
  }

  lake     =  element(split("/", each.key), 2)
  location = var.location
  name     = element(split("/", each.key), 0)

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = element(split("/", each.key), 3)
  description  = element(split("/", each.key), 1)
  display_name = element(split("/", each.key), 1)
  labels       = {
    element(split("=", each.value), 0) = element(split("=", each.value), 1)
  }
  project      = var.project_id

  depends_on  = [google_dataplex_lake.create_cc_analytics_lakes]
}


####################################################################################
# Create Domain5: Central Operations Lakes and Zones                               #
####################################################################################

resource "google_dataplex_lake" "create_central_ops_lakes" {
  location     = var.location
  name         = "central-operations--domain"
  description  = "Central Operations Domain"
  display_name = "Central Operations Domain"

  labels       = {
    domain_type="operations"
  }
  
  project = var.project_id

  depends_on  = [google_dataplex_zone.create_cc_analytics_zones]
}

resource "google_dataplex_zone" "create_central_ops_zones" {
 for_each = {
    "common-utilities/Common Utilities/central-operations--domain/CURATED" : "",
    "operations-data-product-zone/Data Product Zone/central-operations--domain/CURATED" : "",   
  }

  discovery_spec {
    enabled = true
    schedule = "0 * * * *"
  }

  lake     =  element(split("/", each.key), 2)
  location = var.location
  name     = element(split("/", each.key), 0)

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = element(split("/", each.key), 3)
  description  = element(split("/", each.key), 1)
  display_name = element(split("/", each.key), 1)

  project      = var.project_id

  depends_on  = [google_dataplex_lake.create_central_ops_lakes]
}



####################################################################################
# Dataplex Service Account 
####################################################################################


resource "null_resource" "dataplex_permissions_1" {
  provisioner "local-exec" {
    command = format("gcloud projects add-iam-policy-binding %s --member=\"serviceAccount:service-%s@gcp-sa-dataplex.iam.gserviceaccount.com\" --role=\"roles/dataplex.dataReader\"", 
                      var.project_id,
                      var.project_number)
  }

  depends_on = [google_dataplex_zone.create_central_ops_zones]
}

resource "null_resource" "dataplex_permissions_2" {
  provisioner "local-exec" {
    command = format("gcloud projects add-iam-policy-binding %s --member=\"serviceAccount:service-%s@gcp-sa-dataplex.iam.gserviceaccount.com\" --role=\"roles/dataplex.serviceAgent\"", 
                      var.project_id,
                      var.project_number)
  }

  depends_on = [null_resource.dataplex_permissions_1]
}


# Delete the unnecessary dataset created in BigQuery only Zone based datasets 

resource "null_resource" "delete_empty_bq_ds" {
  provisioner "local-exec" {
    command = <<-EOT
    bq rm -r -f -d ${var.project_id}:customer_data_product_zone
    bq rm -r -f -d ${var.project_id}:merchant_data_product_zone
    bq rm -r -f -d ${var.project_id}:authorizations_data_product_zone
    bq rm -r -f -d ${var.project_id}:data_product_zone
    EOT
    }
    depends_on = [null_resource.dataplex_permissions_2]

  }


#sometimes we get API rate limit errors for dataplex; add wait until this is resolved.
#resource "time_sleep" "sleep_after_zones" {
#  create_duration = "60s"
#
#  depends_on = [google_dataplex_zone.create_zones]
#}