/*
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
  limitations under the License.
*/

####################################################################################
# Variables
####################################################################################
variable "project_id" {}
variable "location" {}

resource "google_bigquery_analytics_hub_data_exchange" "listing" {
  project          = var.project_id
  location         = var.location
  data_exchange_id = "datagov_private_exchange"
  display_name     = "datagov private exchange"
  description      = "Datagov priv data exchange"
}

#sometimes we get API rate limit errors for dataplex; add wait until this is resolved.
resource "time_sleep" "sleep_after_data_exchange" {
  create_duration = "30s"

  depends_on = [google_bigquery_analytics_hub_data_exchange.listing]
}

resource "google_bigquery_analytics_hub_listing" "listing1" {
  location         = var.location
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.listing.data_exchange_id
  listing_id       = "customers_data_prod_listing"
  display_name     = "customers_product_listing"
  description      = "example data exchange"

  bigquery_dataset {
    dataset = google_bigquery_dataset.listing1.id
  }
  depends_on = [google_bigquery_analytics_hub_data_exchange.listing]
}

resource "google_bigquery_dataset" "listing1" {
  dataset_id                  = "customer_data_product"
  friendly_name               = "customer_product_listing"
  description                 = "Consumer Data Product"
  project                     = var.project_id
  location                    = var.location
}

resource "google_bigquery_analytics_hub_listing" "listing2" {
  location         = var.location
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.listing.data_exchange_id
  listing_id       = "merchants_data_prod_listing"
  display_name     = "merchant_product_listing"
  description      = "example data exchange"

  bigquery_dataset {
    dataset = google_bigquery_dataset.listing2.id
  }
  depends_on = [google_bigquery_analytics_hub_data_exchange.listing]
}

resource "google_bigquery_dataset" "listing2" {
  dataset_id                  = "merchants_data_product"
  friendly_name               = "merchants_data_product"
  description                 = "merchant Data Product"
  project                     = var.project_id
  location                    = var.location
}

resource "google_bigquery_analytics_hub_listing" "listing3" {
  location         = var.location
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.listing.data_exchange_id
  listing_id       = "trans_data_prod_listing"
  display_name     = "trans_data_prod_listing"
  description      = "example data exchange"

  bigquery_dataset {
    dataset = google_bigquery_dataset.listing3.id
  }
  depends_on = [google_bigquery_analytics_hub_data_exchange.listing]

}

resource "google_bigquery_dataset" "listing3" {
  dataset_id                  = "auth_data_product"
  friendly_name               = "auth_data_product"
  description                 = "auth Data Product"
  project                     = var.project_id
  location                    = var.location
}
