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
variable "date_partition" {}
variable "tmpdir" {}
variable "customers_bucket_name" {}
variable "merchants_bucket_name" {}
variable "transactions_bucket_name" {}
variable "customers_curated_bucket_name" {}
variable "merchants_curated_bucket_name" {}
variable "transactions_curated_bucket_name" {}
variable "transactions_ref_bucket_name" {}
variable "data_gen_git_repo" {}

locals {
  _abs_tmpdir=pathexpand(var.tmpdir)
}


####################################################################################
# Generate Sample Data
####################################################################################
#retrieve data generator
/*
resource "null_resource" "git_clone_datagen" {
  provisioner "local-exec" {
    command = <<-EOT
    rm -rf ./datamesh-datagenerator
    git clone --branch one-click-deploy ${var.data_gen_git_repo}
      EOT 
  }
 
  provisioner "local-exec" {
    command = <<-EOT
      rm -rf ./datamesh-datagenerator
      rm /tmp/data/*
    EOT
    when    = destroy
  } 
} 


#run data creation process
resource "null_resource" "run_datagen" {
  provisioner "local-exec" {
    command = <<-EOT
      cd ./datamesh-datagenerator
      ./oneclick_deploy.sh ${var.date_partition} ${var.tmpdir} ${var.project_id} ${var.customers_bucket_name} ${var.merchants_bucket_name} ${var.transactions_bucket_name}
    EOT
    }
    depends_on = [null_resource.git_clone_datagen]

  }
*/

####################################################################################
# Extract Data
####################################################################################


resource "null_resource" "run_datagen" {
  provisioner "local-exec" {
    command = <<-EOT
      cd ../resources/sample_data
      unzip -o synthetic_financial_data.zip
    EOT
    }
   # depends_on = [null_resource.git_clone_datagen]

  }

####################################################################################
# Create GCS Buckets
####################################################################################


resource "google_storage_bucket" "storage_bucket_1" {
  project                     = var.project_id
  name                        = var.customers_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_2" {
  project                     = var.project_id
  name                        = var.customers_curated_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_3" {
  project                     = var.project_id
  name                        = var.merchants_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_4" {
  project                     = var.project_id
  name                        = var.merchants_curated_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_5" {
  project                     = var.project_id
  name                        = var.transactions_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_6" {
  project                     = var.project_id
  name                        = var.transactions_curated_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}

resource "google_storage_bucket" "storage_bucket_7" {
  project                     = var.project_id
  name                        = var.transactions_ref_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [null_resource.run_datagen]
}



resource "time_sleep" "sleep_after_storage" {
  create_duration = "60s"
  depends_on = [
                google_storage_bucket.storage_bucket_1,
                google_storage_bucket.storage_bucket_2,
                google_storage_bucket.storage_bucket_3,
                google_storage_bucket.storage_bucket_4,
                google_storage_bucket.storage_bucket_5,
                google_storage_bucket.storage_bucket_6,
                google_storage_bucket.storage_bucket_7
              ]
}

####################################################################################
# Create Customer GCS Objects
####################################################################################

resource "google_storage_bucket_object" "gcs_customers_objects" {
  for_each = {
    format("../resources/sample_data/customer.csv") : format("customers_data/dt=%s/customer.csv", var.date_partition),
    format("../resources/sample_data/cc_customer.csv") : format("cc_customers_data/dt=%s/cc_customer.csv", var.date_partition)
  }
  name        = each.value
  source      = each.key
  bucket = var.customers_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}

#Adding empty directory so it can be discovered by Dataplex and does not conflict with BQ 

resource "google_storage_bucket_object" "cc_cust_folder" {
  name          = format("cc_customers_data/dt=%s/",var.date_partition)
  content       = "Not really a directory, but it's empty."
  bucket        = var.customers_curated_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}

resource "google_storage_bucket_object" "cust_folder" {
  name          = format("customers_data/dt=%s/",var.date_partition)
  content       = "Not really a directory, but it's empty."
  bucket        = var.customers_curated_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}


####################################################################################
# Create Merchants GCS Objects
####################################################################################

resource "google_storage_bucket_object" "gcs_merchants_objects" {
  for_each = {
    format("../resources/sample_data/merchants.csv",) : format("merchants_data/dt=%s/merchants.csv", var.date_partition),
    "../resources/sample_data/mcc_codes.csv" : format("mcc_codes/dt=%s/mcc_codes.csv", var.date_partition),
  }
  name        = each.value
  source      = each.key
  bucket = var.merchants_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}

#Adding Empty Directory for Curated to avoid Dataplex entity conflicts

resource "google_storage_bucket_object" "merchant_folder" {
  name          =  format("merchants_data/date=%s/",var.date_partition)
  content       = "Not really a directory, but it's empty."
  bucket        = var.merchants_curated_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}


####################################################################################
# Create Transactions GCS Objects
####################################################################################

resource "google_storage_bucket_object" "gcs_transaction_objects" {
  for_each = {
    format("../resources/sample_data/trans_data.csv") : format("auth_data/dt=%s/trans_data.csv", var.date_partition)
  }
  name        = each.value
  source      = each.key
  bucket = var.transactions_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}

resource "google_storage_bucket_object" "auth_folder" {
  name          = format("auth_data/date=%s/",var.date_partition)
  content       = "Not really a directory, but it's empty."
  bucket        = var.transactions_curated_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}


resource "google_storage_bucket_object" "gcs_transaction_refdata_objects" {
  for_each = toset([
    "signature",
    "card_type_facts",
    "payment_methods",
    "events_type",
    "currency",
    "swiped_code",
    "origination_code",
    "trans_type",
    "card_read_type"
  ])
  name        = format("ref_data/%s/%s.csv", each.key, each.key)
  source      = format("../resources/sample_data/%s.csv", each.key)
  bucket      = var.transactions_ref_bucket_name
  depends_on = [time_sleep.sleep_after_storage]
}

####################################################################################
# Create BigQuery Datasets
# #Removed auth_data_product, customer_data_product, merchants_data_product dataset creation as it will be created as part of analytics hub data exchange and listing

####################################################################################

resource "google_bigquery_dataset" "bigquery_datasets" {
  for_each = toset([ 
    "auth_ref_data",
    "cc_analytics_data_product",
    "customer_private",
    "customer_ref_data",
    "customer_refined_data",
    "merchants_ref_data",
    "merchants_refined_data",
    "pos_auth_refined_data"
  ])
  project                     = var.project_id
  dataset_id                  = each.key
  friendly_name               = each.key
  description                 = "${each.key} Dataset for Dataplex Demo"
  location                    = var.location
  delete_contents_on_destroy  = true
  depends_on = [google_storage_bucket_object.gcs_transaction_refdata_objects]

}

####################################################################################
# Create BigQuery Tables
####################################################################################

resource "random_integer" "jobid" {
  min     = 10
  max     = 19999
    keepers = {
    first = "${timestamp()}"
  }
}

resource "google_bigquery_job" "job" {
  for_each = {
    "merchants_ref_data.mcc_code" : format("gs://%s/mcc_codes/dt=%s/mcc_codes.csv", var.merchants_bucket_name, var.date_partition),
    "auth_ref_data.signature" : format("gs://%s/ref_data/signature/signature.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.card_type_facts" : format("gs://%s/ref_data/card_type_facts/card_type_facts.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.payment_methods" : format("gs://%s/ref_data/payment_methods/payment_methods.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.events_type" : format("gs://%s/ref_data/events_type/events_type.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.currency" : format("gs://%s/ref_data/currency/currency.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.swiped_code" : format("gs://%s/ref_data/swiped_code/swiped_code.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.origination_code" : format("gs://%s/ref_data/origination_code/origination_code.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.trans_type" : format("gs://%s/ref_data/trans_type/trans_type.csv", var.transactions_ref_bucket_name),
    "auth_ref_data.card_read_type" : format("gs://%s/ref_data/card_read_type/card_read_type.csv", var.transactions_ref_bucket_name)
  }
  job_id     = format("job_load_%s_${random_integer.jobid.result}", element(split(".", each.key), 1))
  project    = var.project_id
  location   = var.location
  #labels = {
  #  "my_job" ="load"
  #}

  load {
    source_uris = [
      each.value
    ]

    destination_table {
      project_id = var.project_id
      dataset_id = element(split(".", each.key), 0)
      table_id   = element(split(".", each.key), 1)
    }

    skip_leading_rows = 1
    schema_update_options = ["ALLOW_FIELD_RELAXATION", "ALLOW_FIELD_ADDITION"]

    write_disposition = "WRITE_APPEND"
    autodetect = true
    }

    depends_on  = [google_storage_bucket_object.gcs_transaction_refdata_objects,
                   google_storage_bucket_object.gcs_transaction_objects,
                   google_storage_bucket_object.gcs_customers_objects,
                   google_storage_bucket_object.gcs_merchants_objects,
                   google_storage_bucket_object.cc_cust_folder,
                   google_storage_bucket_object.cust_folder,
                   google_storage_bucket_object.merchant_folder,
                   google_storage_bucket_object.auth_folder,
                   google_bigquery_dataset.bigquery_datasets
                  ]
  }
