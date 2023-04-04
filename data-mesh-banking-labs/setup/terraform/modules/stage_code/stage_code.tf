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
variable "dataplex_process_bucket_name" {}
variable "dataplex_bqtemp_bucket_name" {}



resource "google_storage_bucket" "storage_bucket_process" {
  project                     = var.project_id
  name                        = var.dataplex_process_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true


}

resource "google_storage_bucket" "storage_bucket_bqtemp" {
  project                     = var.project_id
  name                        = var.dataplex_bqtemp_bucket_name
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [google_storage_bucket.storage_bucket_process]
}


resource "google_bigquery_dataset" "bigquery_datasets" {
  for_each = toset([ 
   "central_dlp_data",
   "central_audit_data",
   "central_dq_results",
   "enterprise_reference_data"
  ])
  project                     = var.project_id
  dataset_id                  = each.key
  friendly_name               = each.key
  description                 = "${each.key} Dataset for Dataplex Demo"
  location                    = var.location
  delete_contents_on_destroy  = true
  
  depends_on = [google_storage_bucket.storage_bucket_bqtemp]
}

resource "null_resource" "setup_code" {
  provisioner "local-exec" {
    command = <<-EOT
      cd ../resources/
      gsutil -u ${var.project_id} cp gs://dataplex-dataproc-templates-artifacts/* ./common/.
      wget -P ./common/ https://github.com/mansim07/dataplex-labs/raw/main/setup/resources/code_artifacts/libs/tagmanager-1.0-SNAPSHOT.jar
      java -cp common/tagmanager-1.0-SNAPSHOT.jar  com.google.cloud.dataplex.setup.CreateTagTemplates ${var.project_id} ${var.location} data_product_information
      java -cp common/tagmanager-1.0-SNAPSHOT.jar  com.google.cloud.dataplex.setup.CreateTagTemplates ${var.project_id} ${var.location} data_product_classification
      java -cp common/tagmanager-1.0-SNAPSHOT.jar  com.google.cloud.dataplex.setup.CreateTagTemplates ${var.project_id} ${var.location} data_product_quality
      java -cp common/tagmanager-1.0-SNAPSHOT.jar  com.google.cloud.dataplex.setup.CreateTagTemplates ${var.project_id} ${var.location} data_product_exchange
      java -cp common/tagmanager-1.0-SNAPSHOT.jar  com.google.cloud.dataplex.setup.CreateDLPInspectionTemplate ${var.project_id} global marsbank_dlp_template
      sed -i s/_project_datagov_/${var.project_id}/g code/merchant-source-configs/dq_merchant_data_product.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/merchant-source-configs/dq_merchant_gcs_data.yaml
      sed -i s/_project_datasto_/${var.project_id}/g code/merchant-source-configs/dq_merchant_gcs_data.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/dq_customer_data_product.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/dq_customer_gcs_data.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/dq_tokenized_customer_data_product.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-source-configs/dq_transactions_data_product.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-source-configs/dq_transactions_gcs_data.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-consumer-configs/dq_cc_analytics_data_product.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/merchant-source-configs/data-product-classification-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/data-product-classification-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-source-configs/data-product-classification-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-consumer-configs/data-product-classification-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/merchant-source-configs/data-product-quality-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/data-product-quality-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-source-configs/data-product-quality-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-consumer-configs/data-product-quality-tag-auto.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/transactions-consumer-configs/data-product-exchange-tag-manual.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/customer-source-configs/data-product-exchange-tag-manual.yaml
      sed -i s/_project_datagov_/${var.project_id}/g code/merchant-source-configs/data-product-exchange-tag-manual.yaml
      sed -i s/_locations_/${var.location}/g code/transactions-source-configs/data-product-exchange-tag-manual.yaml 
      sed -i s/_locations_/${var.location}/g code/transactions-consumer-configs/data-product-exchange-tag-manual.yaml
      sed -i s/_locations_/${var.location}/g code/customer-source-configs/data-product-exchange-tag-manual.yaml
      sed -i s/_locations_/${var.location}/g code/merchant-source-configs/data-product-exchange-tag-manual.yaml
      gsutil -m cp -r * gs://${var.dataplex_process_bucket_name}
    EOT
    }
    depends_on = [
                  google_bigquery_dataset.bigquery_datasets,
                  google_storage_bucket.storage_bucket_process,
                  google_storage_bucket.storage_bucket_bqtemp]

  }

