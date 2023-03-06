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


variable "project_id" {}
variable "location" {}
variable "dataplex_process_bucket_name" {}
variable "dataplex_bqtemp_bucket_name" {}
/*
resource "null_resource" "curate_data" {
 for_each = {
    format("projects/%s/locations/%s/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/entities/customers_data",var.project_id, var.location) : format("projects/%s/locations/%s/lakes/consumer-banking--customer--domain/zones/customer-curated-zone/assets/customer-curated-data",var.project_id, var.location)
    #format("projects/%s/locations/%s/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/entities/cc_customers_data",var.project_id, var.location) : format("projects/%s/locations/%s/lakes/consumer-banking--customer--domain/zones/customer-curated-zone/assets/customer-curated-data",var.project_id, var.location),
    #format("projects/%s/locations/%s/lakes/consumer-banking--merchant--domain/zones/merchant-raw-zone/entities/entities/merchants_data",var.project_id, var.location) : format("projects/%s/locations/%s/lakes/consumer-banking--merchant--domain/zones/merchant-curated-zone/assets/merchant-curated-data",var.project_id, var.location),
    #format("projects/%s/locations/%s/lakes/consumer-banking--merchant--domain/zones/merchant-raw-zone/entities/entities/merchants_data",var.project_id, var.location) : format("projects/%s/locations/%s/lakes/consumer-banking--merchant--domain/zones/merchant-curated-zone/assets/mcc_codes",var.project_id, var.location),
    #format("projects/%s/locations/%s/lakes/consumer-banking--creditcards--transaction--domain/zones/authorizations-raw-zone/entities/auth_data",var.project_id, var.location) : format("projects/%s/locations/%s/lakes/consumer-banking--creditcards--transaction--domain/zones/authorization-curated-zone/assets/transactions-curated-data",var.project_id, var.location)
  }  

  provisioner "local-exec" {
    command = format("gcloud beta dataflow flex-template run test --service-account-email %s --template-file-gcs-location %s --region %s --parameters \"inputAssetOrEntitiesList=%s,outputFileFormat=PARQUET,outputAsset=%s\"", 
                     "customer-sa@${var.project_id}.iam.gserviceaccount.com",
                     "gs://dataflow-templates-us-central1/latest/flex/Dataplex_File_Format_Conversion",
                     var.location,
                     each.key,
                     each.value
                     )
  }
}
*/

resource "random_id" "rng" {
  keepers = {
    first = "${timestamp()}"
  }     
  byte_length = 8
}

variable "command_string" {
  type = string
  default = <<-EOT
gcloud dataplex tasks create %s \
    --project=%s \
    --location=%s \
    --vpc-sub-network-name=%s \
    --lake=%s \
    --trigger-type=ON_DEMAND \
    --execution-service-account=%s \
    --spark-main-class="com.google.cloud.dataproc.templates.main.DataProcTemplate" \
    --spark-file-uris="gs://%s/log4j-spark-driver-template.properties" \
    --container-image-java-jars="gs://%s/common/dataproc-templates-1.0-SNAPSHOT.jar" \
    --execution-args=^::^TASK_ARGS="--template=DATAPLEXGCSTOBQ,\
        --templateProperty=project.id=%s,\
        --templateProperty=dataplex.gcs.bq.target.dataset=%s,\
        --templateProperty=gcs.bigquery.temp.bucket.name=%s,\
        --templateProperty=dataplex.gcs.bq.save.mode=append,\
        --templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,\
        --dataplexEntity=%s,\
        --partitionField=ingest_date,\
        --partitionType=DAY,\
        --customSqlGcsPath=gs://%s/customer-source-configs/customercustom.sql"
EOT
}

resource "null_resource" "copy_merchant_data_asset" {
  for_each = {
    format("merchants_refined_data/merchant-sa@%s.iam.gserviceaccount.com", var.project_id) : format("projects/%s/locations/%s/lakes/consumer-banking--merchant--domain/zones/merchant-raw-zone/entities/merchants_data",var.project_id, var.location),
  }

  provisioner "local-exec" {
    command = format(var.command_string,
                format("DATAPLEXGCSTOBQ-%s-%s", element(split("/", each.key), 0), random_id.rng.hex), 
                var.project_id,
                var.location,
                format("regions/us-central1/subnetworks/default", var.project_id),
                element(split("/", each.value), 5),
                element(split("/", each.key), 1),
                var.dataplex_process_bucket_name,
                var.dataplex_process_bucket_name,
                var.project_id,
                element(split("/", each.key), 0),
                var.dataplex_bqtemp_bucket_name,
                each.value,
                var.dataplex_process_bucket_name
                )
  }
}

resource "null_resource" "copy_trans_data_asset" {
  for_each = {
    format("pos_auth_refined_data/merchant-sa@%s.iam.gserviceaccount.com", var.project_id) : format("projects/%s/locations/%s/lakes/consumer-banking--creditcards--transaction--domain/zones/authorizations-raw-zone/entities/auth_data",var.project_id, var.location)
  }

  provisioner "local-exec" {
    command = format(var.command_string,
                format("DATAPLEXGCSTOBQ-%s-%s", element(split("/", each.key), 0), random_id.rng.hex), 
                var.project_id,
                var.location,
                format("regions/us-central1/subnetworks/default", var.project_id),
                element(split("/", each.value), 5),
                element(split("/", each.key), 1),
                var.dataplex_process_bucket_name,
                var.dataplex_process_bucket_name,
                var.project_id,
                element(split("/", each.key), 0),
                var.dataplex_bqtemp_bucket_name,
                each.value,
                var.dataplex_process_bucket_name
                )
  }
  depends_on = [
    null_resource.copy_merchant_data_asset
  ]
}

