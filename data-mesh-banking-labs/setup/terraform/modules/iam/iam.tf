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

####################################################################################
# Create one service account for each data domain
####################################################################################
resource "google_service_account" "data_service_account" {
  project      = var.project_id
   for_each = {
    "customer-sa" : "customer-sa",
    "merchant-sa" : "merchant-sa",
    "cc-trans-consumer-sa" : "cc-trans-consumer-sa",
    "cc-trans-sa" : "cc-trans-sa"
    }
  account_id   = format("%s", each.key)
  display_name = format("Demo Service Account %s", each.value)

}

####################################################################################
# Assign IAM Roles to the above service account. 
# We will use Dataplex for managing Data Security. This module is for compute only.  
####################################################################################

resource "google_project_iam_member" "iam_customer_sa" {
  for_each = toset([
"roles/iam.serviceAccountUser",
"roles/iam.serviceAccountTokenCreator",
"roles/serviceusage.serviceUsageConsumer",
"roles/bigquery.jobUser",
"roles/dataflow.worker",
"roles/dataplex.developer",
"roles/dataplex.metadataReader",
"roles/dataplex.metadataWriter",
"roles/metastore.metadataEditor",
"roles/metastore.serviceAgent",
"roles/dataproc.worker",
"roles/cloudscheduler.jobRunner",
"roles/dataplex.viewer",
"roles/datacatalog.tagEditor",
"roles/bigquery.user"
])
  project  = var.project_id
  role     = each.key
  member   = format("serviceAccount:customer-sa@%s.iam.gserviceaccount.com", var.project_id)

  depends_on = [
    google_service_account.data_service_account
  ]

}


resource "google_project_iam_member" "iam_merchant_sa" {
  for_each = toset([
"roles/iam.serviceAccountUser",
"roles/iam.serviceAccountTokenCreator",
"roles/serviceusage.serviceUsageConsumer",
"roles/artifactregistry.reader",
"roles/bigquery.jobUser",
"roles/dataflow.worker",
"roles/dataplex.editor",
"roles/dataplex.developer",
"roles/dataplex.metadataReader",
"roles/dataplex.metadataWriter",
"roles/metastore.metadataEditor",
"roles/metastore.serviceAgent",
"roles/dataproc.worker",
"roles/dataflow.admin",
"roles/dataflow.worker",
"roles/cloudscheduler.jobRunner",
"roles/dataplex.viewer",
"roles/datacatalog.tagEditor",
"roles/bigquery.user"
])
  project  = var.project_id
  role     = each.key
  member   = format("serviceAccount:merchant-sa@%s.iam.gserviceaccount.com", var.project_id)

  depends_on = [
    google_service_account.data_service_account
  ]
}


resource "google_project_iam_member" "iam_cc_trans_sa" {
  for_each = toset([
"roles/iam.serviceAccountUser",
"roles/iam.serviceAccountTokenCreator",
"roles/serviceusage.serviceUsageConsumer",
"roles/artifactregistry.reader",
"roles/bigquery.jobUser",
"roles/dataflow.worker",
"roles/dataplex.editor",
"roles/dataplex.developer",
"roles/dataplex.metadataReader",
"roles/dataplex.metadataWriter",
"roles/metastore.metadataEditor",
"roles/metastore.serviceAgent",
"roles/dataproc.worker",
"roles/dataflow.admin",
"roles/dataflow.worker",
"roles/cloudscheduler.jobRunner",
"roles/dataplex.viewer",
"roles/datacatalog.tagEditor",
"roles/bigquery.user"
])
  project  = var.project_id
  role     = each.key
  member   = format("serviceAccount:cc-trans-sa@%s.iam.gserviceaccount.com", var.project_id)

  depends_on = [
    google_service_account.data_service_account
  ]
}

resource "google_project_iam_member" "iam_cc_trans_consumer_sa" {
  for_each = toset([
"roles/iam.serviceAccountUser",
"roles/iam.serviceAccountTokenCreator",
"roles/serviceusage.serviceUsageConsumer",
"roles/artifactregistry.reader",
"roles/bigquery.jobUser",
"roles/dataflow.worker",
"roles/dataplex.editor",
"roles/dataplex.developer",
"roles/dataplex.metadataReader",
"roles/dataplex.metadataWriter",
"roles/metastore.metadataEditor",
"roles/metastore.serviceAgent",
"roles/dataproc.worker",
"roles/dataflow.admin",
"roles/dataflow.worker",
"roles/cloudscheduler.jobRunner",
"roles/dataplex.viewer",
"roles/datacatalog.tagEditor",
"roles/bigquery.user"
])
  project  = var.project_id
  role     = each.key
  member   = format("serviceAccount:cc-trans-consumer-sa@%s.iam.gserviceaccount.com", var.project_id)

  depends_on = [
    google_service_account.data_service_account
  ]
}



/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_network_and_iam_steps" {
  create_duration = "120s"
  depends_on = [
               google_project_iam_member.iam_cc_trans_consumer_sa
              ]
}