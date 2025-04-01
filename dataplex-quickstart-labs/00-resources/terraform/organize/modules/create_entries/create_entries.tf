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
  bucket_name = each.value.bucket_name
  bucket_folder = each.value.bucket_folder
  entry_group_id = google_data_catalog_entry_group.entry_group.id
  entry_id = each.value.entry_id
  project_nbr = "${var.project_number}"
  region = var.gcp_region
  project_id = var.project_id

 */

locals {
  project_id = var.project_id
  project_nbr = var.project_nbr
  entry_group_id = var.entry_group_id
  entry_name = var.entry_id
  location = var.region
  bucket_name = var.bucket_name
  folder_name = var.folder_name
  aspect_name = var.aspect_name
}
 
resource "random_uuid" "filename" {
}

resource "null_resource" "create_entry_jsonv3" {
  provisioner "local-exec" {
  command = <<EOT
    json_data=$(cat template.json);\
    modified_json=$(echo "$json_data" | sed "s!#PROJECT!${local.project_id}!g" \
    | sed "s!#LOCATION!${local.location}!g" \
    | sed "s!#BUCKETNAME!${local.bucket_name}!g" \
    | sed "s!#FOLDERNAME!${local.folder_name}!g" \
    | sed "s!#ASPECTNAME!${local.aspect_name}!g");\
    echo $modified_json > txt${random_uuid.filename.result}.json
  EOT
  interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [
    random_uuid.filename
  ]
}

resource "null_resource" "create_entry" {
  provisioner "local-exec" {
command = <<EOT
    gcloud dataplex entries create ${local.entry_name} \
    --project=${local.project_id} \
    --location=${local.location} \
    --entry-group=${local.entry_group_id}	 \
    --entry-type=projects/dataplex-types/locations/global/entryTypes/generic \
    --entry-source-resource=//storage.googleapis.com/projects/_/buckets/${local.bucket_name} \
    --aspects=txt${random_uuid.filename.result}.json
EOT
    interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [
    random_uuid.filename,
    null_resource.create_entry_jsonv3
  ]
}
