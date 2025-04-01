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

resource "null_resource" "discover_tables" {
  provisioner "local-exec" {
    command = "gcloud alpha dataplex datascans create data-discovery ${var.scanname} --location=${var.gcp_region} --data-source-resource='//storage.googleapis.com/projects/${var.project_id}/buckets/${var.bucket}' --bigquery-publishing-table-type=EXTERNAL"
    interpreter = ["/bin/bash", "-c"]
  }
}

/*
* The discovery doesn't seem to run when on-demand is set so forcing a run
*/
resource "null_resource" "run_discovery" {
  provisioner "local-exec" {
    command = <<EOT
    gcloud alpha dataplex datascans run ${var.scanname} --location=${var.gcp_region}; \
    sleep 180; \
    gcloud alpha dataplex datascans run ${var.scanname} --location=${var.gcp_region}; \
EOT
    interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [null_resource.discover_tables]
}

/*******************************************
Wait 10 minutes for Discovery to Complete
********************************************/
resource "time_sleep" "sleep_after_discovery" {
  create_duration = "10m"

  depends_on = [
    null_resource.run_discovery
  ]
}

data "external" "get_table_name_from_bq" {
  program = ["/bin/bash", "./get_table_name.sh", var.scanname] # Script to read the file
  depends_on = [time_sleep.sleep_after_discovery]
}

output "table_name" {
  value = split("\n", data.external.get_table_name_from_bq.result.table_name)
  depends_on = [data.external.get_table_name_from_bq]
}
