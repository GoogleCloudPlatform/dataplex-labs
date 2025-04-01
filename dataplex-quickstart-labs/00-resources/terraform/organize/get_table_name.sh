##
 # Copyright 2025 Google LLC
 #
 # Licensed under the Apache License, Version 2.0 (the "License");
 # you may not use this file except in compliance with the License.
 # You may obtain a copy of the License at
 #
 #      http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.
 ##

#!/bin/bash
#set -o xtrace

# Check if the number of arguments is 7
if [ "$#" -ne 1 ]; then
  echo "Error: This script requires 1 argument." >&2  # Output error to stderr
  echo "Usage: $0 scanname" >&2 # Print correct usage to stderr
  exit 1  # Return an error status (1)
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
  echo "jq is not installed."
  exit 1
fi

scanname=$1

#use the scanname to find the dataset name
cmd="bq ls --format=prettyjson | jq '.[] | select(.labels.\"dataplex-discovery-scan-id\" == \"${scanname}\") | .datasetReference.datasetId'"
dataset_name=`eval "${cmd}"`
dataset_name="${dataset_name//\"/}"

#from the dataset name find the table name
table_name=`bq ls --format=prettyjson $dataset_name  | jq '.[] | select(.labels."metadata-managed-mode" == "discovery_managed") | .tableReference.tableId'`
table_name="${table_name//\"/}"

#if no table name was discovered then return an error
if [[ -z "$table_name" ]]; then
  echo "Error: The scan '${scanname}' did not discovery any tables." >&2
  exit 1
fi
echo '{"table_name":"'$dataset_name.$table_name'"}' # Output text
