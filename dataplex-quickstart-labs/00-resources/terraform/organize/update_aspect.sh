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

# Check if the number of arguments is 7
if [ "$#" -ne 7 ]; then
  echo "Error: This script requires 7 arguments." >&2  # Output error to stderr
  echo "Usage: $0 projectid region entry entrygroup aspectid aspectname aspectvalue" >&2 # Print correct usage to stderr
  exit 1  # Return an error status (1)
fi

project=$1
region=$2
entry=$3
entrygroup=$4
aspectid=$5
aspectname=$6
aspectvalue=$7

yes | pip3 install google-cloud-dataplex -q -q -q --exists-action i

#check if we have a bigquery entry
if [ "$entrygroup" == "@bigquery" ]; then
  dataset=$(echo "$entry" | cut -d'.' -f1)
  table=$(echo "$entry" | cut -d'.' -f2)
  entry="bigquery.googleapis.com/projects/$project/datasets/$dataset/tables/$table"
fi

##echo $entry
python3 update_aspect.py $project $region $entry $entrygroup $aspectid $aspectname $aspectvalue
## ./update_aspect.sh dataplex-demo032025v8 us-central1 "credit_card_795099425658.credit_card_795099425658" "@bigquery" zone type oda-raw-zone