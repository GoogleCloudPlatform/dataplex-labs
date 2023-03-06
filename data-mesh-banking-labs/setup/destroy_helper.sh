#!/bin/bash
#set -x
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "Usage: ./deploy_helper.sh <datastore-projectid> <datagov-projectid> <ldap> <randid>"
    echo "Example: ./deploy_helper.sh my-datastore my-datagov jayoleary 123"
    exit 1
fi
GCP_DATASTORE_PROJECT_ID=$1
GCP_DATAGOV_PROJECT_ID=$2
GCP_ARGOLIS_LDAP=$3
RAND=$4

echo "${GCP_DATASTORE_PROJECT_ID}"
cd ~/datamesh-on-gcp/oneclick/org_policy
gcloud config set project ${GCP_DATASTORE_PROJECT_ID}
terraform destroy -auto-approve -var project_id_storage=${GCP_DATASTORE_PROJECT_ID} -var project_id_governance=${GCP_DATAGOV_PROJECT_ID}
status=$?
[ $status -eq 0 ] && echo "command successful" || exit 1

rm terraform*
rm -rf .terraform*

cd ~/datamesh-on-gcp/oneclick/demo-store/terraform
gcloud config set project ${GCP_DATASTORE_PROJECT_ID}
terraform destroy -auto-approve -var project_id=${GCP_DATASTORE_PROJECT_ID}
status=$?
[ $status -eq 0 ] && echo "command successful" || exit 1

rm terraform*
rm -rf .terraform*

cd ../../demo-gov/terraform
gcloud config set project ${GCP_DATAGOV_PROJECT_ID}

terraform destroy -auto-approve -var project_id_governance=${GCP_DATAGOV_PROJECT_ID} -var project_id_storage=${GCP_DATASTORE_PROJECT_ID} -var ldap=${GCP_ARGOLIS_LDAP} -var user_ip_range=10.6.0.0/24

status=$?
[ $status -eq 0 ] && echo "command successful" || exit 1

rm terraform*
rm -rf .terraform*
