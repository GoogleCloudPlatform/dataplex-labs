#!/bin/bash
#set -x

###################################################################
# This is a helper script to trigger the terraform setup
# Author: maharanam@
###################################################################
if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "Usage: ./deploy_helper.sh <project-d> <ldap-or-email> <env>"
    echo "Example: ./deploy_helper.sh my-datastore maharanam argolis"
    exit 1
fi

GCP_PROJECT_ID=$1
GCP_LDAP=$2
GCP_ENV=$3

# Argolis Environment does has organization admin
# privileges to set organization policies
# Organ policies setup is skipped for non-argolis accounts

if [ ${GCP_ENV} == "argolis" ]; then 
    echo "${GCP_PROJECT_ID}"
    cd ~/dataplex-labs/data-mesh-banking-labs/setup/org_policy
    gcloud config set project ${GCP_PROJECT_ID}
    terraform init
    terraform apply -auto-approve -var project_id=${GCP_PROJECT_ID} 
    status=$?
    [ $status -eq 0 ] && echo "command successful" || exit 1

else 
    echo "${GCP_PROJECT_ID}"
    cd ~/dataplex-labs/data-mesh-banking-labs/setup/org_policy_external
    gcloud config set project ${GCP_PROJECT_ID}
    terraform init
    terraform apply -auto-approve -var project_id=${GCP_PROJECT_ID} 
    status=$?
    [ $status -eq 0 ] && echo "command successful" || exit 1
fi

cd ~/dataplex-labs/data-mesh-banking-labs/setup/terraform
gcloud config set project ${GCP_PROJECT_ID}
terraform init
terraform apply -auto-approve -var project_id=${GCP_PROJECT_ID}
status=$?
[ $status -eq 0 ] && echo "command successful" || exit 1