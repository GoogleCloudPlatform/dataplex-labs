#!/bin/bash
touch backend.tf
rm -r backend.tf
echo 'terraform {
backend "gcs" {
bucket = "'$1'"
prefix = "'$2'/'$3'/Org_Project_Policy-state"
}
}' >> backend.tf
