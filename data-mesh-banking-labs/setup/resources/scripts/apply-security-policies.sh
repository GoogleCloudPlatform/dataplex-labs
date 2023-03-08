#!/bin/bash

export PROJECT_ID=$(gcloud config get-value project)

# Will be done as art of lab#1
# Set the Security Policy for Customer Domain 


#export customer_lake_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:customer-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"
#curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain:setIamPolicy -d " ${customer_lake_policy}"
#if [ $? -eq 0 ]; then 
#    echo "Customer Lake security policy applied successfully!"
#else 
#   echo "Customer Lake security policy application failed!"
#   exit 1 
#fi 
#sleep 1m 


#export customer_data_product_assert_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataReader\",\"members\":[\"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"
#curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-data-product-zone:setIamPolicy -d "${customer_data_product_assert_policy}"

#if [ $? -eq 0 ]; then 
#    echo "Customer Zone security policy applied successfully!"
#else 
#   echo "Customer  Zone security policy application failed!"
#   exit 1 
#fi 
#sleep 1m


# Set the Security Policy for Common Domain 

export central_dq_policy="{\"policy\":{
\"bindings\": [
{
    \"role\": \"roles/dataplex.dataOwner\",
    \"members\": [
    \"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",
\"serviceAccount:cc-trans-sa@${PROJECT_ID}.iam.gserviceaccount.com\",   \"serviceAccount:customer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",    \"serviceAccount:merchant-sa@${PROJECT_ID}.iam.gserviceaccount.com\"
    ]
}
]
}
}"

echo $central_dq_policy

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/operations-data-product-zone:setIamPolicy -d "${central_dq_policy}"

if [ $? -eq 0 ]; then 
    echo "Operations Data Product Zone security policy applied successfully!"
else 
   echo "Operations Data Product Zone security policy application failed!"
   exit 1 
fi 
sleep 1m

# 1. CREATE POLICY
export central_common_util_policy="{\"policy\":{
\"bindings\": [
{
    \"role\": \"roles/dataplex.dataReader\",
    \"members\": [
    \"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",
\"serviceAccount:cc-trans-sa@${PROJECT_ID}.iam.gserviceaccount.com\",   \"serviceAccount:customer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",    \"serviceAccount:merchant-sa@${PROJECT_ID}.iam.gserviceaccount.com\"
    ]
}
]
}
}"

echo " "
# 2. VIEW POLICY
echo "==========="
echo "The policy we just created is "
echo "==========="
echo " "
echo $central_common_util_policy


echo " "
# 3. APPLY POLICY
echo "==========="
curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/common-utilities:setIamPolicy -d "${central_common_util_policy}"
echo " "
echo "==========="

sleep 1m


#gcloud logging --project=${PROJECT_ID} sinks create audits-to-bq bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/central_audit_data --log-filter='resource.type="audited_resource" AND resource.labels.service="dataplex.googleapis.com" AND protoPayload.serviceName="dataplex.googleapis.com"'

#LOGGING_GMSA=`gcloud logging sinks describe audits-to-bq | grep writerIdentity | grep serviceAccount | cut -d":" -f3`
#echo $LOGGING_GMSA

#curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/operations-data-product-zone/assets/audit-data:setIamPolicy -d "{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:$LOGGING_GMSA\"]}]}}" 


export PROJECT_NBR=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
echo $PROJECT_NBR

curl --request POST \
"https://dlp.googleapis.com/v2/projects/${PROJECT_ID}/locations/us-central1/content:inspect" \
--header "X-Goog-User-Project: ${PROJECT_ID}" \
--header "Authorization: Bearer $(gcloud auth print-access-token)" \
--header 'Accept: application/json' \
--header 'Content-Type: application/json' \
--data '{"item":{"value":"google@google.com"}}' \
--compressed

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/central-operations--domain/zones/operations-data-product-zone/assets/dlp-reports:setIamPolicy -d "{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:service-${PROJECT_NBR}@dlp-api.iam.gserviceaccount.com\"]}]}}"

# Set the Security Policy for Merchant Domain 

export merchant_lake_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:merchant-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--merchant--domain:setIamPolicy -d " ${merchant_lake_policy}"

if [ $? -eq 0 ]; then 
    echo "Merchant Lake security policy applied successfully!"
else 
   echo "Merchant Lake security policy application failed!"
   exit 1 
fi 

sleep 1m
export merchant_data_product_assert_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataReader\",\"members\":[\"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--merchant--domain/zones/merchant-data-product-zone:setIamPolicy -d "${merchant_data_product_assert_policy}"

if [ $? -eq 0 ]; then 
    echo "Merchant Zone security policy applied successfully!"
else 
   echo "Merchant Zone security policy application failed!"
   exit 1 
fi 

sleep 1m 

export transaction_lake_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:cc-trans-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}" 

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--creditcards--transaction--domain:setIamPolicy -d "${transaction_lake_policy}"

if [ $? -eq 0 ]; then 
    echo "Transaction Lake security policy applied successfully!"
else 
   echo "Transaction Lake security policy application failed!"
   exit 1 
fi 

sleep 1m 

export  transaction_data_product_assert_policy="{\"policy\":{\"bindings\":[{\"role\":\"roles/dataplex.dataReader\",\"members\":[\"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--creditcards--transaction--domain/zones/authorizations-data-product-zone:setIamPolicy -d "${transaction_data_product_assert_policy}"

if [ $? -eq 0 ]; then 
    echo "Transaction Zone security policy applied successfully!"
else 
   echo "Transaction Zone  policy application failed!"
   exit 1 
fi 

sleep 1m 

export  transaction_consumer_lake_policy="{\"policy\":{\"bindings\":[ {\"role\":\"roles/dataplex.dataOwner\",\"members\":[\"serviceAccount:cc-trans-consumer-sa@${PROJECT_ID}.iam.gserviceaccount.com\"]}]}}"

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://dataplex.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--creditcards--analytics--domain:setIamPolicy -d "${transaction_consumer_lake_policy}"

if [ $? -eq 0 ]; then 
    echo "Credit Card Analytics lake security policy applied successfully!"
else 
   echo "Credit Card Analytics lake  policy application failed!"
   exit 1 
fi 