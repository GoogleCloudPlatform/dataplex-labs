#!/bin/bash

# Set the Security Policy 
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