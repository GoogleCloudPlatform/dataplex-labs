# Dataplex Catalog: Data discovery, search and Data Lineage 

## 1. About

Dataplex makes data easily searchable and discoverable based on a number of facets - including business domains, classification, and operational metrics such as data quality. As a data consumer you simply search for the data you are looking for - based on its domain, classification, ownership, name, and so on. 

Now it's time to explore how you can use the Dataplex catalog to perform advanced data discovery by being to trigger searches based on business metadata, get 360 degree context of your data product, provide business overview and explore data lineage. 

### 1.1. Prerequisites 
Successful completion of lab1, lab2, lab4 and Lab7 

### 1.2. Duration
~20  mins

### 1.3 Concepts


### 1.4. Scope of this lab

1. Adding additional business context through Overview 
2. Data Lineage
3. Create Custom Lineage

### 1.5. Note


### 1.6. Documentation

## 2. Lab

### 2.1 Add rich text overview to your data product

1. Go to Dataplex UI --> Search under Discover --> Type this in the search bar "tag:data_product_information customer data"
2. Click on the customer_data entry 
3. Under **OVERVIEW** -> Click on **+ADD OVERVIEW** -> paste the below text and you also insert a sample icon to represent your data product. 
```
Customer Demograhics Data Product 
This customer data table contains the data for customer demographics of all Bank of Mars retail banking customers. It contains PII information that can be accessed on "need-to-know" basis. 

Customer data is the information that customers give us while interacting with our business through websites, applications, surveys, social media, marketing initiatives, and other online and offline channels. A good business plan depends on customer data. Data-driven businesses recognize the significance of this and take steps to guarantee that they gather the client data points required to enhance client experience and fine-tune business strategy over time.
```
- Click Save 
- Sample screenshot 
![overview](/data-mesh-banking-labs/lab8-data-discovery-lineage/resources/imgs/cust_data_overview.png)
- Now as you see you have a 360 degree view of the customer data product - including technical metadata like schemas and fields , business metadata like wiki style product overview, business metadata such as - classification info, dq scores, data ownership..

![dp-overview](/data-mesh-banking-labs/lab8-data-discovery-lineage/resources/imgs/dp-overview.png) 

4. You can also use the below API call to populate the Overview 

    ```
    export PROJECT_ID=$(gcloud config get-value project)

    entry_name=`curl -X GET -H "x-goog-user-project: ${PROJECT_ID}" -H  "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" "https://datacatalog.googleapis.com/v1/entries:lookup?linkedResource=//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/customer_data_product/tables/customer_data&fields=name" | jq -r '.name'`

    curl -X POST -H "x-goog-user-project: d${PROJECT_ID}" -H  "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application.json" https://datacatalog.googleapis.com/v1/${entry_name}:modifyEntryOverview -d "{\"entryOverview\":{\"overview\":\"  <div class='ping' style='width:2000px;text-align: left;' ></div><header><h1>&#128508; Customer Demograhics Data Product</h1></header><br>This customer data table contains the data for customer demographics of all Bank of Mars retail banking customers. It contains PII information that can be accessed on need-to-know basis. <br> Customer data is the information that customers give us while interacting with our business through websites, applications, surveys, social media, marketing initiatives, and other online and offline channels. A good business plan depends on customer data. Data-driven businesses recognize the significance of this and take steps to guarantee that they gather the client data points required to enhance client experience and fine-tune business strategy over time.\"}}"
    ```


### 2.2 Data discovery and Search 

1. Use business filters to search for data products. Try out the below search string: 

    | What is your end user looking for?  | Search String |
    | ----------------------- | ------------- |
    | Search for data products  | tag:data_product_information |
    | Search for all data products that belong to the consumer banking domain | tag:data_product_information.domain:Consumer Banking  |
    | Search for all data products that are owned by Rebecca Piper (Customer Domain Owner) | tag:data_product_information.domain_owner:hannah.anderson@boma.com |
    | Search based on quality Score | tag:data_product_quality.data_quality_score>50 |
    | List all the data products with PII info | tag:data_product_classification.is_pii=true |
    | List data products which meets the SLA | tag:data_product_quality.timeliness_score=100 |
    | List all data products hosted on Analytics Hub | tag:data_product_exchange.data_exchange_platform:Analytics Hub |
    | List of data products that don’t have Data Quality Scores | tag:data_product_quality.data_quality_score=-1 |
    | Search for data products that our Master Data | tag:data_product_information.data_product_category="Master Data" | 

### 2.3 Explore Data lineage

1. Go to Dataplex UI --> Search under Discover --> Type this in the search bar "system=bigquery credit_card_transaction_data"
2.  Click on the credit_card_transaction_data entry
3.  Click on Data Lineage to explore the lineage
![lineage](/data-mesh-banking-labs/lab8-data-discovery-lineage/resources/imgs/lineage.png)

### 2.4 Create Custom  lineage

#### 2.4.1 Setup the environment variable

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCATION="us-central1"
LOCATION_MULTI="us"
ENTRY_ID="mysql_crm_customer_data"
```

#### 2.4.2 Create a custom entry 

Execute the below command in cloud shell 

```
rm -rf requestCustomEntry.json

echo "{\"description\": \"MySql Customer dataset\",\"displayName\": \"MySql Customer Data\",\"user_specified_type\": \"CSV\", \"fullyQualifiedName\": \"mysql:crm_customer_data\" , \"linked_resource\": \"//mysql.googleapis.com/mysql/crm\", \"user_specified_system\": \"mysql\"}" >>  requestCustomEntry.json

curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -H "x-goog-user-project: $PROJECT_ID" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d @requestCustomEntry.json \
    "https://datacatalog.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION/entryGroups/mysql/entries?entryId=$ENTRY_ID"

```

#### 2.4.3 Create a process

Paste the below in cloud shell-

```

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application.json" \
https://us-datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION_MULTI/processes \
-d "{\
  \"displayName\": \"Load MySQL CRM data into GCS\" \
}"

```

#### 2.4.4 List and capture the  processes id

Paste the below in cloud shell-

```
mkdir -p ~/temp-lineage

rm -rf ~/temp-lineage/lineage_processes_listing.json

curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application.json" \
https://us-datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION_MULTI/processes >> ~/temp-lineage/lineage_processes_listing.json

CUSTOM_LINEAGE_PROCESS_ID=`cat lineage_processes_listing.json | grep -C 1 MySQL | grep name | cut -d'/' -f6 | tr -d \" | tr -d ,`

echo $CUSTOM_LINEAGE_PROCESS_ID

```

Author's process sample:

```
67bb9f5a-88cd-49d4-8d68-a1f82cde7d5a
```

#### 2.4.5 Create a run for the custom lineage process

Paste the below in cloud shell-

```
curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application.json" \
https://us-datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION_MULTI/processes/$CUSTOM_LINEAGE_PROCESS_ID/runs -d "{\
  \"displayName\": \"One time load\", \
  \"startTime\": \"2022-01-23T14:14:11.238Z\", \
  \"endTime\": \"2022-01-23T14:16:11.238Z\", \
  \"state\": \"COMPLETED\" \
}"

```
#### 2.4.6 List and capture the process run 

Paste the below in cloud shell-


```
cd ~/temp-lineage
rm -rf custom_lineage_run.json

curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application.json" \
https://us-datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION_MULTI/processes/$CUSTOM_LINEAGE_PROCESS_ID/runs >> custom_lineage_run.json

CUSTOM_LINEAGE_PROCESS_RUN_ID=`cat custom_lineage_run.json | grep name | grep / | cut -d'/' -f8 | tr -d \" | tr -d ,`

echo $CUSTOM_LINEAGE_PROCESS_RUN_ID

```

#### 2.4.7 List and capture the process run 

Paste the below in cloud shell-

```
curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application.json" \
https://us-datalineage.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION_MULTI/processes/$CUSTOM_LINEAGE_PROCESS_ID/runs/$CUSTOM_LINEAGE_PROCESS_RUN_ID/lineageEvents -d "{\
  \"links\": [ \
    { \
      \"source\": { \
        \"fullyQualifiedName\":\"mysql:crm_customer_data\" \
      }, \
      \"target\": { \
        \"fullyQualifiedName\":\"bigquery:$PROJECT_ID.customer_refined_data.customers_data\" \
      }, \
    } \
  ], \
  \"startTime\": \"2022-01-01T14:14:11.238Z\", \
}"
```
#### 2.4.8 Explore the custom lineage 

1. Go to Dataplex UI --> Search under Discover --> Type this in the search bar "system=bigquery customer_data tag:data_product_information"
2.  Click on the **customer_data** entry
3.  Go to the **Lineage** tab 
4. Click on the **+** on the customers_data node

![custom_lineage](/data-mesh-banking-labs/lab8-data-discovery-lineage/resources/imgs/custom_lineage.png)
<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab8-data-discovery-lineage/) where you will learn to trigger a auto data profiling job

<hr>