# Data Organization in Dataplex

## 1. About

Dataplex allows you to logically organize your data stored in Cloud Storage and BigQuery into lakes and zones, and automate data management and governance across that data to power analytics at scale.

### 1.1. Prerequisites 
Successful completion of the setup

### 1.2. Duration
~20 mins

### 1.3 Concepts

#### 1.3.1 Lake 
A logical construct representing a data domain or business unit. For example, to organize data based on group usage, you can set up a lake per department (for example, Retail, Sales, Finance).

#### 1.3.2 Zone 
A sub-domain within a lake, useful to categorize data by stage (for example, landing, raw, curated_data_analytics, curated_data_science), usage (for example, data contract), or restrictions (for example, security controls, user access levels). Zones are of two types, raw and curated.

- Raw zone: Data that is in its raw format and not subject to strict type-checking.

- Curated zone: Data that is cleaned, formatted, and ready for analytics. The data is columnar, Hive-partitioned, in Parquet, Avro, Orc files, or BigQuery tables. Data undergoes type-checking, for example, to prohibit the use of CSV files because they do not perform as well for SQL access.

#### 1.3.3 Asset 
An asset maps to data stored in either Cloud Storage or BigQuery. You can map data stored in separate Google Cloud projects as assets into a single zone.

#### 1.3.4 Entity
An entity represents metadata for structured and semi-structured data (table) and unstructured data (fileset).

### 1.4. Scope of this lab
 In this lab you will learn how to configure the customer domain oriented lakes, zones and assets as per the diagram below. Terraform based setup takes care of setting it up for the rest of the domains - merchants, transactions, etc..

![Customer Domain Mapping](/data-mesh-banking-labs/lab1-data-organization/resources/imgs/Customer-domain-mapping.png)

### 1.5. Note
This lab currently does not configure a Metastore. 

### 1.6. Documentation
[Quick Start Guide](https://cloud.google.com/dataplex/docs/quickstart-guide)<br>
[Create lakes](https://cloud.google.com/dataplex/docs/create-lake)<br>
[Create zones](https://cloud.google.com/dataplex/docs/add-zone)

<hr>

## 2. Lab

### 2.1. Declare Variables 

In Cloud shell, copy paste the following-

```
export PROJECT_ID=$(gcloud config get-value project)
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
LOCATION="us-central1"
LOCATION_MULTI="US"
LAKE_NM="consumer-banking--customer--domain"
```

### 2.2. Create a customer domain lakes 

To create a customer domain lake, execute the below command

```
gcloud dataplex lakes create ${LAKE_NM} \
 --project=${PROJECT_ID} \
 --location=${LOCATION} \
 --labels=domain_type=source \
 --display-name="Customer Domain"
```

This takes about ~90 seconds<br>

Sample Output: 
```
Waiting for [projects/dataplex-labs-379005/locations/us-central1/operations/operation-1677463716137-5f5a4f4ec8847-22a1a609-efb20a05] to finish...done.     
Created [consumer-banking--customer--domain] Lake created in [projects/dataplex-labs-379005/locations/us-central1].
```


### 2.3. Create the zones within the lakes

```
gcloud dataplex zones create "customer-raw-zone" \
 --project=${PROJECT_ID} \
--lake=$LAKE_NM \
--resource-location-type=SINGLE_REGION \
--location=$LOCATION \
--type=RAW \
--discovery-enabled \
--discovery-schedule="0 * * * *" \
--labels=data_product_category=raw_data
```
Sample Output:
```
Waiting for [projects/dataplex-labs-379005/locations/us-central1/operations/operation-1677463916829-5f5a500e2dd1e-70029e6d-8ac1f72e] to finish...done.     
Created [customer-raw-zone] Zone created in [projects/dataplex-labs-379005/locations/us-central1/lakes/consumer-banking--customer--domain].
```
```
gcloud dataplex zones create "customer-curated-zone" \
 --project=${PROJECT_ID} \
--lake=$LAKE_NM \
--resource-location-type=SINGLE_REGION \
--location=$LOCATION \
--type=CURATED \
--discovery-enabled \
--discovery-schedule="0 * * * *" \
--labels=data_product_category=curated_data
```

```
gcloud dataplex zones create "customer-data-product-zone" \
 --project=${PROJECT_ID} \
--lake=$LAKE_NM \
--resource-location-type=SINGLE_REGION \
--location=$LOCATION \
--type=CURATED \
--discovery-enabled \
--discovery-schedule="0 * * * *" \
--labels=data_product_category=master_data
```

### 2.4. Validate lakes and zones
Go to Dataplex UI -> Go to Manage -> Click on "Customer Domain" lake -? validate the 3 Zones are created as per the screenshot here
![Customer zones](/data-mesh-banking-labs/lab1-data-organization/resources/imgs/Customer-zones.png)

### 2.5. Attach the assets to the zones

Attach the customer raw storage bucket to the raw zone

```
gcloud dataplex assets create customer-raw-data \
 --project=${PROJECT_ID} \
--location=$LOCATION \
--lake=$LAKE_NM \
--zone=customer-raw-zone \
--resource-type=STORAGE_BUCKET \
--resource-name=projects/${PROJECT_ID}/buckets/${PROJECT_ID}_customers_raw_data \
--discovery-enabled --discovery-schedule="0 * * * *" \
--csv-delimiter="|" \
--csv-header-rows=1
```

Attach the assets the customer curate zone

```
gcloud dataplex assets create customer-curated-data \
 --project=${PROJECT_ID} \
--location=$LOCATION \
--lake=$LAKE_NM \
--zone=customer-curated-zone \
--resource-type=STORAGE_BUCKET \
--resource-name=projects/${PROJECT_ID}/buckets/${PROJECT_ID}_customers_curated_data \
--discovery-enabled --discovery-schedule="0 * * * *"
```

```
gcloud dataplex assets create customer-refined-data \
 --project=${PROJECT_ID} \
--location=$LOCATION \
--lake=$LAKE_NM \
--zone=customer-curated-zone \
--resource-type=BIGQUERY_DATASET \
--resource-name=projects/${PROJECT_ID}/datasets/customer_refined_data \
--discovery-enabled --discovery-schedule="0 * * * *"
```

Attach the assets the customer data product zone

```
gcloud dataplex assets create customer-data-product \
 --project=${PROJECT_ID} \
--location=$LOCATION \
--lake=$LAKE_NM \
--zone=customer-data-product-zone \
--resource-type=BIGQUERY_DATASET \
--resource-name=projects/${PROJECT_ID}/datasets/customer_data_product \
--discovery-enabled --discovery-schedule="0 * * * *"
```
<br>

### 2.6. Validate the dataplex entities

The discovery jobs takes a few minutes to run and register the entities. Verify the entities are registered before proceeding further. 

- In the Google Cloud console, go to Dataplex 
- Navigate to the **Manage** view
- In the **Manage** view, click on the "Consumer Banking - Customer Domain" lake
- Go to the **CUSTOMER DATA PRODUT ZONE** zone 
- Click on the "ENTITIES" tab 
- The entities should list atleast 2 of the these - **customer_data** and **cc_customer_data** 

    ![Entities](/data-mesh-banking-labs/lab1-data-organization/resources/imgs/Entities.png)

This validates the discovery job ran successfully 

<br><br>

### 2.7. Review the catalog entries for GCS bucket

- In the Google Cloud console, go to Dataplex 
- Navigate to the **Manage** view
- In the **Manage** view, click on the "Consumer Banking - Customer Domain" lake
- Go to the **CUSTOMER RAW ZONE** zone 
- Go to the **DETAILS** tab 
- Observe the data observability metric. Sometimes this may be a bit delayed. Come back to this tab later to observe the stats. 
- Scroll down and and look for **Discover data** and then  click on **customer-raw-zone** hyperlink
- This will route you to the catalog search tab where you can further browse the tables to look at their technical metadata - Configuratio, Entry details, schema and columns and partition details

    ![Customer Catalog](/data-mesh-banking-labs/lab1-data-organization/resources/imgs/Customer_Catalog.png)

This hows how Dataplex was able to harvest technical medata from Google Cloud Storgae bucket. 

<br>

<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab2-data-security/README.md) where you will apply and manage data security policies using Dataplex

<hr>