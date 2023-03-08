# Building Data products 

## 1. About
Dataplex provides building blocks and plugable data management tasks that can be plugged into data engineering pipelines to build data products. In this lab, you will learn how to build Data Products. The diagram below depicts the overall flow. 

![build-dp-flow](/data-mesh-banking-labs/lab4-build-data-products/resources/imgs/building-dp-flow.png)

### 1.1. Prerequisites
Lab2-data-security successfully completed.

### 1.2. Duration
~40 mins

### 1.3 Concepts
None

### 1.4. Scope of this lab

In this lab,  will focus on building Data products for all the source and consumer oriented domains. 
1. We will use Open Source Spark-serverless based Dataproc Templates to move the data incrementally from GC(raw/curated buckets) to BigQuery(refined datasets)
2. We will use Dataplex DQ to validate the incoming data quality 
3. Use BQ SQL to transform the data and populate the final data products. We can also take necessary actions based on the DQ results(will be added to lab in future)
4. We will use Dataplex catalog to search for the data products based on technical metadata
5. We will use Composer to orchestrate the workflows for Merchants, Credit card analytics and transactions domains.  

### 1.5. Note
None

### 1.6. Documentation
None

## 2. Labs

### 2.1. Building Customer Domain Data Products

#### 2.1.1.  Move customer data from raw/curated zone in GCS to refined zone in BQ
Use Dataproc template to move the data from raw/curated zone to refine zone in BQ  

- **Step 1**: Open cloud shell and run the below command 

    ```bash 
     export PROJECT_ID=$(gcloud config get-value project)

    gcloud dataplex tasks create cust-curated-refined \
        --project=${PROJECT_ID} \
        --location=us-central1 \
    --vpc-sub-network-name=projects/${PROJECT_ID}/regions/us-central1/subnetworks/dataplex-default \
        --lake=consumer-banking--customer--domain \
        --trigger-type=ON_DEMAND \
        --execution-service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com \
        --spark-main-class="com.google.cloud.dataproc.templates.main.DataProcTemplate" \
        --spark-file-uris="gs://${PROJECT_ID}_dataplex_process/common/log4j-spark-driver-template.properties" \
        --container-image-java-jars="gs://${PROJECT_ID}_dataplex_process/common/dataproc-templates-1.0-SNAPSHOT.jar" \
        --execution-args=^::^TASK_ARGS="--template=DATAPLEXGCSTOBQ,\
            --templateProperty=project.id=${PROJECT_ID},\
            --templateProperty=dataplex.gcs.bq.target.dataset=customer_refined_data,\
            --templateProperty=gcs.bigquery.temp.bucket.name=${PROJECT_ID}_dataplex_temp,\
            --templateProperty=dataplex.gcs.bq.save.mode=append,\
            --templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,\
            --dataplexEntity=projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/entities/customers_data,\
            --partitionField=ingest_date,\
            --partitionType=DAY,\
            --targetTableName=customers_data,\
            --customSqlGcsPath=gs://${PROJECT_ID}_dataplex_process/code/customer-source-configs/customercustom.sql"
    ```
    Sample Output: 
    ```Create request issued for: [cust-curated-refined]
    Waiting for operation [projects/dataplex-lab5/locations/us-central1/operations/operation-1673931152987-5f26e77c0967f-9cfa82a7-22f0b7d8] to complete...working.
    Waiting for operation [projects/dataplex-lab5/locations/us-central1/operations/operation-1673931152987-5f26e77c0967f-9cfa82a7-22f0b7d8] to complete...done.
    Created task [cust-curated-refined].
    ```
 - **Step 2**: Monitor the Job. It will take a few seconds to spin up, execute and complete 
    - Go to Dataplex process tab → Choose “Custom Spark” → Click on the name of your task → Click on the job-id (wait for a few seconds and refresh if the job_id URL is not active) → This will take you to Dataproc Batch tab where you can look at the output for jobs logs and Details tab for input arguments 
     ![dataplex-task-output](/data-mesh-banking-labs/lab4-build-data-products/resources/imgs/dplx-task-output.png)

     One the status is “Successful” move on to next step

- **Step 3**: Validate the data. This is a critical validation to make sure data is populated.  
    - Open cloud shell and execute the below 2 command and make sure the count > 0
        ```bash 
        export PROJECT_ID=$(gcloud config get-value project)

        bq query --use_legacy_sql=false \
        "SELECT
        COUNT(*)
        FROM
        ${PROJECT_ID}.customer_refined_data.customers_data"
        ```
        Result: 
        ```
        admin_@cloudshell:~ (dataplex-lab5)$ export PROJECT_ID=$(gcloud config get-value project)

        bq query --use_legacy_sql=false \
        "SELECT
        COUNT(*)
        FROM
        ${PROJECT_ID}.customer_refined_data.customers_data"
        Your active configuration is: [cloudshell-4044]
        +------+
        | f0_  |
        +------+
        | 2020 |
        +------+
        ```
- **Step 4**: Populate the Customer Credit Card Profile  data feed 
  - Open cloud shell 

    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)
    
    gcloud dataplex tasks create cc-cust-curated-refined \
    --project=${PROJECT_ID} \
    --location=us-central1 \
    --vpc-sub-network-name=projects/${PROJECT_ID}/regions/us-central1/subnetworks/dataplex-default \
    --lake=consumer-banking--customer--domain \
    --trigger-type=ON_DEMAND \
    --execution-service-account=customer-sa@${PROJECT_ID}.iam.gserviceaccount.com \
    --spark-main-class="com.google.cloud.dataproc.templates.main.DataProcTemplate" \
    --spark-file-uris="gs://${PROJECT_ID}_dataplex_process/common/log4j-spark-driver-template.properties" \
    --container-image-java-jars="gs://${PROJECT_ID}_dataplex_process/common/dataproc-templates-1.0-SNAPSHOT.jar" \
    --execution-args=^::^TASK_ARGS="--template=DATAPLEXGCSTOBQ,\
        --templateProperty=project.id=${PROJECT_ID},\
        --templateProperty=dataplex.gcs.bq.target.dataset=customer_refined_data,\
        --templateProperty=gcs.bigquery.temp.bucket.name=${PROJECT_ID}_dataplex_temp,\
        --templateProperty=dataplex.gcs.bq.save.mode=append,\
        --templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,\
        --dataplexEntity=projects/${PROJECT_ID}/locations/us-central1/lakes/consumer-banking--customer--domain/zones/customer-raw-zone/entities/cc_customers_data,\
        --partitionField=ingest_date,\
        --partitionType=DAY,\
        --targetTableName=cc_customers_data,\
      --customSqlGcsPath=gs://${PROJECT_ID}_dataplex_process/code/customer-source-configs/customercustom.sql"
    ```

- **Step5**: Monitor the job using the instructions specified in step#2

- **Step6**: Validate the data. This is a critical validation to make sure data is populated.  
    -   Open cloud shell and execute the below 2 command and make sure the count > 0
        ```bash 
        export PROJECT_ID=$(gcloud config get-value project)

        bq query --use_legacy_sql=false \
        "SELECT
        COUNT(*)
        FROM
        ${PROJECT_ID}.customer_refined_data.cc_customers_data"
        ```
        Result:
        ```
        admin_@cloudshell:~ (dataplex-lab5)$ export PROJECT_ID=$(gcloud config get-value project)

        bq query --use_legacy_sql=false \
        "SELECT
        COUNT(*)
        FROM
        ${PROJECT_ID}.customer_refined_data.cc_customers_data"
        Your active configuration is: [cloudshell-4044]
        +------+
        | f0_  |
        +------+
        | 2000 |
        +------+
        ```

#### 2.1.2. Transform and load the data from refined zone to data product zone
In this sub task we will trigger a composer workflow that will first create the bigquery table schemas if they don't exists, then trigger the Dataplex data quality job to validate the raw data and then load the customer data product. This shows how Dataplex can easily integrate with your data pipelines. 

- **Step1** Grant the service account access to raw zone for DQ

    ```bash
    curl --request PATCH  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/datasets/customer_raw_zone" \
    --header 'Accept: application/json' \
    --header 'Content-Type: application/json' \
    --data "{\"access\":[{\"userByEmail\":\"customer-sa@${PROJECT_ID}.iam.gserviceaccount.com\",\"role\":\"OWNER\"}]}" \
    --compressed 
    ```
- **Step2:** Go to Composer Service UI → You should see ${PROJECT_ID}-composer environment. Click on the Airflow UI.

    ![airflow UI](/data-mesh-banking-labs/lab4-build-data-products/resources/imgs/airflow-ui.png)

- **Step3**: Search for  “etl_with_dq_customer_data_product_wf”
- **Step4**: Trigger the DAG manually and monitor
    - You can go to Dataplex --> Process Tab --> Under the "Data Quality" Tab, you will find the DQ job that was triggered by the airflow job.  
- **Step5**: Once the DAG successfully completes, validate all the 3 Customer Data Products are Populated in BigQuery DS customer_data_product

    ![bq_cust_results](/data-mesh-banking-labs/lab4-build-data-products/resources/imgs/bq_cust_results.png)

### 2.2. Building Merchant Domain Data Products

- **Step1**: Go to Airflow UI
- **Step2**: Click on the “etl_with_dq_merchant_data_product_wf”
- **Step 3**: Trigger DAG Manually and Monitor
- **Step 4**: Validate ${PROJECT_ID}.merchants_data_product.core_merchants is populated in BQ
    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)

    bq query --use_legacy_sql=false \
    "SELECT COUNT(*) FROM ${PROJECT_ID}.merchants_data_product.core_merchants"
    ```
    Result: 
    ```
    Your active configuration is: [cloudshell-4044]
    +------+
    | f0_  |
    +------+
    | 6000 |
    ```

### 2.3. Building the Auth Data product

- **Step1:** Go to Airflow UI 
- **Step2:** Click on the “etl_with_dq_transactions_data_product_wf”
- **Step 3:** Trigger DAG Manually and Monitor
The job should execute in less than a minute. Refresh the terraform screen for updates. 
- **Step 4:** Validate ${PROJECT_ID}.auth_data_product.auth_table is populated in BigQuery  
    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)

    bq query --use_legacy_sql=false \
    "SELECT COUNT(*) FROM ${PROJECT_ID}.auth_data_product.auth_table"
    ```
    Result
    ```
    admin_@cloudshell:~ (dataplex-lab5)$ export PROJECT_ID=$(gcloud config get-value project)

    bq query --use_legacy_sql=false \
    "SELECT COUNT(*) FROM ${PROJECT_ID}.pos_auth_refined_data.auth_data"
    Your active configuration is: [cloudshell-4044]
    +------+
    | f0_  |
    +------+
    | 6000 |
    +------+
    ```


### 2.4. Building Credit card Analytics Consumer Data products

- **Step1:** Go to Airflow UI 
- **Step2:** Click on the “etl-transactions-analytics-process”
- **Step 3:** Trigger DAG Manually and Monitor
- **Step 4:** Validate ${PROJECT_ID}.cc_analytics_data_product.credit_card_transaction_data is populated in BigQuery  
    ```bash 
    export PROJECT_ID=$(gcloud config get-value project)
    bq query --use_legacy_sql=false \
    "SELECT COUNT(*) FROM ${PROJECT_ID}.auth_data_product.auth_table"
    ```
    Result
    ```
    admin_@cloudshell:~ (dataplex-lab5)$ export PROJECT_ID=$(gcloud config get-value project)

    bq query --use_legacy_sql=false \
    "SELECT COUNT(*) FROM ${PROJECT_ID}.cc_analytics_data_product.credit_card_transaction_data"
    Your active configuration is: [cloudshell-4044]
    +------+
    | f0_  |
    +------+
    | 6000 |
    +------+
    ```

## Summary
In this lab you learned how Dataplex provides serverless data management tasks that easily integrate and complement organizations existing Data Engineering pipleines through simple and open APIs. 

Organizations can build customer data management Dataplex templates and make it avilable throughout their organization. 

<hr>

This concludes the lab module. Either proceed to the [main menu](../README.md) or to the [next module](../lab5-data-classification/README.md) you will use DLP for classifying sensitive data

<hr>