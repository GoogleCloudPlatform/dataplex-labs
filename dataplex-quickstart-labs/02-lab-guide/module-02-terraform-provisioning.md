# M2: Provisioning the lab environment

The previous lab module provided an overview of what gets provisioned automatically at the onset of the lab. In this module, we will complete the provisioning with Terraform. This will create a foundational lab environment within which we can learn Dataplex in the subsequent modules. 

### Prerequisites
1. A GCP project
2. Ability to update organizational policies

### Duration
Approximately 45 minutes


### Pictorial overview of what gets provisioned via Terraform

![AF](../01-images/m1-00.png)   
<br><br>


<hr>

## 1. Getting set up for the lab

### 1.1. Clone the git repo

Run in Cloud Shell-
```
cd ~
git clone https://github.com/anagha-google/dataplex-labs-ak.git
```

### 1.2. Set up working directory

Run in Cloud Shell-
```
cp -r dataplex-labs-ak/dataplex-quickstart-labs ~/
```

<hr>

## 2. Declare variables
 
Paste this in Cloud Shell
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
ORG_ID=`gcloud organizations list --format="value(name)"`
CLOUD_COMPOSER_IMG_VERSION="composer-2.1.3-airflow-2.3.4"
YOUR_GCP_REGION="us-central1"
YOUR_GCP_ZONE="us-central1-a"
YOUR_GCP_MULTI_REGION="US"
BQ_CONNECTOR_JAR_GCS_URI="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
```

<hr>

## 3. Run the Terraform init
```
cd ~/dataplex-quickstart-labs/00-resources/terraform

terraform init
```

## 4. Run the Terraform plan
```
cd ~/dataplex-quickstart-labs/00-resources/terraform

terraform plan \
  -var="project_id=${PROJECT_ID}" \
  -var="project_number=${PROJECT_NBR}" \
  -var="gcp_account_name=${GCP_ACCOUNT_NAME}" \
  -var="org_id=${ORG_ID}"  \
  -var="cloud_composer_image_version=${CLOUD_COMPOSER_IMG_VERSION}" \
  -var="gcp_region=${YOUR_GCP_REGION}" \
  -var="gcp_zone=${YOUR_GCP_ZONE}" \
  -var="gcp_multi_region=${YOUR_GCP_MULTI_REGION}" \
  -var="bq_connector_jar_gcs_uri=${BQ_CONNECTOR_JAR_GCS_URI}" 
```

<hr>

## 5. Provision the environment
```
cd ~/dataplex-quickstart-labs/00-resources/terraform

rm -rf dataplex-quickstart-lab.output

terraform apply \
  -var="project_id=${PROJECT_ID}" \
  -var="project_number=${PROJECT_NBR}" \
  -var="gcp_account_name=${GCP_ACCOUNT_NAME}" \
  -var="org_id=${ORG_ID}"  \
  -var="cloud_composer_image_version=${CLOUD_COMPOSER_IMG_VERSION}" \
  -var="gcp_region=${YOUR_GCP_REGION}" \
  -var="gcp_zone=${YOUR_GCP_ZONE}" \
  -var="gcp_multi_region=${YOUR_GCP_MULTI_REGION}" \
  -var="bq_connector_jar_gcs_uri=${BQ_CONNECTOR_JAR_GCS_URI}" \
  --auto-approve >> dataplex-quickstart-lab.output
```

You can tail the log file to view the provisioning process logs in a separate Cloud Shell tab-
```
tail -f ~/dataplex-quickstart-labs/00-resources/terraform/dataplex-quickstart-lab.output
```

Total time taken: ~45 minutes.

<hr>

## 6. Validate the environment setup

### 6.1. Cloud Storage

A number of buckets will be automatically created by the Terraform, and content copied into them. The following is a listing.

#### 6.1.1. Declare variables

Paste the below in Cloud Shell scoped to the project you will use for the Dataplex Quickstart Lab. Modify these variables as needed-
```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
GCP_ACCOUNT_NAME=`gcloud auth list --filter=status:ACTIVE --format="value(account)"`
ORG_ID=`gcloud organizations list --format="value(name)"`
CLOUD_COMPOSER_IMG_VERSION="composer-2.1.3-airflow-2.3.4"
YOUR_GCP_REGION="us-central1"
YOUR_GCP_ZONE="us-central1-a"
YOUR_GCP_MULTI_REGION="US"
```

#### 6.1.2. Cloud Storage Buckets created

You should see the listing below, when you paste the command below in Cloud Shell-
```
gsutil ls
```

The author's output-
```
THIS IS INFORMATIONAL 
(the author's project number is 705495340985, and therefore appears as suffix, your listing will reflect your project number)

gs://curated-data-705495340985/
gs://lab-spark-bucket-705495340985/
gs://product-data-705495340985/
gs://raw-code-705495340985/
gs://raw-data-705495340985/
gs://raw-data-sensitive-705495340985/
gs://raw-model-705495340985/
gs://raw-model-metrics-705495340985/
gs://raw-model-mleap-bundle-705495340985/
gs://raw-notebook-705495340985/
gs://us-central1-oda-70549534098-275215ea-bucket/ <-- created by Cloud Composer
```

#### 6.1.3. Raw Datasets

You should see the listing below, when you paste the command below in Cloud Shell-

```
gsutil ls -r gs://raw-data-$PROJECT_NBR/
```

This is what it should look like-
```
THIS IS INFORMATIONAL

-CELL TOWER DATA SAMPLE-
------------------------
├── cell-tower-anomaly-detection
│   ├── reference_data
│   │   └── ctad_service_threshold_ref.csv
│   └── transactions_data
│       └── ctad_transactions.csv


-CRIMES DATA SAMPLE-
--------------------
├── chicago-crimes
│   └── reference_data
│       └── crimes_chicago_iucr_ref.csv

-ICECREAM SALES DATA SAMPLE-
----------------------------
├── icecream-sales-forecasting
│   └── isf_icecream_sales_transactions.csv

-TELCO CUSTOMER CHURN DATA SAMPLE-
----------------------------------
└── telco-customer-churn-prediction
    ├── machine_learning_scoring
    │   └── tccp_customer_churn_score_candidates.csv
    └── machine_learning_training
        └── tccp_customer_churn_train_candidates.csv

```

#### 6.1.4. Raw Sensitive Datasets

You should see the listing below-

```
gsutil ls -r gs://raw-data-sensitive-$PROJECT_NBR/
```

This is what it should look like-
```
THIS IS INFORMATIONAL

-BANKING DATA SAMPLE-
---------------------
├── banking
│   ├── customers_raw
│   │   ├── credit_card_customers
│   │   │   └── date=2022-05-01
│   │   │       └── credit_card_customers.csv
│   │   └── customers
│   │       └── date=2022-05-01
│   │           └── customers.csv


```

#### 6.1.5. Curated Datasets

You should see the listing below, when you paste the command below in Cloud Shell-

```
gsutil ls -r gs://curated-data-$PROJECT_NBR/
```

This is what it should look like-
```
THIS IS INFORMATIONAL


-CELL TOWER DATA SAMPLE-
------------------------
├── cell-tower-anomaly-detection
│   ├── master_data
│   │   ├── ctad_part-00000-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet
│   │   ├── ctad_part-00001-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet
│   │   ├── ctad_part-00002-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet
│   │   └── ctad_part-00003-fc7d6e20-dbda-4143-91b5-d9414310dfd1-c000.snappy.parquet

-RETAIL TRANSACTIONS DATA SAMPLE-
---------------------------------
├── retail-transactions-anomaly-detection
│   └── rtad_sales.parquet


```

#### 6.1.6. Notebooks

You should see the listing below, when you paste the command below in Cloud Shell-

```
gsutil ls -r gs://raw-notebook-$PROJECT_NBR/
```

This is what it should look like-
```
THIS IS INFORMATIONAL


-CHICAGO CRIMES ANALYSIS STARTER NOTEBOOK-
------------------------------------------

├── chicago-crimes-analysis
│   └── chicago-crimes-analytics.ipynb
├── icecream-sales-forecasting
│   └── icecream-sales-forecasting.ipynb

-RETAIL TRANSACTIONS STARTER NOTEBOOK-
--------------------------------------

├── retail-transactions-anomaly-detection
│   └── retail-transactions-anomaly-detection.ipynb

-TELCO CUSTOMER CHURN PREDICTION STARTER NOTEBOOK-
--------------------------------------------------

└── telco-customer-churn-prediction
    ├── batch_scoring.ipynb
    ├── hyperparameter_tuning.ipynb
    ├── model_training.ipynb
    └── preprocessing.ipynb


```

#### 6.1.7. Scripts


You should see the listing below, when you paste the command below in Cloud Shell-

```
gsutil ls -r gs://raw-notebook-$PROJECT_NBR/
```

This is what it should look like-
```
THIS IS INFORMATIONAL


-AIRFLOW DAG STARTER SCRIPTS-
------------------------------------------

├── airflow
│   └── chicago-crimes-analytics
│       ├── bq_lineage_pipeline.py
│       └── spark_custom_lineage_pipeline.py

-PYSPARK STARTER SCRIPTS-
--------------------------------------

├── pyspark
│   └── chicago-crimes-analytics
│       ├── crimes_report.py
│       └── curate_crimes.py

-SPARK SQL STARTER SCRIPTS-
--------------------------------------------------

└── spark-sql
    └── retail-transactions-anomaly-detection
        └── retail-transactions-anomaly-detection.sql


```

#### 6.1.8. The rest of the buckets
```
- THIS IS INFORMATIONAL -


gs://lab-spark-bucket-705495340985/ --> For use by Dataproc Serverless Spark

gs://raw-model-705495340985/ --> For use in the Teco Customer Churn Prediction exercise with Dataplex Explore notebooks
gs://raw-model-metrics-705495340985/ --> For use in the Telco Customer Churn Prediction exercise with Dataplex Explore notebooks
gs://raw-model-mleap-bundle-705495340985/ --> For use in the Telco Customer Churn Prediction exercise with Dataplex Explore notebooks

gs://us-central1-oda-70549534098-275215ea-bucket/ --> Automatically created bucket by Cloud Composer service
```

<hr>


### 6.2. BigQuery

Nothing is provisioned at the onset of the lab.

<hr>


### 6.3. Cloud Composer

1. A Cloud Composer environment is created
2. Two DAGs are placed in the cloud composer DAG directory

Here is the author's listing-
```
- THIS IS INFORMATIONAL -

└── chicago-crimes-analytics
    ├── bq_lineage_pipeline.py
    └── spark_custom_lineage_pipeline.py
```

And here are the DAGs in the Airflow UI-

![AF](../01-images/01-01.png)   
<br><br>

<hr>

### 6.4. Dataproc Metastore Service

Its empty at the onset and does not have any precreated databases.


<hr>

This concludes the module. Please proceed to the [next module](module-03-organize-your-data-lake.md).

<hr>
