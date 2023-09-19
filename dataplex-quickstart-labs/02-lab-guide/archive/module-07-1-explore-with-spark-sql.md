
# M7-1: Explore Dataplex entities with Spark SQL on Data Exploration Workbench

To recap, a queryable Dataplex entity is a table - a BigQuery table, or a BigLake table/external table on structured Cloud Storage objects in the Dataplex Lake. 

In this lab module, we will query the raw GCS entity Chicago Crimes Reference Data (IUCR codes) using Spark SQL on the Data Engineering DEW Environment.

<hr>

### Prerequisites

1. Successful completion of the prior modules

2. If you run queries that use the BigQuery API, you will need to grant the principal the following role-<br>
roles/serviceusage.serviceUsageConsumer


### Duration

~ 15 minutes


### Pictorial overview of lab

![IAM](../01-images/m71-00.png)   
<br><br>

<hr>



## Lab

### 0. Recap

We created two environment templates in a prior module for exploration. Review this feature from LAKE-ENVIRONMENT as show below-

![DEW-1](../01-images/module-08-1-pre-1.png)   
<br><br>

![DEW-1](../01-images/module-08-1-pre-2.png)   
<br><br>

### 1. Setup

Lets go ahead and grant the User Managed Service Account the role below, from Cloud Shell-

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
UMSA_FQN="lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$UMSA_FQN \
--role="roles/serviceusage.serviceUsageConsumer"

```

### 2. Navigate to the Spark SQL Workbench 
Navigate to the Dataplex UI -> Explore as showin below, in the Cloud Console-

![DEW-1](../01-images/module-08-1-00.png)   
<br><br>
<hr>

### 3. Query the GCS external table Chicago Crimes Reference Data that has IUCR codes

Run the query below, which queries crimes in the table created in lab sub-module 4 in the raw zone.

```
select * from oda_raw_zone.chicago_crimes_reference_data limit 100
```

Author's output-
![DEW-1](../01-images/module-08-1-01.png)   
<br><br>

Author's output-
![DEW-1](../01-images/module-08-1-02.png)   
<br><br>


<hr>

### 4. Explore the table - count distinct IUCR codes

Run an aggregation query-
```
select count(distinct iucr) from oda_raw_zone.chicago_crimes_reference_data 
```

Author's output-

![DEW-1](../01-images/module-08-1-03.png)   
<br><br>

<hr>

<hr>

### 5. Persist the SQL script

We will persist the SQL with the name -
```
chicago-crimes-distinct-iucr-count.sql
```


Follow the steps as shown below-<br>

![DEW-1](../01-images/module-08-1-04.png)   
<br><br>

Notice where the script is persisted - in the content store in Dataplex.

![DEW-1](../01-images/module-08-1-05.png)   
<br><br>

<hr>

### 6. Schedule the SQL script to run

We will schedule a report to run and write results to a GCS bucket.

#### 6.1. Schedule a Spark SQL script via UI

Follow the steps detailed in screenshots below-

![DEW-1](../01-images/module-08-1-06.png)   
<br><br>

![DEW-1](../01-images/module-08-1-07.png)   
<br><br>


#### 6.2. FYI - Schedule a Spark SQL script via gcloud

You can schedule with a gcloud command.
```
THIS IS INFORMATIONAL - DO NOT RUN

PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
PROJECT_NBR=`gcloud projects describe $PROJECT_ID | grep projectNumber | cut -d':' -f2 |  tr -d "'" | xargs`
VPC_NM="lab-vpc-$PROJECT_NBR"

UMSA_FQN="lab-sa@${PROJECT_ID}.iam.gserviceaccount.com"
LOCATION="us-central1"
LAKE_NM="oda-lake"
SQL_SCRIPT_CONTENT_STORE_URI="projects/$PROJECT_ID/locations/$LOCATION/lakes/$LAKE_NM/contentitems/chicago-crimes.sql"
RAND_VAL=$RANDOM

gcloud dataplex tasks create chicago-crimes-report-$RAND_VAL \
--project=$PROJECT_ID \
--location=$LOCATION \
--lake=$LAKE_NM \
--trigger-type=ON_DEMAND  \
--execution-service-account="$UMSA_FQN" \
--vpc-network-name="$VPC_NM"  \
--spark-sql-script="$SQL_SCRIPT_CONTENT_STORE_URI" \ 
--execution-args=^::^TASK_ARGS="--output_location,gs://raw-data-36819656457/chicago-crimes-report-$RAND_VAL,--output_format,csv"

```

### 7. Share SQL scripts with other users

You can share scripts with other principals as shown below-

![DEW-1](../01-images/module-08-1-08.png)   
<br><br>

![DEW-1](../01-images/module-08-1-09.png)   
<br><br>
<hr>

### 8. Query a native table in the BigQuery 

Try running a query against a BigQuery dataset and it will fail. This is because the data Exploration Workbench only supports assets in the lake.


![DEW-1](../01-images/module-08-1-10.png)   
<br><br>

<hr>

This concludes the lab module. Proceed to the [next module](module-07-2-explore-with-jupyter-notebooks.md).

<hr>
