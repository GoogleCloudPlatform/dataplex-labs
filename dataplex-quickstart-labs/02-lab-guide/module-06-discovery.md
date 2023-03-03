# M6: Automated discovery, schema inference and external table creation

When a Dataplex Zone's discovery option is enabled, and assets are added to the Dataplex Zone of a Dataplex Lake that has a Dataproc Metastore Service (DPMS) attached with Data Catalog Sync enabled, the following happen automatically-
1. Assets will be discovered
2. Schema will be inferred for objects in Cloud Storage
3. External table definition will be created, based on schema inference in Dataproc Metastore Service (Hive Metastore Service) 
4. External table definition will ALSO be created, based on schema inference in BigQuery
5. The tables will be available as Dataplex Zone level entities
6. The tables will be cataloged in Data Catalog and will be searchable

This lab module covers the above for assets registered in the prior module.

Discovery can be scheduled and set to be incremental, can be launched via API on-demand and in full scan mode.

### Learning units

1. [Lab - Review discovered Cloud Storage based entities in Dataplex](module-06-discovery.md#1-lab---review-discovered-cloud-storage-based-entities-in-dataplex)
2. [Lab - Review external tables created for Cloud Storage based entities by Dataplex, in BigQuery](module-06-discovery.md#2-lab---review-external-tables-created-for-cloud-storage-based-entities-by-dataplex-in-bigquery)
3. [Lab - Review external tables created for Cloud Storage based entities by Dataplex, in Dataproc Metastore Service](module-06-discovery.md#3-lab---review-external-tables-created-for-cloud-storage-based-entities-by-dataplex-in-dataproc-metastore-service)

### Prerequisites

Completion of prior modules

### Duration

~ 15 minutes

### Pictorial overview of lab

![IAM](../01-images/m6-00.png)   
<br><br>

<hr>


## 1. Lab - Review discovered Cloud Storage based entities in Dataplex

### 1.1. Discovery of data assets in the Raw Zone: oda-raw-zone

#### 1.1.1. UI view

Navigate to the Dataplex Zones UI for ODA-RAW-ZONE, and you will see "Miscellaneous Datasets" asset. Notice that it has an "Action required" flag.

![DISC-1](../01-images/05-01.png)   
<br><br>

<hr>

#### 1.1.2. Review entities automatically created

Click on "Entities". You should see multiple GCS based tables. Their names are based off of the directory names in GCS.

![DISC-2](../01-images/05-12.png)   
<br><br>

<hr>

#### 1.1.3. Review a GCS based entity's details 

Click on "icecream_sales_forecasting"; And then "Details". Review the details.

![DISC-3](../01-images/05-03.png)   
<br><br>

Click on "SCHEMA AND COLUMN TAGS". Review the schema inferred.

![DISC-4](../01-images/05-04.png)   
<br><br>

<hr>

### 1.2. Discovery of data assets in the Raw Zone: oda-raw-sensitive-zone

#### 1.2.1. UI view

Navigate to the Dataplex Zones UI for ODA-RAW-SENSITIVE-ZONE, and you will see "Banking Datasets" asset. 

![DISC-1](../01-images/M06-banking-Dataplex-01.png)   
<br><br>

<hr>

#### 1.2.2. Review entities automatically created

Click on "Entities". You should see multiple GCS based tables. Their names are based off of the directory names in GCS.

![DISC-1](../01-images/M06-banking-Dataplex-02.png)   
<br><br>

<hr>

#### 1.2.3. Review a GCS based entity's details 

Click on the entity "banking_customers_raw_customers; And then "Details". Review the details.

![DISC-1](../01-images/M06-banking-Dataplex-03.png)   
<br><br>

#### 1.2.4. Review a GCS based entity's schema inference

Click on "SCHEMA AND COLUMN TAGS". Review the schema inferred.

![DISC-1](../01-images/M06-banking-Dataplex-04.png)    
<br><br>


#### 1.2.5. Review a GCS based entity's partition inference 

Click on "PARTITION DETAILS". Review the partition inferred.

![DISC-1](../01-images/M06-banking-Dataplex-05.png)   
<br><br>

Click on "VIEW PARTITION FILES"

#### 1.2.6. Review the files in GCS & observe how it all ties together


![DISC-1](../01-images/M06-banking-Dataplex-06.png)   
<br><br>

<hr>

### 1.3. Discovery of data assets in the Raw Zone: oda-curated-zone

#### 1.3.1. UI view

Navigate to the Dataplex Zones UI for ODA-CURATED-ZONE, and you will see "Miscellaneous Datasets" asset. 

![DISC-1](../01-images/M06-banking-Dataplex-07.png)   
<br><br>

<hr>

#### 1.3.2. Review entities automatically created

Click on "Entities". You should see multiple GCS based tables. Their names are based off of the directory names in GCS.

![DISC-1](../01-images/M06-banking-Dataplex-08.png)   
<br><br>

<hr>

#### 1.3.3. Review a GCS based entity's details 

Click on the entity "retail_transactions_anomaly_detection; And then "Details". Review the details.

![DISC-1](../01-images/M06-banking-Dataplex-09.png)   
<br><br>

#### 1.3.4. Review a GCS based entity's schema inference

Click on "SCHEMA AND COLUMN TAGS". Review the schema inferred.

![DISC-1](../01-images/M06-banking-Dataplex-10.png)    
<br><br>


#### 1.3.5. Review a GCS based entity's partition inference 

Click on "PARTITION DETAILS". This table is not partitioned.

![DISC-1](../01-images/M06-banking-Dataplex-11.png)   
<br><br>
<br>

<hr>

### 1.4. Discovery of data assets in the Miscellaneous (RAW) Zone: oda-misc-zone

#### 1.4.1. UI view

Navigate to the Dataplex Zones UI for ODA-MISC-ZONE, and you will see "Code Assets" & "Notebooks" asset. 

![DISC-1](../01-images/M06-banking-Dataplex-12.png)   
<br><br>

<hr>

#### 1.4.2. Review entities automatically createdd

Click on "Entities". You should see multiple fileset listings. 
![DISC-1](../01-images/M06-banking-Dataplex-13.png)   
<br><br>

<hr>

#### 1.4.3. Review the "Code Assets" entity's details 


![DISC-1](../01-images/M06-banking-Dataplex-14.png)   
<br><br>

#### 1.4.4. Review the "Notebooks" entity's details 


![DISC-1](../01-images/M06-banking-Dataplex-15.png)    
<br><br>



<br>

<hr>


<hr>




## 2. Lab - Review external tables created for Cloud Storage based entities by Dataplex, in BigQuery

### 2.1. Datasets and Tables in BigQuery UI

![DISC-1](../01-images/M06-banking-Dataplex-16.png)    
<br><br>

Expand the datasets

![DISC-1](../01-images/M06-banking-Dataplex-17.png)    
<br><br>


### 2.2. Query a Raw Zone (BigQuery external) table in BigQuery UI

Switch to the BigQuery UI and to the dataset called oda_raw_zone. This dataset was automatically created by Dataplex when we created a zone. Notice the two tables listed there. Run a query on the Icecream Sales Forecasting table and review the results.

```
SELECT * FROM oda_curated_zone.retail_transactions_anomaly_detection LIMIT 2
```

![DISC-1](../01-images/M06-banking-Dataplex-18.png)    
<br><br>

<hr>

### 2.3. Query a Raw Sensitive Zone (BigQuery external) table in BigQuery UI

```
SELECT * FROM oda_raw_sensitive_zone.banking_customers_raw_customers WHERE DATE='2022-05-01' LIMIT 2
```

![DISC-1](../01-images/M06-banking-Dataplex-19.png)    
<br><br>

<hr>
    
### 2.4. Query a Curated Zone (BigQuery external) table in BigQuery UI

```
SELECT * FROM `dataplex-quickstart-labs.oda_curated_zone.retail_transactions_anomaly_detection` LIMIT 2
```

![DISC-1](../01-images/M06-banking-Dataplex-20.png)    
<br><br>

<hr>
<hr>


## 3. Lab - Review external tables created for Cloud Storage based entities by Dataplex, in Dataproc Metastore Service

### 3.1. Explore databases in Dataproc Metatsore

1. Click on Dataplex Explore icon on the left navigation bar
 
![DISC-00-1](../01-images/06-00-exp-01.png)   
<br><br>

2. Leaving all things default, click on the SQL editor "+" sign and type the below in it

```
show databases;
```

<br><br>

3. Let it run. 

![DISC-00-2](../01-images/06-00-exp-02.png)   
<br><br>

4. Results

![DISC-00-3](../01-images/06-00-exp-03.png)   
<br><br>

<hr>

### 3.2. Explore tables in Dataproc Metatsore in the raw zone

#### 3.2.1. List tables in Dataplex Explore Spark SQL editor

```
SHOW tables in oda_raw_zone;
```

![DISC-00-3](../01-images/06-00-exp-04.png)   
<br><br>

#### 3.2.2. Query an external table in Dataplex Explore Spark SQL editor

```
SELECT * FROM 	
oda_raw_zone.chicago_crimes_reference_data
LIMIT 2
```

![DISC-00-4](../01-images/06-00-exp-05.png)   
<br><br>

<hr>

### 3.3. Explore tables in Dataproc Metatsore in the raw sensitive zone

#### 3.3.1. List tables in Dataplex Explore Spark SQL editor

```
SHOW tables in 	oda_raw_sensitive_zone;
```

![DISC-00-5](../01-images/06-00-exp-06.png)   
<br><br>

#### 3.3.2. Query a table in Dataplex Explore Spark SQL editor

```
SELECT * FROM oda_raw_sensitive_zone.banking_customers_raw_customers LIMIT 2;
```

![DISC-00-7](../01-images/06-00-exp-07.png)   
<br><br>

<hr>

### 3.4. Explore tables in Dataproc Metatsore in the curated zone

#### 3.4.1. List tables in Dataplex Explore Spark SQL editor

```
SHOW tables in 	oda_curated_zone;
```

![DISC-00-8](../01-images/06-00-exp-08.png)   
<br><br>

#### 3.4.2. Query a table in Dataplex Explore Spark SQL editor

```
SELECT * FROM oda_curated_zone.retail_transactions_anomaly_detection LIMIT 2;
```

![DISC-00-9](../01-images/06-00-exp-09.png)   
<br><br>

<hr>

<hr>

This concludes the lab module. Proceed to the [next module](module-07-1-explore-with-spark-sql.md).

<hr>
